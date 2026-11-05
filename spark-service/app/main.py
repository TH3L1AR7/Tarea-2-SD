import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from pymongo import MongoClient
from pyspark import SparkConf, SparkContext, StorageLevel

from . import pipeline

MONGO_URI            = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB             = os.getenv("MONGO_DB", "soccerway")
MONGO_COLLECTION     = os.getenv("MONGO_COLLECTION", "partidos")
MONGO_COLLECTION_OUT = os.getenv("MONGO_COLLECTION_OUTPUT", "resultados")
SPARK_PARTITIONS     = int(os.getenv("SPARK_PARTITIONS", "1"))
SAMPLE_FRACTION      = float(os.getenv("SAMPLE_FRACTION", "1.0"))
SAMPLE_SEED          = int(os.getenv("SAMPLE_SEED", "42"))
SPARK_MASTER         = os.getenv("SPARK_MASTER", "local[4]")
TEAMS_FILE           = os.getenv("TEAMS_FILE", "/shared/teams.json")

_sc: SparkContext = None
_alias_bc = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _sc, _alias_bc
    conf = SparkConf().setAppName("soccerway-analysis").setMaster(SPARK_MASTER)
    _sc = SparkContext(conf=conf)
    _sc.setLogLevel("ERROR")
    with open(TEAMS_FILE, encoding="utf-8") as f:
        _alias_bc = _sc.broadcast(pipeline.build_alias_map(json.load(f)))
    yield
    _sc.stop()


app = FastAPI(title="Spark Service", lifespan=lifespan)


def _read_mongo() -> tuple[list[dict], float]:
    client = MongoClient(MONGO_URI)
    try:
        t0 = time.perf_counter()
        docs = list(client[MONGO_DB][MONGO_COLLECTION].find({}, {"_id": 0}))
        return docs, time.perf_counter() - t0
    finally:
        client.close()


def _timed(fn, *args):
    t0 = time.perf_counter()
    out = fn(*args)
    return out, round(time.perf_counter() - t0, 4)


@app.get("/health")
def health():
    return {"status": "ok", "master": SPARK_MASTER,
            "default_partitions": SPARK_PARTITIONS, "default_sample_fraction": SAMPLE_FRACTION}


@app.post("/read-benchmark")
def read_benchmark():
    docs, t_read = _read_mongo()
    t0 = time.perf_counter()
    n = _sc.parallelize(docs, 1).count()
    t_rdd = time.perf_counter() - t0
    return {"documentos": n, "t_mongo_read_seg": round(t_read, 4),
            "t_rdd_load_seg": round(t_rdd, 4), "t_total_seg": round(t_read + t_rdd, 4)}


@app.post("/analyze")
def analyze(
    sample_fraction: float = Query(default=None, gt=0.0, le=1.0),
    partitions: int        = Query(default=None, ge=1, le=64),
    seed: int              = Query(default=None),
    persist: bool          = Query(default=True, description="Guardar resultados en MongoDB"),
):
    fraction = SAMPLE_FRACTION if sample_fraction is None else sample_fraction
    n_parts  = SPARK_PARTITIONS if partitions is None else partitions
    seed     = SAMPLE_SEED if seed is None else seed
    t_start  = time.perf_counter()

    docs, t_mongo_read = _read_mongo()
    if not docs:
        raise HTTPException(404, "No hay partidos en MongoDB. Ejecuta POST /collect en el collector.")
    t0 = time.perf_counter()
    raw = _sc.parallelize(docs, 1).persist(StorageLevel.MEMORY_ONLY)
    raw.count()
    t_rdd_load = time.perf_counter() - t0

    (limpio, reporte), t_limpieza = _timed(pipeline.limpiar, raw, _alias_bc)

    t0 = time.perf_counter()
    dataset = limpio if fraction >= 1.0 else limpio.sample(False, fraction, seed)
    if n_parts > 1:
        dataset = dataset.repartition(n_parts)
    partidos = dataset.persist(StorageLevel.MEMORY_ONLY)
    n_partidos = partidos.count()
    contrib = partidos.flatMap(pipeline.contribuciones).persist(StorageLevel.MEMORY_ONLY)
    n_contrib = contrib.count()
    t_cache = time.perf_counter() - t0
    raw.unpersist()

    fuentes = {"partidos": partidos, "contrib": contrib}
    resultados, timings = {}, {}
    for nombre, fuente, fn in pipeline.QUERIES:
        resultados[nombre], timings[nombre.split("_")[0]] = _timed(fn, fuentes[fuente])

    partidos.unpersist()
    contrib.unpersist()

    meta = {
        "partidos_analizados":  n_partidos,
        "contribuciones":       n_contrib,
        "spark_partitions":     n_parts,
        "sample_fraction":      fraction,
        "seed":                 seed,
        "limpieza":             reporte,
        "tiempos_seg": {
            "t_mongo_read":   round(t_mongo_read, 4),
            "t_rdd_load":     round(t_rdd_load, 4),
            "t_limpieza":     t_limpieza,
            "t_cache":        round(t_cache, 4),
            "t_queries_total": round(sum(timings.values()), 4),
            "t_total":        round(time.perf_counter() - t_start, 4),
        },
        "timings_queries_seg": timings,
    }

    if persist:
        client = MongoClient(MONGO_URI)
        try:
            client[MONGO_DB][MONGO_COLLECTION_OUT].replace_one(
                {"_id": f"frac={fraction}|parts={n_parts}|seed={seed}"},
                {**meta, "resultados": resultados,
                 "generado_en": datetime.now(timezone.utc).isoformat()},
                upsert=True,
            )
        finally:
            client.close()

    return {**meta, "resultados": resultados}
