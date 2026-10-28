import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from pymongo import MongoClient
from pyspark import SparkConf, SparkContext

MONGO_URI            = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB             = os.getenv("MONGO_DB", "soccerway")
MONGO_COLLECTION     = os.getenv("MONGO_COLLECTION", "partidos")
MONGO_COLLECTION_OUT = os.getenv("MONGO_COLLECTION_OUTPUT", "resultados")
SPARK_PARTITIONS     = int(os.getenv("SPARK_PARTITIONS", "1"))
SAMPLE_FRACTION      = float(os.getenv("SAMPLE_FRACTION", "1.0"))

_sc: SparkContext = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _sc
    conf = SparkConf().setAppName("soccerway-analysis").setMaster("local[*]")
    _sc = SparkContext(conf=conf)
    _sc.setLogLevel("ERROR")
    yield
    _sc.stop()


app = FastAPI(lifespan=lifespan)


# ─── carga y submuestreo ──────────────────────────────────────────────────────

def _load_partidos(fraction: float) -> tuple[list[dict], float]:
    """Lee partidos de MongoDB. Retorna (lista, tiempo_lectura_seg)."""
    client = MongoClient(MONGO_URI)
    t0 = time.perf_counter()
    partidos = list(client[MONGO_DB][MONGO_COLLECTION].find({}, {"_id": 0}))
    t_read = time.perf_counter() - t0
    client.close()
    if fraction < 1.0:
        n = max(1, int(len(partidos) * fraction))
        partidos = partidos[:n]
    return partidos, t_read


# ─── endpoints ────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "default_partitions": SPARK_PARTITIONS,
        "default_sample_fraction": SAMPLE_FRACTION,
    }


@app.post("/analyze")
async def analyze(
    sample_fraction: float = Query(default=None, ge=0.01, le=1.0),
    partitions: int        = Query(default=None, ge=1, le=64),
):
    """
    Ejecuta las 8 consultas analíticas sobre los partidos almacenados.

    Query params opcionales (sobreescriben las variables de entorno):
      - sample_fraction: fracción del dataset a usar (0.01–1.0)
      - partitions: número de particiones del RDD (1–64)
    """
    fraction = sample_fraction if sample_fraction is not None else SAMPLE_FRACTION
    n_parts  = partitions      if partitions      is not None else SPARK_PARTITIONS

    # ── 1. Lectura desde MongoDB (con timing) ─────────────────────────────
    partidos, t_mongo_read = _load_partidos(fraction)
    total = len(partidos)

    if total == 0:
        return {"error": "No hay partidos en la base de datos. Ejecuta /collect primero."}

    # ── 2. Crear RDD y cachear ────────────────────────────────────────────
    rdd = _sc.parallelize(partidos, n_parts).cache()
    rdd.count()   # materializa el cache antes de medir queries

    timings: dict[str, float] = {}
    resultados: dict = {}

    # ══════════════════════════════════════════════════════════════════════
    # Q1 – Estadísticas por equipo: PJ, V, E, D
    # ══════════════════════════════════════════════════════════════════════
    t0 = time.perf_counter()

    def stats_equipo(p):
        gl = p.get("goles_local")    or 0
        gv = p.get("goles_visitante") or 0
        if gl > gv:
            lr, vr = (1, 0, 0), (0, 0, 1)
        elif gl < gv:
            lr, vr = (0, 0, 1), (1, 0, 0)
        else:
            lr = vr = (0, 1, 0)
        return [
            (p["equipo_local"],     (1,) + lr),
            (p["equipo_visitante"], (1,) + vr),
        ]

    def red(a, b):
        return tuple(x + y for x, y in zip(a, b))

    q1_raw = rdd.flatMap(stats_equipo).reduceByKey(red).collect()
    resultados["q1_estadisticas_por_equipo"] = sorted(
        [{"equipo": k, "PJ": v[0], "V": v[1], "E": v[2], "D": v[3]} for k, v in q1_raw],
        key=lambda x: x["PJ"], reverse=True,
    )
    timings["q1"] = round(time.perf_counter() - t0, 4)

    # ══════════════════════════════════════════════════════════════════════
    # Q2 – Goles por equipo: GF, GC, diferencia de goles
    # ══════════════════════════════════════════════════════════════════════
    t0 = time.perf_counter()

    def goles_equipo(p):
        gl = p.get("goles_local")    or 0
        gv = p.get("goles_visitante") or 0
        return [
            (p["equipo_local"],     (gl, gv)),
            (p["equipo_visitante"], (gv, gl)),
        ]

    q2_raw = rdd.flatMap(goles_equipo).reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])).collect()
    resultados["q2_goles_por_equipo"] = sorted(
        [{"equipo": k, "GF": v[0], "GC": v[1], "DG": v[0]-v[1]} for k, v in q2_raw],
        key=lambda x: x["GF"], reverse=True,
    )
    timings["q2"] = round(time.perf_counter() - t0, 4)

    # ══════════════════════════════════════════════════════════════════════
    # Q3 – Puntos y rendimiento: PTS, % victorias, promedio puntos/partido
    # ══════════════════════════════════════════════════════════════════════
    t0 = time.perf_counter()

    def puntos_equipo(p):
        gl = p.get("goles_local")    or 0
        gv = p.get("goles_visitante") or 0
        if gl > gv:   pl, pv = 3, 0
        elif gl < gv: pl, pv = 0, 3
        else:         pl = pv = 1
        return [
            (p["equipo_local"],     (pl, 1)),
            (p["equipo_visitante"], (pv, 1)),
        ]

    q3_raw = rdd.flatMap(puntos_equipo).reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])).collect()
    q1_map = {x["equipo"]: x["V"] for x in resultados["q1_estadisticas_por_equipo"]}
    resultados["q3_puntos_rendimiento"] = sorted(
        [{"equipo": k,
          "PTS":  v[0],
          "PJ":   v[1],
          "pct_victorias":       round(q1_map.get(k, 0) / v[1] * 100, 2) if v[1] > 0 else 0,
          "prom_puntos_partido": round(v[0] / v[1], 3) if v[1] > 0 else 0}
         for k, v in q3_raw],
        key=lambda x: x["PTS"], reverse=True,
    )
    timings["q3"] = round(time.perf_counter() - t0, 4)

    # ══════════════════════════════════════════════════════════════════════
    # Q4 – Distribución de resultados: local / empate / visitante
    # ══════════════════════════════════════════════════════════════════════
    t0 = time.perf_counter()

    def clasificar(p):
        gl = p.get("goles_local")    or 0
        gv = p.get("goles_visitante") or 0
        if gl > gv: return ("victoria_local",    1)
        if gv > gl: return ("victoria_visitante", 1)
        return ("empate", 1)

    dist = dict(rdd.map(clasificar).reduceByKey(lambda a, b: a + b).collect())
    resultados["q4_distribucion_resultados"] = {
        k: {"cantidad": v, "porcentaje": round(v / total * 100, 2)}
        for k, v in dist.items()
    }
    timings["q4"] = round(time.perf_counter() - t0, 4)

    # ══════════════════════════════════════════════════════════════════════
    # Q5 – Promedio de goles por partido
    # ══════════════════════════════════════════════════════════════════════
    t0 = time.perf_counter()
    suma = rdd.map(
        lambda p: (p.get("goles_local") or 0) + (p.get("goles_visitante") or 0)
    ).sum()
    resultados["q5_promedio_goles_partido"] = round(suma / total, 3)
    timings["q5"] = round(time.perf_counter() - t0, 4)

    # ══════════════════════════════════════════════════════════════════════
    # Q6 – Partidos por temporada
    # ══════════════════════════════════════════════════════════════════════
    t0 = time.perf_counter()
    q6_raw = (
        rdd.map(lambda p: (p.get("temporada") or "desconocida", 1))
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )
    resultados["q6_partidos_por_temporada"] = sorted(
        [{"temporada": k, "partidos": v} for k, v in q6_raw],
        key=lambda x: x["temporada"] or "",
    )
    timings["q6"] = round(time.perf_counter() - t0, 4)

    # ══════════════════════════════════════════════════════════════════════
    # Q7 – Rendimiento ofensivo: top 10 equipos con más GF
    # ══════════════════════════════════════════════════════════════════════
    t0 = time.perf_counter()
    resultados["q7_rendimiento_ofensivo"] = [
        {"equipo": x["equipo"], "GF": x["GF"]}
        for x in resultados["q2_goles_por_equipo"][:10]
    ]
    timings["q7"] = round(time.perf_counter() - t0, 4)

    # ══════════════════════════════════════════════════════════════════════
    # Q8 – Top 10 partidos con más goles
    # ══════════════════════════════════════════════════════════════════════
    t0 = time.perf_counter()
    resultados["q8_partidos_mas_goles"] = (
        rdd.map(lambda p: {
            "partido":   f"{p['equipo_local']} vs {p['equipo_visitante']}",
            "fecha":     p.get("fecha", ""),
            "resultado": f"{p.get('goles_local', 0)}-{p.get('goles_visitante', 0)}",
            "goles":     (p.get("goles_local") or 0) + (p.get("goles_visitante") or 0),
        })
        .sortBy(lambda x: x["goles"], ascending=False)
        .take(10)
    )
    timings["q8"] = round(time.perf_counter() - t0, 4)

    # ── 3. Persistir resultados en MongoDB ────────────────────────────────
    client = MongoClient(MONGO_URI)
    col_out = client[MONGO_DB][MONGO_COLLECTION_OUT]
    col_out.delete_many({})
    col_out.insert_one({
        "partidos_analizados": total,
        "spark_partitions":    n_parts,
        "sample_fraction":     fraction,
        "t_mongo_read_seg":    round(t_mongo_read, 4),
        "timings_segundos":    timings,
        **resultados,
    })
    client.close()

    return {
        "partidos_analizados": total,
        "spark_partitions":    n_parts,
        "sample_fraction":     fraction,
        "t_mongo_read_seg":    round(t_mongo_read, 4),
        "timings_segundos":    timings,
        "resultados":          resultados,
    }