import os
from pyspark import SparkContext, SparkConf
from pymongo import MongoClient
from fastapi import FastAPI
from contextlib import asynccontextmanager

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB = os.getenv("MONGO_DB", "soccerway")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "partidos")
MONGO_COLLECTION_OUTPUT = os.getenv("MONGO_COLLECTION_OUTPUT", "resultados")
SPARK_PARTITIONS = int(os.getenv("SPARK_PARTITIONS", "1"))
SAMPLE_FRACTION = float(os.getenv("SAMPLE_FRACTION", "1.0"))

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


def _load_rdd():
    """Lee partidos desde MongoDB y crea un RDD con las particiones configuradas."""
    client = MongoClient(MONGO_URI)
    partidos = list(client[MONGO_DB][MONGO_COLLECTION].find({}, {"_id": 0}))
    client.close()

    if SAMPLE_FRACTION < 1.0:
        n = max(1, int(len(partidos) * SAMPLE_FRACTION))
        partidos = partidos[:n]

    return _sc.parallelize(partidos, SPARK_PARTITIONS)


@app.get("/health")
async def health():
    return {"status": "ok", "partitions": SPARK_PARTITIONS, "sample_fraction": SAMPLE_FRACTION}


@app.post("/analyze")
async def analyze():
    """Ejecuta las 8 queries analíticas con Spark RDD sobre los partidos almacenados."""
    rdd = _load_rdd()
    total = rdd.count()

    if total == 0:
        return {"error": "No hay partidos en la base de datos. Ejecuta /collect primero."}

    resultados = {}

    # Q1: Goles totales por equipo (local + visitante)
    q1 = (
        rdd.flatMap(lambda p: [
            (p["equipo_local"],    p.get("goles_local", 0) or 0),
            (p["equipo_visitante"], p.get("goles_visitante", 0) or 0),
        ])
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )
    resultados["q1_goles_por_equipo"] = sorted(
        [{"equipo": k, "goles": v} for k, v in q1],
        key=lambda x: x["goles"], reverse=True,
    )

    # Q2: Victorias por equipo
    def victorias(p):
        gl = p.get("goles_local") or 0
        gv = p.get("goles_visitante") or 0
        if gl > gv:
            return [(p["equipo_local"], 1)]
        if gv > gl:
            return [(p["equipo_visitante"], 1)]
        return [(p["equipo_local"], 0), (p["equipo_visitante"], 0)]

    q2 = rdd.flatMap(victorias).reduceByKey(lambda a, b: a + b).collect()
    resultados["q2_victorias_por_equipo"] = sorted(
        [{"equipo": k, "victorias": v} for k, v in q2],
        key=lambda x: x["victorias"], reverse=True,
    )

    # Q3: Empates por equipo
    q3 = (
        rdd.filter(lambda p: p.get("goles_local") is not None
                             and p.get("goles_local") == p.get("goles_visitante"))
        .flatMap(lambda p: [(p["equipo_local"], 1), (p["equipo_visitante"], 1)])
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )
    resultados["q3_empates_por_equipo"] = sorted(
        [{"equipo": k, "empates": v} for k, v in q3],
        key=lambda x: x["empates"], reverse=True,
    )

    # Q4: Promedio de goles por partido
    suma = rdd.map(
        lambda p: (p.get("goles_local") or 0) + (p.get("goles_visitante") or 0)
    ).sum()
    resultados["q4_promedio_goles_por_partido"] = round(suma / total, 3)

    # Q5: Top 10 partidos más goleados
    resultados["q5_top10_partidos_goleados"] = (
        rdd.map(lambda p: {
            "partido":   f"{p['equipo_local']} vs {p['equipo_visitante']}",
            "fecha":     p.get("fecha", ""),
            "resultado": f"{p.get('goles_local', 0)}-{p.get('goles_visitante', 0)}",
            "goles":     (p.get("goles_local") or 0) + (p.get("goles_visitante") or 0),
        })
        .sortBy(lambda x: x["goles"], ascending=False)
        .take(10)
    )

    # Q6: Distribución de resultados (victoria local / empate / victoria visitante)
    def clasificar(p):
        gl = p.get("goles_local") or 0
        gv = p.get("goles_visitante") or 0
        if gl > gv:   return ("victoria_local", 1)
        if gv > gl:   return ("victoria_visitante", 1)
        return ("empate", 1)

    dist = dict(rdd.map(clasificar).reduceByKey(lambda a, b: a + b).collect())
    resultados["q6_distribucion_resultados"] = {
        k: {"cantidad": v, "porcentaje": round(v / total * 100, 2)}
        for k, v in dist.items()
    }

    # Q7: Rendimiento como local (ratio victorias_local / partidos_local)
    q7 = (
        rdd.map(lambda p: (
            p["equipo_local"],
            (1 if (p.get("goles_local") or 0) > (p.get("goles_visitante") or 0) else 0, 1),
        ))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .map(lambda kv: {
            "equipo":          kv[0],
            "victorias_local": kv[1][0],
            "partidos_local":  kv[1][1],
            "ratio":           round(kv[1][0] / kv[1][1], 3) if kv[1][1] > 0 else 0,
        })
        .sortBy(lambda x: x["ratio"], ascending=False)
        .collect()
    )
    resultados["q7_rendimiento_local"] = q7

    # Q8: Goles totales por temporada
    q8 = (
        rdd.map(lambda p: (
            p.get("temporada", "desconocida"),
            (p.get("goles_local") or 0) + (p.get("goles_visitante") or 0),
        ))
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )
    resultados["q8_goles_por_temporada"] = sorted(
        [{"temporada": k, "goles_totales": v} for k, v in q8],
        key=lambda x: x["goles_totales"], reverse=True,
    )

    # Persistir resultados en MongoDB
    client = MongoClient(MONGO_URI)
    col_out = client[MONGO_DB][MONGO_COLLECTION_OUTPUT]
    col_out.delete_many({})
    col_out.insert_one({
        "partidos_analizados": total,
        "spark_partitions":    SPARK_PARTITIONS,
        "sample_fraction":     SAMPLE_FRACTION,
        **resultados,
    })
    client.close()

    return {"partidos_analizados": total, "resultados": resultados}
