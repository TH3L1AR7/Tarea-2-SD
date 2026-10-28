"""
Validación de corrección (sección "Consultas analíticas" del enunciado).

Usa un conjunto pequeño de partidos con resultados conocidos (incluye registros
sucios a propósito), calcula a mano las métricas esperadas y las compara con lo
que produce Spark con 1, 2 y 4 particiones. No necesita MongoDB.

Uso (dentro del contenedor):
    docker compose exec spark-service python -m app.validate
"""

import json
import sys

from pyspark import SparkConf, SparkContext

from . import pipeline

ALIAS = pipeline.build_alias_map({
    "teams": [{"name": "Colo Colo"}, {"name": "U. De Chile"}, {"name": "U. Católica"}],
    "aliases": {"Universidad Católica": "U. Católica"},
})

# A = Colo Colo, B = U. De Chile, C = U. Católica
PARTIDOS = [
    # válidos
    {"match_id": "m1", "fecha": "2024-03-01", "temporada": "2024", "estado": "finalizado",
     "equipo_local": "Colo Colo", "equipo_visitante": "U. De Chile", "goles_local": 2, "goles_visitante": 1},
    {"match_id": "m2", "fecha": "2024-03-08", "temporada": "2024", "estado": "finalizado",
     "equipo_local": "U. De Chile", "equipo_visitante": "U. Católica\n2", "goles_local": 0, "goles_visitante": 0},
    {"match_id": "m3", "fecha": "2024-04-02", "temporada": "2024", "estado": "finalizado",
     "equipo_local": "Universidad Católica", "equipo_visitante": "Colo Colo (Chi)", "goles_local": 3, "goles_visitante": 3},
    {"match_id": "m4", "fecha": "2025-02-10", "temporada": "2025", "estado": "finalizado",
     "equipo_local": "U. Católica", "equipo_visitante": "U. De Chile", "goles_local": 1, "goles_visitante": 4},
    {"match_id": "m5", "fecha": None, "temporada": "2025", "estado": "finalizado",
     "equipo_local": "Colo Colo", "equipo_visitante": "U. Católica", "goles_local": 0, "goles_visitante": 1},
    # sucios: deben descartarse
    {"match_id": "m1", "fecha": "2024-03-01", "temporada": "2024", "estado": "finalizado",       # dup id
     "equipo_local": "Colo Colo", "equipo_visitante": "U. De Chile", "goles_local": 2, "goles_visitante": 1},
    {"match_id": "m1b", "fecha": "2024-03-01", "temporada": "2024", "estado": "finalizado",      # dup clave natural
     "equipo_local": "Colo Colo", "equipo_visitante": "U. De Chile", "goles_local": 2, "goles_visitante": 1},
    {"match_id": "x1", "fecha": "2025-05-01", "temporada": "2025", "estado": "programado",       # no finalizado
     "equipo_local": "Colo Colo", "equipo_visitante": "U. De Chile", "goles_local": None, "goles_visitante": None},
    {"match_id": "x2", "fecha": "2025-05-02", "temporada": "2025", "estado": "suspendido",       # no finalizado
     "equipo_local": "U. De Chile", "equipo_visitante": "U. Católica", "goles_local": 1, "goles_visitante": 0},
    {"match_id": "x3", "fecha": "2025-05-03", "temporada": "2025", "estado": "finalizado",       # marcador inválido
     "equipo_local": "U. Católica", "equipo_visitante": "Colo Colo", "goles_local": None, "goles_visitante": 2},
    {"match_id": "x4", "fecha": "2025-05-04", "temporada": "2025", "estado": "finalizado",       # sin visitante
     "equipo_local": "Colo Colo", "equipo_visitante": None, "goles_local": 1, "goles_visitante": 0},
    {"match_id": "x5", "fecha": "2025-05-05", "temporada": "2025", "estado": "finalizado",       # goles negativos
     "equipo_local": "Colo Colo", "equipo_visitante": "U. Católica", "goles_local": -1, "goles_visitante": 0},
]

# ── Esperado, calculado a mano ───────────────────────────────────────────────
# m1 CC 2-1 UCh | m2 UCh 0-0 UC | m3 UC 3-3 CC | m4 UC 1-4 UCh | m5 CC 0-1 UC
ESPERADO = {
    "descartes": {"sin_campos_esenciales": 1, "no_finalizados": 2, "resultado_invalido": 2,
                  "mismo_equipo": 0, "duplicado_match_id": 1, "duplicado_clave_natural": 1},
    "q1": {"Colo Colo":   {"PJ": 3, "V": 1, "E": 1, "D": 1},
           "U. De Chile": {"PJ": 3, "V": 1, "E": 1, "D": 1},
           "U. Católica": {"PJ": 4, "V": 1, "E": 2, "D": 1}},
    "q2": {"Colo Colo":   {"GF": 5, "GC": 5, "DG": 0},
           "U. De Chile": {"GF": 5, "GC": 3, "DG": 2},
           "U. Católica": {"GF": 5, "GC": 7, "DG": -2}},
    "q3": {"Colo Colo":   {"PTS": 4, "pct_victorias": 33.33, "prom_puntos_partido": 1.333},
           "U. De Chile": {"PTS": 4, "pct_victorias": 33.33, "prom_puntos_partido": 1.333},
           "U. Católica": {"PTS": 5, "pct_victorias": 25.0,  "prom_puntos_partido": 1.25}},
    "q4": {"victoria_local": 1, "empate": 2, "victoria_visitante": 2},
    "q5": 3.0,   # (3+0+6+5+1)=15 goles / 5 partidos
    "q6_temporada": {"2024": 3, "2025": 2},
    "q6_mes": {"2024-03": 2, "2024-04": 1, "2025-02": 1},   # m5 sin fecha no cuenta por mes
    "q7_top1_gf": 5,
    "q8_top": ("m3", 6),
}


def _check(nombre, obtenido, esperado, errores):
    ok = obtenido == esperado
    print(f"  [{'OK' if ok else 'FALLA'}] {nombre}")
    if not ok:
        errores.append(nombre)
        print(f"      esperado: {esperado}\n      obtenido: {obtenido}")


def correr(sc, parts):
    bc = sc.broadcast(ALIAS)
    raw = sc.parallelize(PARTIDOS, 1)
    limpio, rep = pipeline.limpiar(raw, bc)
    if parts > 1:
        limpio = limpio.repartition(parts)
    partidos = limpio.cache()
    contrib = partidos.flatMap(pipeline.contribuciones).cache()
    r = {n: fn(partidos if src == "partidos" else contrib) for n, src, fn in pipeline.QUERIES}
    return rep, r


def main():
    sc = SparkContext(conf=SparkConf().setAppName("validate").setMaster("local[4]"))
    sc.setLogLevel("ERROR")
    errores, firmas = [], {}
    for parts in (1, 2, 4):
        print(f"\n=== {parts} partición(es) ===")
        rep, r = correr(sc, parts)
        _check("descartes por regla", rep["descartes"], ESPERADO["descartes"], errores)
        _check("Q1 PJ/V/E/D", {x["equipo"]: {k: x[k] for k in ("PJ", "V", "E", "D")}
                               for x in r["q1_estadisticas_por_equipo"]}, ESPERADO["q1"], errores)
        _check("Q2 GF/GC/DG", {x["equipo"]: {k: x[k] for k in ("GF", "GC", "DG")}
                               for x in r["q2_goles_por_equipo"]}, ESPERADO["q2"], errores)
        _check("Q3 PTS/%V/prom", {x["equipo"]: {k: x[k] for k in ("PTS", "pct_victorias", "prom_puntos_partido")}
                                  for x in r["q3_puntos_rendimiento"]["global"]}, ESPERADO["q3"], errores)
        _check("Q4 distribución", {k: v["cantidad"] for k, v in r["q4_distribucion_resultados"].items()},
               ESPERADO["q4"], errores)
        _check("Q5 promedio goles", r["q5_promedio_goles"]["promedio_goles_partido"], ESPERADO["q5"], errores)
        _check("Q6 por temporada", {x["temporada"]: x["partidos"] for x in r["q6_partidos_por_periodo"]["por_temporada"]},
               ESPERADO["q6_temporada"], errores)
        _check("Q6 por mes", {x["mes"]: x["partidos"] for x in r["q6_partidos_por_periodo"]["por_mes"]},
               ESPERADO["q6_mes"], errores)
        _check("Q7 top GF", r["q7_rendimiento_ofensivo"][0]["GF"], ESPERADO["q7_top1_gf"], errores)
        _check("Q8 partido con más goles", (r["q8_partidos_mas_goles"][0]["match_id"],
                                            r["q8_partidos_mas_goles"][0]["goles"]), ESPERADO["q8_top"], errores)
        firmas[parts] = json.dumps(r, sort_keys=True, ensure_ascii=False)

    print("\n=== Invariancia respecto a particiones ===")
    _check("resultados idénticos con 1, 2 y 4 particiones", len(set(firmas.values())), 1, errores)
    sc.stop()
    print(f"\n{'TODO OK' if not errores else f'{len(errores)} FALLA(S)'}")
    sys.exit(1 if errores else 0)


if __name__ == "__main__":
    main()
