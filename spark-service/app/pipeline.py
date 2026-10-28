"""
Pipeline Spark RDD: limpieza distribuida, modelado de contribuciones y 8 consultas.

Se mantiene separado de la API (main.py) para poder validarlo con datos conocidos
(app/validate.py) sin MongoDB.

Formato de una contribución (paso 5 del flujo):
    (equipo, (temporada, GF, GC, resultado, puntos))   resultado ∈ {"V","E","D"}
"""

import re
import unicodedata

GOLES_MAX = 20  # cota de sanidad: un marcador > 20 se considera error de scraping

_RE_TARJETAS = re.compile(r"\s*\n\s*\d+\s*$")   # "U. Católica\n2" -> tarjetas rojas
_RE_PAIS = re.compile(r"\s*\((Chi|CHI)\)\s*$")   # "Coquimbo (Chi)"
_RE_ESPACIOS = re.compile(r"\s+")


# ══════════════════════════════════════════════════════════════════════════════
# Normalización
# ══════════════════════════════════════════════════════════════════════════════

def _clave(nombre: str) -> str:
    """Clave para comparar nombres sin importar mayúsculas/tildes/puntos."""
    n = unicodedata.normalize("NFKD", nombre)
    n = "".join(c for c in n if not unicodedata.combining(c))
    return "".join(c for c in n.lower() if c.isalnum())


def build_alias_map(teams_cfg: dict) -> dict:
    """{clave_normalizada: nombre_canónico} a partir de shared/teams.json."""
    m = {}
    for t in teams_cfg.get("teams", []):
        m[_clave(t["name"])] = t["name"]
    for variante, canon in teams_cfg.get("aliases", {}).items():
        m[_clave(variante)] = canon
        m[_clave(canon)] = canon
    return m


def normalizar_nombre(nombre, alias_map: dict):
    if not nombre:
        return nombre
    n = _RE_TARJETAS.sub("", nombre)
    n = _RE_PAIS.sub("", n)
    n = _RE_ESPACIOS.sub(" ", n).strip()
    return alias_map.get(_clave(n), n)


def _es_gol_valido(g) -> bool:
    return isinstance(g, int) and not isinstance(g, bool) and 0 <= g <= GOLES_MAX


# ══════════════════════════════════════════════════════════════════════════════
# Limpieza (paso 4)
# ══════════════════════════════════════════════════════════════════════════════

def limpiar(raw_rdd, alias_bc):
    """Aplica las reglas de limpieza en orden y cuenta registros en cada etapa.

    Retorna (rdd_limpio, conteos). rdd_limpio NO está cacheado (lo decide el caller).
    Cada .count() materializa la etapa: se usa solo para el reporte de limpieza.
    """
    c = {"0_crudos": raw_rdd.count()}

    # R1 – Validación de campos esenciales: id y ambos equipos
    r1 = raw_rdd.filter(lambda p: p.get("match_id") and p.get("equipo_local") and p.get("equipo_visitante"))
    c["1_con_campos_esenciales"] = r1.count()

    # R2 – Filtrado de partidos: solo finalizados (programado/suspendido/reprogramado fuera)
    r2 = r1.filter(lambda p: p.get("estado") == "finalizado")
    c["2_finalizados"] = r2.count()

    # R3 – Validación de resultados: goles enteros, no negativos, <= GOLES_MAX
    r3 = r2.filter(lambda p: _es_gol_valido(p.get("goles_local")) and _es_gol_valido(p.get("goles_visitante")))
    c["3_resultado_valido"] = r3.count()

    # R4 – Normalización de nombres + unificación de formato
    def _norm(p):
        am = alias_bc.value
        q = dict(p)
        q["equipo_local"] = normalizar_nombre(p["equipo_local"], am)
        q["equipo_visitante"] = normalizar_nombre(p["equipo_visitante"], am)
        q["temporada"] = str(p["temporada"]) if p.get("temporada") else None
        return q

    r4 = r3.map(_norm).filter(lambda p: p["equipo_local"] != p["equipo_visitante"])
    c["4_normalizados_equipos_distintos"] = r4.count()

    # R5 – Duplicados (criterio 1): mismo match_id -> se conserva uno
    r5 = r4.map(lambda p: (p["match_id"], p)).reduceByKey(lambda a, b: a).values()
    c["5_unicos_por_match_id"] = r5.count()

    # R6 – Duplicados (criterio 2): mismo (fecha, local, visitante) con distinto id.
    #      Solo aplica si hay fecha; los que no tienen fecha pasan tal cual.
    con_fecha = r5.filter(lambda p: p.get("fecha"))
    sin_fecha = r5.filter(lambda p: not p.get("fecha"))
    r6 = (con_fecha.map(lambda p: ((p["fecha"], p["equipo_local"], p["equipo_visitante"]), p))
          .reduceByKey(lambda a, b: a if a["match_id"] <= b["match_id"] else b)
          .values()
          .union(sin_fecha))
    c["6_unicos_por_clave_natural"] = r6.count()

    descartes = {
        "sin_campos_esenciales": c["0_crudos"] - c["1_con_campos_esenciales"],
        "no_finalizados":        c["1_con_campos_esenciales"] - c["2_finalizados"],
        "resultado_invalido":    c["2_finalizados"] - c["3_resultado_valido"],
        "mismo_equipo":          c["3_resultado_valido"] - c["4_normalizados_equipos_distintos"],
        "duplicado_match_id":    c["4_normalizados_equipos_distintos"] - c["5_unicos_por_match_id"],
        "duplicado_clave_natural": c["5_unicos_por_match_id"] - c["6_unicos_por_clave_natural"],
    }
    return r6, {"etapas": c, "descartes": descartes}


# ══════════════════════════════════════════════════════════════════════════════
# Contribuciones (paso 5)
# ══════════════════════════════════════════════════════════════════════════════

def _resultado(gf, gc):
    if gf > gc:
        return "V", 3
    if gf == gc:
        return "E", 1
    return "D", 0


def contribuciones(p):
    """Un partido -> dos registros clave-valor, uno por equipo."""
    gl, gv, temp = p["goles_local"], p["goles_visitante"], p.get("temporada")
    rl, pl = _resultado(gl, gv)
    rv, pv = _resultado(gv, gl)
    return [
        (p["equipo_local"],     (temp, gl, gv, rl, pl)),
        (p["equipo_visitante"], (temp, gv, gl, rv, pv)),
    ]


def _a_metricas(c):
    """Contribución -> vector sumable (PJ, V, E, D, GF, GC, PTS)."""
    _, gf, gc, r, pts = c
    return (1, int(r == "V"), int(r == "E"), int(r == "D"), gf, gc, pts)


def _sumar(a, b):
    return tuple(x + y for x, y in zip(a, b))


# ══════════════════════════════════════════════════════════════════════════════
# Consultas (paso 6). Cada una es independiente y lanza su propio job de Spark.
# `partidos`  = RDD limpio (cacheado)
# `contrib`   = RDD de contribuciones (cacheado)
# ══════════════════════════════════════════════════════════════════════════════

def q1_estadisticas_por_equipo(contrib):
    """PJ, V, E, D por equipo."""
    out = (contrib.mapValues(lambda c: _a_metricas(c)[:4])
           .reduceByKey(_sumar)
           .collect())
    return sorted([{"equipo": k, "PJ": v[0], "V": v[1], "E": v[2], "D": v[3]} for k, v in out],
                  key=lambda x: (-x["PJ"], x["equipo"]))


def q2_goles_por_equipo(contrib):
    """GF, GC, DG por equipo."""
    out = (contrib.mapValues(lambda c: (c[1], c[2]))
           .reduceByKey(_sumar)
           .collect())
    return sorted([{"equipo": k, "GF": v[0], "GC": v[1], "DG": v[0] - v[1]} for k, v in out],
                  key=lambda x: (-x["DG"], -x["GF"], x["equipo"]))


def _rendimiento(k, v):
    pj, vic, pts = v
    return {"PJ": pj, "PTS": pts,
            "pct_victorias": round(vic / pj * 100, 2) if pj else 0.0,
            "prom_puntos_partido": round(pts / pj, 3) if pj else 0.0}


def q3_puntos_rendimiento(contrib):
    """PTS, % victorias y puntos/partido: global por equipo y por (equipo, temporada)."""
    base = contrib.mapValues(lambda c: (1, int(c[3] == "V"), c[4]))
    glob = base.reduceByKey(_sumar).collect()
    por_temp = (contrib.map(lambda kv: ((kv[0], kv[1][0] or "desconocida"),
                                        (1, int(kv[1][3] == "V"), kv[1][4])))
                .reduceByKey(_sumar).collect())
    return {
        "global": sorted([{"equipo": k, **_rendimiento(k, v)} for k, v in glob],
                         key=lambda x: (-x["PTS"], x["equipo"])),
        "por_temporada": sorted([{"equipo": k[0], "temporada": k[1], **_rendimiento(k, v)}
                                 for k, v in por_temp],
                                key=lambda x: (x["temporada"], -x["PTS"], x["equipo"])),
    }


def q4_distribucion_resultados(partidos):
    """Victorias local / visitante / empates."""
    def clas(p):
        gl, gv = p["goles_local"], p["goles_visitante"]
        return "victoria_local" if gl > gv else "victoria_visitante" if gv > gl else "empate"

    out = dict(partidos.map(lambda p: (clas(p), 1)).reduceByKey(lambda a, b: a + b).collect())
    total = sum(out.values())
    return {k: {"cantidad": out.get(k, 0),
                "porcentaje": round(out.get(k, 0) / total * 100, 2) if total else 0.0}
            for k in ("victoria_local", "empate", "victoria_visitante")}


def q5_promedio_goles(partidos):
    """Promedio de goles por partido (suma y conteo en una sola pasada)."""
    s, n = (partidos.map(lambda p: (p["goles_local"] + p["goles_visitante"], 1))
            .reduce(_sumar))
    return {"partidos": n, "goles_totales": s, "promedio_goles_partido": round(s / n, 3) if n else 0.0}


def q6_partidos_por_periodo(partidos):
    """Partidos por temporada y por mes (AAAA-MM). Los sin fecha cuentan en
    temporada pero no en mes: no se descartan del dataset (enunciado 4.3)."""
    por_temp = (partidos.map(lambda p: (p.get("temporada") or "desconocida", 1))
                .reduceByKey(lambda a, b: a + b).collect())
    por_mes = (partidos.filter(lambda p: p.get("fecha"))
               .map(lambda p: (p["fecha"][:7], 1))
               .reduceByKey(lambda a, b: a + b).collect())
    return {"por_temporada": [{"temporada": k, "partidos": v} for k, v in sorted(por_temp)],
            "por_mes": [{"mes": k, "partidos": v} for k, v in sorted(por_mes)]}


def q7_rendimiento_ofensivo(contrib, top=10):
    """Top equipos por goles a favor (con promedio GF por partido)."""
    out = (contrib.mapValues(lambda c: (c[1], 1))
           .reduceByKey(_sumar)
           .takeOrdered(top, key=lambda kv: (-kv[1][0], kv[0])))
    return [{"equipo": k, "GF": v[0], "PJ": v[1], "GF_por_partido": round(v[0] / v[1], 3)}
            for k, v in out]


def q8_partidos_mas_goles(partidos, top=10):
    """Top encuentros con más goles. takeOrdered evita ordenar todo el RDD."""
    out = partidos.takeOrdered(
        top, key=lambda p: (-(p["goles_local"] + p["goles_visitante"]), p["match_id"]))
    return [{"match_id": p["match_id"],
             "partido": f"{p['equipo_local']} vs {p['equipo_visitante']}",
             "fecha": p.get("fecha"), "temporada": p.get("temporada"),
             "resultado": f"{p['goles_local']}-{p['goles_visitante']}",
             "goles": p["goles_local"] + p["goles_visitante"]} for p in out]


QUERIES = [
    ("q1_estadisticas_por_equipo", "contrib",  q1_estadisticas_por_equipo),
    ("q2_goles_por_equipo",        "contrib",  q2_goles_por_equipo),
    ("q3_puntos_rendimiento",      "contrib",  q3_puntos_rendimiento),
    ("q4_distribucion_resultados", "partidos", q4_distribucion_resultados),
    ("q5_promedio_goles",          "partidos", q5_promedio_goles),
    ("q6_partidos_por_periodo",    "partidos", q6_partidos_por_periodo),
    ("q7_rendimiento_ofensivo",    "contrib",  q7_rendimiento_ofensivo),
    ("q8_partidos_mas_goles",      "partidos", q8_partidos_mas_goles),
]
