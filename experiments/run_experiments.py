"""
Experimentos Tarea 2 - Sistemas Distribuidos
============================================
Uso (con `docker compose up -d --build` corriendo):

    pip install requests matplotlib        # matplotlib es opcional (gráficos)
    python experiments/run_experiments.py               # raspa 1 vez + todo
    python experiments/run_experiments.py --skip-collect  # usa el snapshot ya raspado

Qué hace:
  0. (opcional) POST /collect: raspa Soccerway UNA sola vez y guarda snapshot.
  1. Sección 5.1: REPS x (reingesta completa en MongoDB) y REPS x (lectura MongoDB -> RDD).
  2. Warm-up: 1 ejecución descartada (arranque de JVM / workers Python).
  3. Experimento 1: N, N/2, N/4 con 1 partición, REPS repeticiones cada uno.
  4. Experimento 2: 1, 2, 4 particiones con N completo, REPS repeticiones cada uno.
  5. Verifica que los resultados sean idénticos entre particiones.

Salida en experiments/results/<timestamp>/ : JSON crudo, CSV resumen y PNG.
"""

import argparse
import csv
import json
import statistics as st
import sys
from datetime import datetime
from pathlib import Path

import requests

COLLECTOR = "http://localhost:8001"
SPARK     = "http://localhost:8002"
REPS      = 3
QUERIES   = [f"q{i}" for i in range(1, 9)]

OUT = Path(__file__).parent / "results" / datetime.now().strftime("%Y%m%d-%H%M%S")


def _post(url, **kw):
    r = requests.post(url, timeout=kw.pop("timeout", 600), **kw)
    r.raise_for_status()
    return r.json()


def _save_json(name, data):
    (OUT / f"{name}.json").write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def _save_csv(name, rows):
    if not rows:
        return
    with open(OUT / f"{name}.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def _mean_std(xs):
    return round(st.mean(xs), 4), round(st.stdev(xs), 4) if len(xs) > 1 else 0.0


def analyze(fraction, parts):
    return _post(f"{SPARK}/analyze", params={"sample_fraction": fraction, "partitions": parts})


# ─── Sección 5.1 ─────────────────────────────────────────────────────────────

def mongodb_timings():
    print("\n=== 5.1 MongoDB: escritura y lectura ===")
    writes, reads = [], []
    for i in range(1, REPS + 1):
        w = _post(f"{COLLECTOR}/reingest")
        r = _post(f"{SPARK}/read-benchmark")
        writes.append(w)
        reads.append(r)
        print(f"  rep {i}: write={w['t_write_total_seg']}s ({w['insertados']} docs, "
              f"{w['t_write_avg_por_doc_seg']} s/doc) | read={r['t_mongo_read_seg']}s + rdd={r['t_rdd_load_seg']}s")
    rows = [{"rep": i + 1, "insertados": w["insertados"],
             "t_write_total_seg": w["t_write_total_seg"], "t_write_avg_por_doc_seg": w["t_write_avg_por_doc_seg"],
             "t_mongo_read_seg": r["t_mongo_read_seg"], "t_rdd_load_seg": r["t_rdd_load_seg"],
             "t_read_total_seg": r["t_total_seg"]} for i, (w, r) in enumerate(zip(writes, reads))]
    prom = {k: _mean_std([row[k] for row in rows])[0] for k in rows[0] if k not in ("rep", "insertados")}
    rows.append({"rep": "promedio", "insertados": rows[0]["insertados"], **prom})
    _save_json("sec51_mongodb", {"writes": writes, "reads": reads})
    _save_csv("sec51_mongodb", rows)
    return rows


# ─── Experimentos Spark ──────────────────────────────────────────────────────

def experimento(nombre, configs):
    print(f"\n=== {nombre} ===")
    crudo, filas, firmas = [], [], {}
    for label, fraction, parts in configs:
        runs = [analyze(fraction, parts) for _ in range(REPS)]
        crudo.append({"label": label, "runs": runs})
        fila = {"config": label, "fraction": fraction, "partitions": parts,
                "partidos": runs[0]["partidos_analizados"]}
        for q in QUERIES:
            m, s = _mean_std([r["timings_queries_seg"][q] for r in runs])
            fila[f"{q}_mean"], fila[f"{q}_std"] = m, s
        for k in ("t_queries_total", "t_cache", "t_limpieza", "t_mongo_read", "t_rdd_load", "t_total"):
            fila[f"{k}_mean"], fila[f"{k}_std"] = _mean_std([r["tiempos_seg"][k] for r in runs])
        filas.append(fila)
        firmas[label] = json.dumps(runs[0]["resultados"], sort_keys=True, ensure_ascii=False)
        print(f"  {label:<14} partidos={fila['partidos']:<5} queries={fila['t_queries_total_mean']}s "
              f"(±{fila['t_queries_total_std']})")
    _save_json(nombre, crudo)
    _save_csv(nombre, filas)
    return filas, firmas, crudo


def graficar(filas, nombre, eje):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("  (matplotlib no instalado: se omiten gráficos)")
        return
    x = [f["config"] for f in filas]
    fig, ax = plt.subplots(figsize=(9, 4.5))
    w = 0.8 / len(filas)
    for i, f in enumerate(filas):
        ax.bar([j + i * w for j in range(len(QUERIES))], [f[f"{q}_mean"] for q in QUERIES], w,
               yerr=[f[f"{q}_std"] for q in QUERIES], label=x[i], capsize=2)
    ax.set_xticks([j + w * (len(filas) - 1) / 2 for j in range(len(QUERIES))], [q.upper() for q in QUERIES])
    ax.set_ylabel("tiempo (s)")
    ax.set_title(f"{nombre}: tiempo por consulta (media ± desv., {REPS} reps) — {eje}")
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT / f"{nombre}.png", dpi=150)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-collect", action="store_true", help="no raspar; usar snapshot existente")
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)

    for url in (COLLECTOR, SPARK):
        requests.get(f"{url}/health", timeout=5).raise_for_status()

    if not args.skip_collect:
        print("[collect] raspando Soccerway (una sola vez)...")
        c = _post(f"{COLLECTOR}/collect", timeout=3600)
        _save_json("collect", c)
        print(f"  recolectados={c['recolectados']} insertados={c['insertados']} "
              f"descartados={c['descartados']} dup_lote={c['duplicados_en_lote']} dup_db={c['duplicados_en_db']}")
        if c["errores"]:
            print(f"  ERRORES: {c['errores']}")

    mongodb_timings()

    print("\n[warm-up] ejecución descartada")
    warm = analyze(1.0, 1)
    _save_json("limpieza", warm["limpieza"])
    print(f"  limpieza: {warm['limpieza']['etapas']}")
    n = warm["partidos_analizados"]
    print(f"  N válidos = {n} {'(OK >= 1000)' if n >= 1000 else '(¡MENOS DE 1000! revisar recolección)'}")

    f1, _, _ = experimento("exp1_volumen", [("N", 1.0, 1), ("N/2", 0.5, 1), ("N/4", 0.25, 1)])
    graficar(f1, "exp1_volumen", "1 partición")

    f2, firmas, _ = experimento("exp2_particiones",
                                [("1 partición", 1.0, 1), ("2 particiones", 1.0, 2), ("4 particiones", 1.0, 4)])
    graficar(f2, "exp2_particiones", f"N={n}")

    iguales = len(set(firmas.values())) == 1
    print(f"\n[verificación] resultados idénticos con 1/2/4 particiones: {'SÍ' if iguales else 'NO'}")
    _save_json("verificacion_particiones", {"identicos": iguales})
    print(f"\nResultados en {OUT}")
    sys.exit(0 if iguales else 1)


if __name__ == "__main__":
    main()
