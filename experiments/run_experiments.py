"""
Experimentos Tarea 2 - Sistemas Distribuidos
============================================
Ejecuta todos los experimentos y guarda los resultados en results/.

Estructura de salida (igual a Tarea 1):
  results/events.jsonl                              ← log corriente de cada run
  results/mongodb_timings_<timestamp>.json/.csv     ← Sección 5.1
  results/exp1_vary_data_<timestamp>.json/.csv      ← Experimento 1
  results/exp2_vary_partitions_<timestamp>.json/.csv← Experimento 2

Uso:
    python experiments/run_experiments.py

Requiere: pip install requests
"""

import csv
import json
import time
from datetime import datetime
from pathlib import Path

import requests

COLLECTOR = "http://localhost:8001"
SPARK     = "http://localhost:8002"
RESULTS   = Path(__file__).parent / "results"
RESULTS.mkdir(exist_ok=True)

EVENTS_LOG = RESULTS / "events.jsonl"
N_REPS     = 3  # repeticiones por medición


# ─── utilidades ──────────────────────────────────────────────────────────────

def _ts() -> str:
    """Timestamp legible para nombre de archivo."""
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _log_event(event_type: str, label: str, detail: dict | None = None):
    """Agrega una línea al log events.jsonl (igual que Tarea 1)."""
    entry = {
        "event_type": event_type,
        "label":      label,
        "detail":     detail,
        "timestamp":  time.time(),
    }
    with open(EVENTS_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def save_json(label: str, data: dict) -> Path:
    path = RESULTS / f"{label}_{_ts()}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  → {path.name}")
    return path


def save_csv(label: str, rows: list[dict]) -> Path:
    if not rows:
        return None
    path = RESULTS / f"{label}_{_ts()}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"  → {path.name}")
    return path


# ─── llamadas a los servicios ─────────────────────────────────────────────────

def delete_partidos():
    r = requests.delete(f"{COLLECTOR}/delete", timeout=30)
    r.raise_for_status()
    return r.json()


def collect():
    print("  [collect] raspando soccerway... (~10 min)")
    r = requests.post(f"{COLLECTOR}/collect", timeout=1200)
    r.raise_for_status()
    return r.json()


def analyze(sample_fraction: float = 1.0, partitions: int = 1) -> dict:
    r = requests.post(
        f"{SPARK}/analyze",
        params={"sample_fraction": sample_fraction, "partitions": partitions},
        timeout=300,
    )
    r.raise_for_status()
    return r.json()


# ══════════════════════════════════════════════════════════════════════════════
# SECCIÓN 5.1 — Tiempos MongoDB (escritura y lectura, 3 repeticiones)
# ══════════════════════════════════════════════════════════════════════════════

def run_mongodb_timings():
    print("\n=== SECCIÓN 5.1: Tiempos MongoDB ===")
    write_reps: list[dict] = []
    read_reps:  list[dict] = []

    for i in range(1, N_REPS + 1):
        # ── Escritura: delete + collect ─────────────────────────────────────
        print(f"\n  Repetición {i}/{N_REPS} — escritura")
        delete_partidos()
        result = collect()
        wr = {
            "rep":                     i,
            "insertados":              result["insertados"],
            "t_write_total_seg":       result["t_write_total_seg"],
            "t_write_avg_por_doc_seg": result["t_write_avg_por_doc_seg"],
        }
        write_reps.append(wr)
        _log_event("write", f"5.1_rep{i}", wr)
        print(f"     write_total={result['t_write_total_seg']}s  avg/doc={result['t_write_avg_por_doc_seg']}s")

        # ── Lectura: analyze ────────────────────────────────────────────────
        print(f"  Repetición {i}/{N_REPS} — lectura")
        spark = analyze(sample_fraction=1.0, partitions=1)
        rr = {
            "rep":              i,
            "t_mongo_read_seg": spark["t_mongo_read_seg"],
            "partidos":         spark["partidos_analizados"],
        }
        read_reps.append(rr)
        _log_event("read", f"5.1_rep{i}", rr)
        print(f"     read={spark['t_mongo_read_seg']}s")

    avg_write     = sum(r["t_write_total_seg"]       for r in write_reps) / N_REPS
    avg_write_doc = sum(r["t_write_avg_por_doc_seg"] for r in write_reps) / N_REPS
    avg_read      = sum(r["t_mongo_read_seg"]        for r in read_reps)  / N_REPS

    summary = {
        "descripcion":        "Tiempos de escritura y lectura en MongoDB (3 repeticiones)",
        "config":             {"repeticiones": N_REPS},
        "write_repeticiones": write_reps,
        "read_repeticiones":  read_reps,
        "promedios": {
            "t_write_total_seg_avg":       round(avg_write, 4),
            "t_write_avg_por_doc_seg_avg": round(avg_write_doc, 6),
            "t_mongo_read_seg_avg":        round(avg_read, 4),
        },
    }
    save_json("mongodb_timings", summary)

    # CSV: una fila por repetición con escritura + lectura juntas
    csv_rows = []
    for i in range(N_REPS):
        csv_rows.append({
            "rep":                     i + 1,
            "insertados":              write_reps[i]["insertados"],
            "t_write_total_seg":       write_reps[i]["t_write_total_seg"],
            "t_write_avg_por_doc_seg": write_reps[i]["t_write_avg_por_doc_seg"],
            "t_mongo_read_seg":        read_reps[i]["t_mongo_read_seg"],
            "partidos":                read_reps[i]["partidos"],
        })
    save_csv("mongodb_timings", csv_rows)

    print(f"\n  Promedios → write={avg_write:.4f}s  read={avg_read:.4f}s")
    return summary


# ══════════════════════════════════════════════════════════════════════════════
# EXPERIMENTO 1 — Varía tamaño de datos (N, N/2, N/4), particiones fijas en 1
# ══════════════════════════════════════════════════════════════════════════════

def run_experiment1():
    print("\n=== EXPERIMENTO 1: Varía tamaño de datos (partitions=1) ===")
    configs = [
        {"sample_fraction": 1.00, "partitions": 1, "label": "N (100%)"},
        {"sample_fraction": 0.50, "partitions": 1, "label": "N/2 (50%)"},
        {"sample_fraction": 0.25, "partitions": 1, "label": "N/4 (25%)"},
    ]
    runs:     list[dict] = []
    csv_rows: list[dict] = []

    for cfg in configs:
        print(f"\n  [{cfg['label']}] fraction={cfg['sample_fraction']} partitions={cfg['partitions']}")
        spark = analyze(cfg["sample_fraction"], cfg["partitions"])

        run = {
            "label":               cfg["label"],
            "sample_fraction":     cfg["sample_fraction"],
            "partitions":          cfg["partitions"],
            "partidos_analizados": spark["partidos_analizados"],
            "t_mongo_read_seg":    spark["t_mongo_read_seg"],
            "timings_segundos":    spark["timings_segundos"],
        }
        runs.append(run)
        _log_event("exp1", cfg["label"], {k: v for k, v in run.items() if k != "timings_segundos"})
        print(f"     partidos={spark['partidos_analizados']}  read={spark['t_mongo_read_seg']}s")
        print(f"     timings={spark['timings_segundos']}")

        # CSV: una fila por configuración, columnas planas por query
        row = {
            "label":               cfg["label"],
            "sample_fraction":     cfg["sample_fraction"],
            "partitions":          cfg["partitions"],
            "partidos_analizados": spark["partidos_analizados"],
            "t_mongo_read_seg":    spark["t_mongo_read_seg"],
        }
        for q, t in spark["timings_segundos"].items():
            row[f"t_{q}_seg"] = t
        csv_rows.append(row)

    summary = {
        "descripcion": "Experimento 1: varía tamaño de datos, particiones fijas en 1",
        "config":      {"partitions_fijas": 1, "fracciones": [1.0, 0.5, 0.25]},
        "runs":        runs,
    }
    save_json("exp1_vary_data", summary)
    save_csv("exp1_vary_data", csv_rows)
    return summary


# ══════════════════════════════════════════════════════════════════════════════
# EXPERIMENTO 2 — Varía particiones (1, 2, 4), datos fijos en N completo
# ══════════════════════════════════════════════════════════════════════════════

def run_experiment2():
    print("\n=== EXPERIMENTO 2: Varía particiones (sample_fraction=1.0) ===")
    configs = [
        {"sample_fraction": 1.0, "partitions": 1, "label": "1 partición"},
        {"sample_fraction": 1.0, "partitions": 2, "label": "2 particiones"},
        {"sample_fraction": 1.0, "partitions": 4, "label": "4 particiones"},
    ]
    runs:     list[dict] = []
    csv_rows: list[dict] = []

    for cfg in configs:
        print(f"\n  [{cfg['label']}] fraction={cfg['sample_fraction']} partitions={cfg['partitions']}")
        spark = analyze(cfg["sample_fraction"], cfg["partitions"])

        run = {
            "label":               cfg["label"],
            "sample_fraction":     cfg["sample_fraction"],
            "partitions":          cfg["partitions"],
            "partidos_analizados": spark["partidos_analizados"],
            "t_mongo_read_seg":    spark["t_mongo_read_seg"],
            "timings_segundos":    spark["timings_segundos"],
        }
        runs.append(run)
        _log_event("exp2", cfg["label"], {k: v for k, v in run.items() if k != "timings_segundos"})
        print(f"     partidos={spark['partidos_analizados']}  read={spark['t_mongo_read_seg']}s")
        print(f"     timings={spark['timings_segundos']}")

        row = {
            "label":               cfg["label"],
            "sample_fraction":     cfg["sample_fraction"],
            "partitions":          cfg["partitions"],
            "partidos_analizados": spark["partidos_analizados"],
            "t_mongo_read_seg":    spark["t_mongo_read_seg"],
        }
        for q, t in spark["timings_segundos"].items():
            row[f"t_{q}_seg"] = t
        csv_rows.append(row)

    summary = {
        "descripcion": "Experimento 2: varía particiones, datos fijos en N completo",
        "config":      {"fraction_fija": 1.0, "particiones": [1, 2, 4]},
        "runs":        runs,
    }
    save_json("exp2_vary_partitions", summary)
    save_csv("exp2_vary_partitions", csv_rows)
    return summary


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("Verificando servicios...")
    try:
        requests.get(f"{COLLECTOR}/health", timeout=5).raise_for_status()
        requests.get(f"{SPARK}/health",     timeout=5).raise_for_status()
        print("  ✓ collector y spark responden")
    except Exception as e:
        print(f"  ✗ Error: {e}")
        print("  Asegúrate de que docker-compose up -d esté corriendo.")
        exit(1)

    _log_event("session_start", "all_experiments", {"timestamp_iso": datetime.now().isoformat()})
    t_total = time.perf_counter()

    # Sección 5.1 (3 × delete+collect ≈ 30 min)
    run_mongodb_timings()

    # Experimentos (la DB tiene datos al finalizar 5.1)
    run_experiment1()
    run_experiment2()

    elapsed = time.perf_counter() - t_total
    _log_event("session_end", "all_experiments", {"elapsed_s": round(elapsed, 1)})

    print(f"\n{'='*60}")
    print(f"Todos los experimentos completados en {elapsed:.1f}s")
    print(f"Resultados en: experiments/results/")
    print(f"  events.jsonl                  ← log corriente")
    print(f"  mongodb_timings_*.json/.csv   ← Sección 5.1")
    print(f"  exp1_vary_data_*.json/.csv    ← Experimento 1")
    print(f"  exp2_vary_partitions_*.json/.csv ← Experimento 2")
