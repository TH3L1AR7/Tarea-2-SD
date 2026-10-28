"""
Collector Service — Recolección + Ingesta en MongoDB (pasos 1 y 2 del flujo).

Flujo:
  POST /collect   -> raspa las temporadas de la Liga de Primera vía scraper-service,
                     guarda un snapshot JSON de lo raspado y lo ingesta en MongoDB.
  POST /reingest  -> borra la colección y vuelve a insertar el último snapshot
                     (para medir escritura 3+ veces SIN volver a raspar Soccerway).

Decisiones:
  - Solo se usan páginas de liga (liga-de-primera[-AAAA]/resultados/): todo lo que
    entra es Primera División y la temporada sale de la URL. Las páginas por equipo
    (Q6 de la T1) mezclan Copa Libertadores / Sudamericana / Copa Chile.
  - En MongoDB se guardan los datos CRUDOS: también partidos programados/suspendidos.
    Spark decide qué usar en cada análisis (enunciado 4.3: no eliminar incompletos
    indiscriminadamente). Solo se descartan registros no identificables.
  - Deduplicación: _id = match_id (índice único implícito) + índice único en match_id.
  - Inserción: insert_many(ordered=False) en lote; los duplicados (E11000) se cuentan.
"""

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import BulkWriteError

MONGO_URL     = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
DB_NAME       = os.getenv("MONGO_DB", "soccerway")
COLLECTION    = os.getenv("MONGO_COLLECTION", "partidos")
SCRAPER_URL   = os.getenv("SCRAPER_URL", "http://scraper-service:8000")
SNAPSHOT_PATH = Path(os.getenv("SNAPSHOT_PATH", "/data/raw_snapshot.json"))
MAX_CLICKS    = int(os.getenv("MAX_CLICKS", "60"))

BASE = "https://cl.soccerway.com/chile"
# Temporada actual (sin sufijo) + históricas. VERIFICAR que cada URL exista en Soccerway.
SEASON_URLS = [f"{BASE}/liga-de-primera/resultados/"] + [
    f"{BASE}/liga-de-primera-{y}/resultados/" for y in range(2025, 2018, -1)
]

_client: AsyncIOMotorClient = None
_col = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _client, _col
    _client = AsyncIOMotorClient(MONGO_URL)
    _col = _client[DB_NAME][COLLECTION]
    await _col.create_index("match_id", unique=True)
    await _col.create_index([("temporada", 1), ("estado", 1)])
    yield
    _client.close()


app = FastAPI(title="Collector Service", lifespan=lifespan)


# ─── helpers ──────────────────────────────────────────────────────────────────

async def _scrape_season(http: httpx.AsyncClient, season_url: str) -> list[dict]:
    payload = {"query_type": "Q7", "params": {"season_url": season_url, "max_clicks": MAX_CLICKS}}
    resp = await http.post(f"{SCRAPER_URL}/scrape", json=payload, timeout=900)
    resp.raise_for_status()
    return resp.json().get("data", [])


def _motivo_descarte(m: dict) -> str | None:
    """Solo se descarta lo que no se puede identificar ni asociar a equipos.
    Todo lo demás se guarda crudo y lo filtra Spark."""
    if not m.get("match_id"):
        return "sin_match_id"
    if not m.get("equipo_local") or not m.get("equipo_visitante"):
        return "sin_equipos"
    return None


def _preparar(matches: list[dict]) -> tuple[list[dict], dict, int]:
    """Valida y deduplica dentro del lote. Retorna (docs, descartados_por_motivo, dup_lote)."""
    docs, seen = [], set()
    descartados: dict[str, int] = {}
    dup_lote = 0
    for m in matches:
        motivo = _motivo_descarte(m)
        if motivo:
            descartados[motivo] = descartados.get(motivo, 0) + 1
            continue
        if m["match_id"] in seen:
            dup_lote += 1
            continue
        seen.add(m["match_id"])
        m["_id"] = m["match_id"]
        docs.append(m)
    return docs, descartados, dup_lote


async def _insertar(docs: list[dict]) -> tuple[int, int, float]:
    """insert_many sin orden: sigue aunque haya duplicados. Retorna (insertados, dup_db, t_seg)."""
    if not docs:
        return 0, 0, 0.0
    t0 = time.perf_counter()
    try:
        res = await _col.insert_many(docs, ordered=False)
        inserted, dup_db = len(res.inserted_ids), 0
    except BulkWriteError as e:
        errs = e.details.get("writeErrors", [])
        dup_db = sum(1 for w in errs if w.get("code") == 11000)
        otros = [w for w in errs if w.get("code") != 11000]
        if otros:
            raise HTTPException(500, f"Errores de escritura no-duplicado: {otros[:3]}")
        inserted = e.details.get("nInserted", 0)
    return inserted, dup_db, time.perf_counter() - t0


async def _ingestar(matches: list[dict]) -> dict:
    docs, descartados, dup_lote = _preparar(matches)
    inserted, dup_db, t_write = await _insertar(docs)
    estados: dict[str, int] = {}
    for d in docs:
        estados[d.get("estado") or "None"] = estados.get(d.get("estado") or "None", 0) + 1
    return {
        "recolectados":            len(matches),
        "insertados":              inserted,
        "descartados":             sum(descartados.values()),
        "descartados_por_motivo":  descartados,
        "duplicados_en_lote":      dup_lote,
        "duplicados_en_db":        dup_db,
        "estados_en_lote":         estados,
        "total_en_db":             await _col.count_documents({}),
        "t_write_total_seg":       round(t_write, 4),
        "t_write_avg_por_doc_seg": round(t_write / inserted, 6) if inserted else 0,
    }


# ─── endpoints ────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "db": DB_NAME, "collection": COLLECTION}


@app.get("/count")
async def count():
    total = await _col.count_documents({})
    por_estado = await _col.aggregate([{"$group": {"_id": "$estado", "n": {"$sum": 1}}}]).to_list(None)
    por_temporada = await _col.aggregate(
        [{"$group": {"_id": "$temporada", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]
    ).to_list(None)
    return {"total_en_db": total,
            "por_estado": {d["_id"]: d["n"] for d in por_estado},
            "por_temporada": {d["_id"]: d["n"] for d in por_temporada}}


@app.delete("/delete")
async def delete_all():
    result = await _col.delete_many({})
    return {"deleted": result.deleted_count}


@app.post("/collect")
async def collect():
    """Paso 1 + 2: raspa todas las temporadas, guarda snapshot e ingesta."""
    errors: list[dict] = []
    por_temporada: dict[str, int] = {}
    all_matches: list[dict] = []

    t0 = time.perf_counter()
    async with httpx.AsyncClient() as http:
        results = await asyncio.gather(*[_scrape_season(http, u) for u in SEASON_URLS],
                                       return_exceptions=True)
    t_scrape = time.perf_counter() - t0

    for url, res in zip(SEASON_URLS, results):
        tag = url.split("/chile/")[1].split("/")[0]
        if isinstance(res, Exception):
            errors.append({"source": tag, "error": str(res)[:200]})
            continue
        por_temporada[tag] = len(res)
        all_matches.extend(res)

    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_PATH.write_text(json.dumps(all_matches, ensure_ascii=False), encoding="utf-8")

    resumen = await _ingestar(all_matches)
    return {"t_scrape_seg": round(t_scrape, 2),
            "recolectados_por_pagina": por_temporada,
            **resumen,
            "errores": errors}


@app.post("/reingest")
async def reingest():
    """Borra la colección e inserta el último snapshot (medición 5.1 sin re-raspar)."""
    if not SNAPSHOT_PATH.exists():
        raise HTTPException(404, "No hay snapshot. Ejecuta POST /collect primero.")
    matches = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
    await _col.delete_many({})
    return await _ingestar(matches)
