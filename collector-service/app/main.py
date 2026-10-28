import asyncio
import os
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError

MONGO_URL   = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
DB_NAME     = os.getenv("MONGO_DB", "soccerway")
COLLECTION  = os.getenv("MONGO_COLLECTION", "partidos")
SCRAPER_URL = os.getenv("SCRAPER_URL", "http://scraper-service:8000")

MAX_CLICKS = 10  # Q6: team pages (current season only)

# Q7: temporadas históricas completas
SEASON_URLS = [
    "https://cl.soccerway.com/chile/liga-de-primera-2024/resultados/",
    "https://cl.soccerway.com/chile/liga-de-primera-2023/resultados/",
    "https://cl.soccerway.com/chile/liga-de-primera-2022/resultados/",
    "https://cl.soccerway.com/chile/liga-de-primera-2021/resultados/",
    "https://cl.soccerway.com/chile/liga-de-primera-2020/resultados/",
    "https://cl.soccerway.com/chile/liga-de-primera-2019/resultados/",
]

_client: AsyncIOMotorClient = None
_col = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _client, _col
    _client = AsyncIOMotorClient(MONGO_URL)
    db = _client[DB_NAME]
    _col = db[COLLECTION]
    await _col.create_index("match_id", unique=True)
    yield
    _client.close()


app = FastAPI(title="Collector Service", lifespan=lifespan)


# ─── helpers ──────────────────────────────────────────────────────────────────

async def _scrape_team_q6(http: httpx.AsyncClient, team: dict) -> list[dict]:
    payload = {
        "query_type": "Q6",
        "params": {
            "team_slug": team["slug"],
            "team_id": team["id"],
            "max_clicks": MAX_CLICKS,
        },
    }
    resp = await http.post(f"{SCRAPER_URL}/scrape", json=payload, timeout=300)
    resp.raise_for_status()
    return resp.json().get("data", [])


async def _scrape_season_q7(http: httpx.AsyncClient, season_url: str) -> list[dict]:
    payload = {
        "query_type": "Q7",
        "params": {
            "season_url": season_url,
            "max_clicks": 40,
        },
    }
    resp = await http.post(f"{SCRAPER_URL}/scrape", json=payload, timeout=600)
    resp.raise_for_status()
    return resp.json().get("data", [])


async def _save_matches(matches: list[dict]) -> tuple[int, int, float]:
    """Inserta partidos finalizados. Retorna (insertados, duplicados, tiempo_total_seg)."""
    inserted = duplicates = 0
    t0 = time.perf_counter()
    for m in matches:
        if m.get("estado") != "finalizado":
            continue
        try:
            await _col.insert_one(m)
            inserted += 1
        except DuplicateKeyError:
            duplicates += 1
    t_write = time.perf_counter() - t0
    return inserted, duplicates, t_write


# ─── endpoints ────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "db": DB_NAME, "collection": COLLECTION}


@app.get("/count")
async def count():
    n = await _col.count_documents({})
    return {"total_en_db": n}


@app.delete("/delete")
async def delete_all():
    result = await _col.delete_many({})
    return {"deleted": result.deleted_count}


@app.post("/collect")
async def collect():
    total_inserted   = 0
    total_duplicates = 0
    total_write_time = 0.0
    errors: list[dict] = []

    async with httpx.AsyncClient() as http:
        # ── Q6: resultados por equipo (temporada actual) ───────────────────
        teams_resp = await http.get(f"{SCRAPER_URL}/teams", timeout=15)
        teams_resp.raise_for_status()
        teams = teams_resp.json()

        q6_tasks = [_scrape_team_q6(http, t) for t in teams]
        q6_results = await asyncio.gather(*q6_tasks, return_exceptions=True)

        for team, result in zip(teams, q6_results):
            if isinstance(result, Exception):
                errors.append({"source": f"Q6/{team['slug']}", "error": str(result)[:120]})
                continue
            ins, dups, tw = await _save_matches(result)
            total_inserted   += ins
            total_duplicates += dups
            total_write_time += tw

        # ── Q7: temporadas históricas completas ────────────────────────────
        q7_tasks = [_scrape_season_q7(http, url) for url in SEASON_URLS]
        q7_results = await asyncio.gather(*q7_tasks, return_exceptions=True)

        for season_url, result in zip(SEASON_URLS, q7_results):
            try:
                year = season_url.split("liga-de-primera-")[1].split("/")[0]
            except IndexError:
                year = season_url
            if isinstance(result, Exception):
                errors.append({"source": f"Q7/{year}", "error": str(result)[:120]})
                continue
            ins, dups, tw = await _save_matches(result)
            total_inserted   += ins
            total_duplicates += dups
            total_write_time += tw

    total_en_db = await _col.count_documents({})
    avg_write = round(total_write_time / total_inserted, 6) if total_inserted > 0 else 0

    return {
        "insertados":              total_inserted,
        "duplicados":              total_duplicates,
        "total_en_db":             total_en_db,
        "t_write_total_seg":       round(total_write_time, 4),
        "t_write_avg_por_doc_seg": avg_write,
        "errores":                 errors,
    }
