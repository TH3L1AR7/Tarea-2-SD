import json
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any

from . import scraper

TEAMS_PATH = Path(__file__).resolve().parents[2] / "shared" / "teams.json"
_teams_by_code: dict[str, dict[str, str]] = {}


class ScrapeRequest(BaseModel):
    query_type: str
    params: dict[str, Any] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _teams_by_code
    with open(TEAMS_PATH, encoding="utf-8") as f:
        raw = json.load(f)
    _teams_by_code = {t["code"]: t for t in raw["teams"]}
    await scraper.init_browser()
    yield
    await scraper.close_browser()


app = FastAPI(lifespan=lifespan)


def _resolve_team(code: str) -> dict[str, str]:
    team = _teams_by_code.get(code)
    if team is None:
        raise HTTPException(status_code=422, detail=f"Equipo desconocido: {code}")
    return team


@app.get("/health")
async def health():
    return {"status": "ok", "teams_loaded": len(_teams_by_code)}


@app.post("/scrape")
async def scrape(req: ScrapeRequest):
    try:
        if req.query_type == "Q1":
            team = _resolve_team(req.params["team"])
            data = await scraper.scrape_team_upcoming_matches(team["slug"], team["id"])
        elif req.query_type == "Q2":
            team = _resolve_team(req.params["team"])
            data = await scraper.scrape_team_recent_matches(team["slug"], team["id"])
        elif req.query_type == "Q3":
            team_a = _resolve_team(req.params["team_a"])
            team_b = _resolve_team(req.params["team_b"])
            data = await scraper.scrape_head_to_head(
                team_a["slug"], team_a["id"], team_b["slug"], team_b["id"]
            )
        elif req.query_type == "Q4":
            data = await scraper.scrape_matches_by_date_range(
                req.params["date_from"], req.params["date_to"]
            )
        elif req.query_type == "Q5":
            data = await scraper.scrape_standings()
        else:
            raise HTTPException(status_code=400, detail=f"query_type desconocido: {req.query_type}")

        return {"query_type": req.query_type, "data": data}

    except HTTPException:
        raise
    except KeyError as e:
        raise HTTPException(status_code=422, detail=f"Falta parámetro requerido: {e}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Error de scraping: {e}")  