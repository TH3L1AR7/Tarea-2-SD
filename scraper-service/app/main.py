from contextlib import asynccontextmanager
from typing import Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from . import scraper


@asynccontextmanager
async def lifespan(app: FastAPI):
    await scraper.init_browser()
    yield
    await scraper.close_browser()


app = FastAPI(title="Scraper Service", lifespan=lifespan)

TEAMS = [
    {"slug": "u-de-chile",      "id": "xW771C6U"},
    {"slug": "colo-colo",       "id": "th4HPIws"},
    {"slug": "coquimbo",        "id": "hWejMZ6h"},
    {"slug": "u-de-concepcion", "id": "6BcMMext"},
    {"slug": "limache",         "id": "YPTUuyqJ"},
    {"slug": "nublense",        "id": "vVvwgdNn"},
    {"slug": "dep-concepcion",  "id": "GKJrZE7I"},
    {"slug": "a-italiano",      "id": "rN3Jxc8g"},
    {"slug": "everton",         "id": "Kr3LOxgm"},
    {"slug": "u-catolica",      "id": "Qe432hiO"},
    {"slug": "la-serena",       "id": "d6kYgx8t"},
    {"slug": "o-higgins",       "id": "hYrshGxg"},
    {"slug": "u-la-calera",     "id": "M34g8ulJ"},
    {"slug": "huachipato",      "id": "SlhZpIaP"},
    {"slug": "palestino",       "id": "U35cjf75"},
    {"slug": "cobresal",        "id": "YeUwzehC"},
]


class ScrapeRequest(BaseModel):
    query_type: str
    params: dict[str, Any] = {}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/teams")
async def get_teams():
    return TEAMS


@app.post("/scrape")
async def scrape_endpoint(req: ScrapeRequest):
    qt = req.query_type
    p = req.params
    try:
        if qt == "Q1":
            data = await scraper.scrape_team_upcoming_matches(p["team_slug"], p["team_id"])
        elif qt == "Q2":
            data = await scraper.scrape_team_recent_matches(p["team_slug"], p["team_id"])
        elif qt == "Q3":
            data = await scraper.scrape_head_to_head(
                p["team_a_slug"], p["team_a_id"],
                p["team_b_slug"], p["team_b_id"],
            )
        elif qt == "Q4":
            data = await scraper.scrape_matches_by_date_range(p["date_from"], p["date_to"])
        elif qt == "Q5":
            data = await scraper.scrape_standings()
        elif qt == "Q6":
            max_clicks = int(p.get("max_clicks", 10))
            data = await scraper.scrape_team_results_extended(
                p["team_slug"], p["team_id"], max_clicks=max_clicks
            )
        elif qt == "Q7":
            max_clicks = int(p.get("max_clicks", 40))
            data = await scraper.scrape_season_results(
                p["season_url"], max_clicks=max_clicks
            )
        else:
            raise HTTPException(status_code=400, detail=f"query_type desconocido: {qt}")
        return {"query_type": qt, "count": len(data), "data": data}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
