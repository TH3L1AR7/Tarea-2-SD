import asyncio
import unicodedata
from datetime import datetime
from typing import Any

from playwright.async_api import async_playwright, Browser, Page
from contextlib import asynccontextmanager

BASE_URL = "https://cl.soccerway.com/chile/liga-de-primera"

URLS = {
    "standings": f"{BASE_URL}/tabla-de-posiciones/",
    "results": f"{BASE_URL}/resultados/",
    "fixtures": f"{BASE_URL}/partidos/",
}

MATCH_ROW_SELECTOR = "div.event__match"

_browser: Browser | None = None
_playwright = None
_semaphore = asyncio.Semaphore(4)


async def init_browser() -> None:
    global _browser, _playwright
    _playwright = await async_playwright().start()
    _browser = await _playwright.chromium.launch(headless=True)


async def close_browser() -> None:
    global _browser, _playwright
    if _browser is not None:
        await _browser.close()
    if _playwright is not None:
        await _playwright.stop()


@asynccontextmanager
async def scrape_page():
    assert _browser is not None
    async with _semaphore:
        context = await _browser.new_context(locale="es-CL")
        page = await context.new_page()
        try:
            yield page
        finally:
            await page.close()
            await context.close()


async def _goto_and_wait(page: Page, url: str, selector: str, attempts: int = 3, timeout: int = 10_000) -> None:
    for attempt in range(attempts):
        try:
            await page.goto(url, wait_until="domcontentloaded")
            await page.wait_for_selector(selector, timeout=timeout)
            return
        except Exception:
            if attempt == attempts - 1:
                raise
            await asyncio.sleep(1.5 ** attempt)


async def scrape_standings() -> list[dict[str, Any]]:
    row_selector = "div.ui-table__row"
    async with scrape_page() as page:
        await _goto_and_wait(page, URLS["standings"], row_selector)
        rows = await page.query_selector_all(row_selector)
        standings = []
        for row in rows:
            participant_el = await row.query_selector(
                ".table__cell--participant .tableCellParticipant"
            )
            if participant_el is None:
                continue
            team_name = (await participant_el.inner_text()).strip()

            value_cells = await row.query_selector_all(".table__cell--value")
            values = [(await c.inner_text()).strip() for c in value_cells]
            if len(values) < 7:
                continue

            played, won, drawn, lost = values[0], values[1], values[2], values[3]
            gf_ga = values[4]
            goals_for, _, goals_against = gf_ga.partition(":")
            points = values[6]

            standings.append({
                "team": team_name,
                "played": _to_int(played),
                "won": _to_int(won),
                "drawn": _to_int(drawn),
                "lost": _to_int(lost),
                "goals_for": _to_int(goals_for),
                "goals_against": _to_int(goals_against),
                "points": _to_int(points),
            })
        return standings


def _normalize_name(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return "".join(c for c in text.lower() if c.isalnum())


async def _extract_match_from_row(row) -> dict[str, Any]:
    row_id = await row.get_attribute("id")
    match_id = row_id.split("_")[-1] if row_id else None

    stage_time_el = await row.query_selector('[data-testid="wcl-stageTime"]')
    stage_time = (await stage_time_el.inner_text()).strip() if stage_time_el else None

    home_el = await row.query_selector(".event__homeParticipant")
    away_el = await row.query_selector(".event__awayParticipant")
    home_team = (await home_el.inner_text()).strip() if home_el else None
    away_team = (await away_el.inner_text()).strip() if away_el else None

    home_score_el = await row.query_selector('[data-testid="wcl-tableScore"][data-side="home"]')
    away_score_el = await row.query_selector('[data-testid="wcl-tableScore"][data-side="away"]')
    home_score = (await home_score_el.inner_text()).strip() if home_score_el else None
    away_score = (await away_score_el.inner_text()).strip() if away_score_el else None

    href_el = await row.query_selector("a.eventRowLink")
    href = await href_el.get_attribute("href") if href_el else None

    return {
        "match_id": match_id,
        "date_or_time": stage_time,
        "home_team": home_team,
        "away_team": away_team,
        "home_score": _to_int(home_score) if home_score else None,
        "away_score": _to_int(away_score) if away_score else None,
        "url": href,
    }


def _team_url(team_slug: str, team_id: str, tab: str = "") -> str:
    base = f"https://cl.soccerway.com/equipo/{team_slug}/{team_id}/"
    return f"{base}{tab}/" if tab else base


async def scrape_team_recent_matches(team_slug: str, team_id: str) -> list[dict[str, Any]]:
    url = _team_url(team_slug, team_id, tab="resultados")
    async with scrape_page() as page:
        await _goto_and_wait(page, url, MATCH_ROW_SELECTOR)
        rows = await page.query_selector_all(MATCH_ROW_SELECTOR)
        return [await _extract_match_from_row(r) for r in rows]


async def scrape_team_upcoming_matches(team_slug: str, team_id: str) -> list[dict[str, Any]]:
    url = _team_url(team_slug, team_id, tab="partidos")
    async with scrape_page() as page:
        await _goto_and_wait(page, url, MATCH_ROW_SELECTOR)
        rows = await page.query_selector_all(MATCH_ROW_SELECTOR)
        return [await _extract_match_from_row(r) for r in rows]


async def scrape_head_to_head(
    team_a_slug: str, team_a_id: str, team_b_slug: str, team_b_id: str
) -> list[dict[str, Any]]:
    all_matches = await scrape_team_recent_matches(team_a_slug, team_a_id)
    target = _normalize_name(team_b_slug.replace("-", " "))
    return [
        m for m in all_matches
        if target in _normalize_name(m.get("home_team") or "")
        or target in _normalize_name(m.get("away_team") or "")
    ]


async def _scrape_matches_from_url(url: str, d_from, d_to) -> list[dict[str, Any]]:
    async with scrape_page() as page:
        await _goto_and_wait(page, url, MATCH_ROW_SELECTOR)
        rows = await page.query_selector_all(MATCH_ROW_SELECTOR)
        matches = []
        for row in rows:
            m = await _extract_match_from_row(row)
            parsed_date = _parse_stage_time(m.get("date_or_time"))
            if parsed_date and d_from <= parsed_date <= d_to:
                matches.append(m)
        return matches


async def scrape_matches_by_date_range(date_from: str, date_to: str) -> list[dict[str, Any]]:
    d_from = datetime.fromisoformat(date_from).date()
    d_to = datetime.fromisoformat(date_to).date()
    results, fixtures = await asyncio.gather(
        _scrape_matches_from_url(URLS["results"], d_from, d_to),
        _scrape_matches_from_url(URLS["fixtures"], d_from, d_to),
    )
    seen = set()
    merged = []
    for m in results + fixtures:
        if m["match_id"] not in seen:
            seen.add(m["match_id"])
            merged.append(m)
    return merged


def _parse_stage_time(text: str | None, year: int | None = None) -> Any:
    if not text:
        return None
    text = text.strip()
    date_part = text.split(" ")[0].rstrip(".")
    try:
        day_str, month_str = date_part.split(".")
        y = year or datetime.now().year
        return datetime(y, int(month_str), int(day_str)).date()
    except (ValueError, AttributeError):
        return None


def _to_int(value: str) -> int:
    try:
        return int(value.strip())
    except ValueError:
        return 0


