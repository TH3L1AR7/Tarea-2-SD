import asyncio
import re
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
COMPETITION = "Primera División"

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


def _infer_estado(home_score, away_score, stage_txt: str = "") -> str:
    """Estado del encuentro. Primero mira el texto de estado de la fila
    (Aplazado / Suspendido / Abandonado...), luego la presencia de marcador."""
    s = (stage_txt or "").lower()
    if any(k in s for k in ("aplaz", "postp", "reprog")):
        return "reprogramado"
    if any(k in s for k in ("susp", "aband", "cancel", "interr")):
        return "suspendido"
    if home_score is not None and away_score is not None:
        return "finalizado"
    return "programado"


async def _extract_match_from_row(
    row, season_year: int | None = None, competicion: str = COMPETITION
) -> dict[str, Any]:
    """Extrae un partido de una fila. `season_year` es el año de la temporada
    (sacado de la URL); se usa para completar fechas que vienen sin año."""
    row_id = await row.get_attribute("id")
    match_id = row_id.split("_")[-1] if row_id else None

    stage_time_el = await row.query_selector('[data-testid="wcl-stageTime"]')
    stage_time = (await stage_time_el.inner_text()).strip() if stage_time_el else None

    # Texto de estado (Aplazado, Suspendido, ...). VERIFICAR selector en el navegador.
    stage_el = await row.query_selector(".event__stage")
    stage_txt = (await stage_el.inner_text()).strip() if stage_el else ""

    home_el = await row.query_selector(".event__homeParticipant")
    away_el = await row.query_selector(".event__awayParticipant")
    home_team = (await home_el.inner_text()).strip() if home_el else None
    away_team = (await away_el.inner_text()).strip() if away_el else None

    home_score_el = await row.query_selector('[data-testid="wcl-tableScore"][data-side="home"]')
    away_score_el = await row.query_selector('[data-testid="wcl-tableScore"][data-side="away"]')
    home_score_raw = (await home_score_el.inner_text()).strip() if home_score_el else None
    away_score_raw = (await away_score_el.inner_text()).strip() if away_score_el else None

    goles_local = _to_int(home_score_raw)
    goles_visitante = _to_int(away_score_raw)

    href_el = await row.query_selector("a.eventRowLink")
    href = await href_el.get_attribute("href") if href_el else None

    parsed_date = _parse_stage_time(stage_time, season_year)
    if season_year is not None:
        temporada = str(season_year)
    else:
        temporada = str(parsed_date.year) if parsed_date else None

    return {
        "_id": match_id,
        "match_id": match_id,
        "fecha": parsed_date.isoformat() if parsed_date else None,
        "fecha_raw": stage_time,                       # texto original, para auditoría
        "temporada": temporada,
        "competicion": competicion,
        "equipo_local": home_team,
        "equipo_visitante": away_team,
        "goles_local": goles_local,
        "goles_visitante": goles_visitante,
        "goles_raw": [home_score_raw, away_score_raw], # texto original, para auditoría
        "estado": _infer_estado(goles_local, goles_visitante, stage_txt),
        "estado_raw": stage_txt,
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
        if target in _normalize_name(m.get("equipo_local") or "")
        or target in _normalize_name(m.get("equipo_visitante") or "")
    ]


async def _scrape_matches_from_url(url: str, d_from, d_to) -> list[dict[str, Any]]:
    async with scrape_page() as page:
        await _goto_and_wait(page, url, MATCH_ROW_SELECTOR)
        rows = await page.query_selector_all(MATCH_ROW_SELECTOR)
        matches = []
        for row in rows:
            m = await _extract_match_from_row(row, season_year=d_from.year)
            parsed_date = datetime.fromisoformat(m["fecha"]).date() if m.get("fecha") else None
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


# ─── Lógica compartida para scraping con botón "show more" ────────────────────

MORE_TEXTS = ["Mostrar más partidos", "Mostrar más", "Show more matches", "Show more"]
MORE_SELECTORS = ["a.event__more", "div.event__more", "[class*='event__more']"]
COOKIE_SELECTORS = ["#onetrust-accept-btn-handler", "button:has-text('Acepto')",
                    "button:has-text('Aceptar')", "button:has-text('I Accept')"]


async def _accept_cookies(page: Page) -> None:
    """El banner de cookies tapa el botón 'Mostrar más'."""
    for sel in COOKIE_SELECTORS:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=1_000):
                await btn.click()
                await asyncio.sleep(0.5)
                return
        except Exception:
            continue


async def _find_more_button(page: Page):
    """Busca el botón 'Mostrar más' por texto y luego por selector. Solo visibles."""
    for txt in MORE_TEXTS:
        loc = page.get_by_text(txt, exact=False)
        for i in range(await loc.count()):
            el = loc.nth(i)
            if await el.is_visible():
                return el
    for sel in MORE_SELECTORS:
        loc = page.locator(sel)
        for i in range(await loc.count()):
            el = loc.nth(i)
            if await el.is_visible():
                return el
    return None


async def _scrape_extended_from_url(
    url: str, max_clicks: int = 10, season_year: int | None = None
) -> list[dict[str, Any]]:
    """Raspa una URL con botón 'Mostrar más'. Se detiene cuando no hay botón o
    cuando un clic no agrega filas nuevas (evita clics inútiles al sitio)."""
    async with scrape_page() as page:
        await _goto_and_wait(page, url, MATCH_ROW_SELECTOR)
        await _accept_cookies(page)

        clicks = 0
        n_rows = await page.locator(MATCH_ROW_SELECTOR).count()
        while clicks < max_clicks:
            btn = await _find_more_button(page)
            if btn is None:
                break
            try:
                await btn.scroll_into_view_if_needed()
                await btn.click(timeout=5_000)
            except Exception:
                try:
                    await btn.evaluate("el => el.click()")
                except Exception:
                    break
            clicks += 1

            nuevas = False
            for _ in range(16):  # hasta ~8 s esperando filas nuevas
                await asyncio.sleep(0.5)
                n_new = await page.locator(MATCH_ROW_SELECTOR).count()
                if n_new > n_rows:
                    n_rows, nuevas = n_new, True
                    break
            if not nuevas:
                break

        print(f"[scraper] {url} clicks={clicks} filas={n_rows}", flush=True)

        all_matches: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for row in await page.query_selector_all(MATCH_ROW_SELECTOR):
            m = await _extract_match_from_row(row, season_year)
            mid = m.get("match_id")
            if mid and mid not in seen_ids:
                seen_ids.add(mid)
                all_matches.append(m)
    return all_matches


async def scrape_team_results_extended(
    team_slug: str, team_id: str, max_clicks: int = 10
) -> list[dict[str, Any]]:
    """Raspa resultados extendidos de un equipo (Q6)."""
    url = _team_url(team_slug, team_id, tab="resultados")
    return await _scrape_extended_from_url(url, max_clicks=max_clicks)


async def scrape_season_results(
    season_url: str, max_clicks: int = 40
) -> list[dict[str, Any]]:
    """Raspa todos los resultados de una temporada completa (Q7).
    Ejemplos de season_url:
      https://cl.soccerway.com/chile/liga-de-primera-2024/resultados/  -> temporada 2024
      https://cl.soccerway.com/chile/liga-de-primera/resultados/       -> temporada actual
    """
    m = re.search(r"liga-de-primera-(\d{4})", season_url)
    season_year = int(m.group(1)) if m else datetime.now().year
    return await _scrape_extended_from_url(season_url, max_clicks=max_clicks, season_year=season_year)


_DATE_RE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})?")


def _parse_stage_time(text: str | None, year: int | None = None) -> Any:
    """Parsea 'dd.mm. hh:mm' o 'dd.mm.aaaa'. Si el texto no trae año usa `year`
    (año de la temporada). Si no hay año disponible devuelve None: nunca se
    inventa el año con datetime.now()."""
    if not text:
        return None
    m = _DATE_RE.match(text.strip())
    if not m:
        return None
    d, mth, y = m.groups()
    y = int(y) if y else year
    if y is None:
        return None
    try:
        return datetime(y, int(mth), int(d)).date()
    except ValueError:
        return None


def _to_int(value) -> int | None:
    """Convierte a int. Valores no numéricos -> None (inválido), no 0."""
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except ValueError:
        return None