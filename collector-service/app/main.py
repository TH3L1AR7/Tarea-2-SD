import os
import asyncio
import httpx
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi import FastAPI
from contextlib import asynccontextmanager

SCRAPER_URL = os.getenv("SCRAPER_URL", "http://scraper-service:8000")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB = os.getenv("MONGO_DB", "soccerway")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "partidos")

_mongo: AsyncIOMotorClient = None
_collection = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _mongo, _collection
    _mongo = AsyncIOMotorClient(MONGO_URI)
    db = _mongo[MONGO_DB]
    _collection = db[MONGO_COLLECTION]
    # Índice único por match_id para deduplicación automática
    await _collection.create_index("match_id", unique=True)
    yield
    _mongo.close()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    count = await _collection.count_documents({})
    return {"status": "ok", "partidos_en_db": count}


@app.post("/collect")
async def collect():
    """
    Recorre todos los equipos, scrapea sus partidos históricos
    y los guarda en MongoDB con deduplicación automática.
    """
    async with httpx.AsyncClient(timeout=120) as client:
        # 1. Obtener lista de equipos
        resp = await client.get(f"{SCRAPER_URL}/teams")
        resp.raise_for_status()
        teams = resp.json()["teams"]

        insertados = 0
        descartados = 0
        errores = 0

        # 2. Scrapear partidos de cada equipo (Q2 = últimos partidos)
        for team in teams:
            try:
                r = await client.post(
                    f"{SCRAPER_URL}/scrape",
                    json={"query_type": "Q2", "params": {"team": team["code"]}},
                    timeout=120,
                )
                r.raise_for_status()
                partidos = r.json().get("data", [])

                # 3. Guardar en MongoDB, ignorar duplicados
                for partido in partidos:
                    if not partido.get("match_id"):
                        descartados += 1
                        continue
                    # Solo partidos finalizados
                    if partido.get("estado") != "finalizado":
                        descartados += 1
                        continue
                    # Campos incompletos
                    if partido.get("equipo_local") is None or partido.get("equipo_visitante") is None:
                        descartados += 1
                        continue

                    try:
                        await _collection.insert_one({
                            "_id": partido["match_id"],
                            **{k: v for k, v in partido.items() if k != "_id"},
                        })
                        insertados += 1
                    except Exception:
                        # Duplicado — ya existe en la colección
                        descartados += 1

            except Exception as e:
                errores += 1
                print(f"Error scrapeando {team['code']}: {e}")

            # Pausa entre equipos para no saturar Soccerway
            await asyncio.sleep(2)

        total = await _collection.count_documents({})
        return {
            "insertados": insertados,
            "descartados_o_duplicados": descartados,
            "errores": errores,
            "total_en_db": total,
        }


@app.delete("/collection")
async def clear_collection():
    """Limpia la colección (útil para re-recolectar desde cero)."""
    result = await _collection.delete_many({})
    return {"eliminados": result.deleted_count}
