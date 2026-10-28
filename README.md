# Tarea 2 — Procesamiento Distribuido de Datos (Grupo 10)

Flujo: **Soccerway → scraper → collector → MongoDB → Spark (RDD) → MongoDB (`resultados`)**.

## Arquitectura

| Servicio | Puerto | Responsabilidad |
|---|---|---|
| `mongodb` | 27017 | Almacenamiento persistente. `partidos` (crudos) y `resultados` (agregados). Volumen `mongo_data`. |
| `scraper-service` | 8000 | Scraper de la Tarea 1 (Playwright), adaptado: fecha con año de la temporada, estado del partido, marcador inválido → `null`. |
| `collector-service` | 8001 | Recolección de todas las temporadas de la Liga de Primera + ingesta en MongoDB con conteo de insertados / descartados / duplicados. Guarda un snapshot JSON (volumen `collector_data`). |
| `spark-service` | 8002 | Carga MongoDB → RDD, limpieza, contribuciones por equipo, 8 consultas, guarda resultados. |

Código Spark: `spark-service/app/pipeline.py` (lógica) y `spark-service/app/main.py` (API).

## Requisitos

- Docker y Docker Compose v2
- Python 3.10+ en el host solo para los experimentos: `pip install requests matplotlib`

## Ejecución

```bash
cp .env.example .env
docker compose up -d --build

# 1–2. Recolección + ingesta (raspa Soccerway; tarda varios minutos)
curl -X POST localhost:8001/collect
curl localhost:8001/count            # total, por estado y por temporada

# 3–7. Limpieza + 8 consultas + persistencia en `resultados`
curl -X POST "localhost:8002/analyze?sample_fraction=1.0&partitions=1"
```

Ver resultados guardados:
```bash
docker compose exec mongodb mongosh soccerway --eval 'db.resultados.find({}, {resultados:0}).pretty()'
```

## Endpoints

| Método | Ruta | Descripción |
|---|---|---|
| POST | `:8001/collect` | Raspa todas las temporadas, guarda snapshot e ingesta. |
| POST | `:8001/reingest` | Vacía `partidos` y vuelve a insertar el snapshot (mide escritura sin re-raspar). |
| GET | `:8001/count` | Conteos por estado y temporada. |
| DELETE | `:8001/delete` | Vacía `partidos`. |
| POST | `:8002/analyze?sample_fraction=&partitions=&seed=` | Pipeline completo. Devuelve reporte de limpieza, tiempos y resultados. |
| POST | `:8002/read-benchmark` | Solo lectura MongoDB → RDD (sección 5.1). |

## Validación de corrección

```bash
docker compose exec spark-service python -m app.validate
```
Ejecuta las reglas de limpieza y las 8 consultas sobre 12 partidos conocidos (5 válidos + 7 sucios),
compara contra valores calculados a mano y verifica que los resultados sean idénticos con 1, 2 y 4 particiones.

## Experimentos

```bash
python experiments/run_experiments.py                 # raspa 1 vez y corre todo
python experiments/run_experiments.py --skip-collect  # reutiliza el snapshot
```
Genera `experiments/results/<timestamp>/`:

| Archivo | Contenido |
|---|---|
| `collect.json` | Recolectados, insertados, descartados (por motivo), duplicados. |
| `sec51_mongodb.csv` | 3 repeticiones de escritura (`/reingest`) y lectura (`/read-benchmark`) + promedio. |
| `limpieza.json` | Registros por etapa de limpieza y descartes por regla. |
| `exp1_volumen.csv/.png` | N, N/2, N/4 con 1 partición (media ± desv. de 3 reps por consulta). |
| `exp2_particiones.csv/.png` | 1, 2, 4 particiones con N completo. |
| `verificacion_particiones.json` | Resultados idénticos entre particiones (sí/no). |

Se ejecuta 1 corrida de calentamiento descartada antes de medir (arranque de JVM y workers Python).

## Decisiones de diseño

**Recolección.** Solo páginas de liga (`liga-de-primera[-AAAA]/resultados/`, temporadas 2019 a la actual): todo es
Primera División y la temporada se toma de la URL. Las páginas por equipo de la T1 mezclaban copas internacionales.

**Esquema del documento (`partidos`).**
```json
{ "_id": "<match_id>", "match_id": "...", "fecha": "2024-03-01", "fecha_raw": "01.03. 20:00",
  "temporada": "2024", "competicion": "Primera División",
  "equipo_local": "...", "equipo_visitante": "...", "goles_local": 2, "goles_visitante": 1,
  "goles_raw": ["2", "1"], "estado": "finalizado|programado|suspendido|reprogramado",
  "estado_raw": "...", "url": "..." }
```
Los campos `*_raw` guardan el texto original para auditar la limpieza.

**Deduplicación e índices.** `_id = match_id` + índice único en `match_id` → una re-ejecución no duplica.
Índice secundario `(temporada, estado)` para conteos. Inserción con `insert_many(ordered=False)`: un solo
round-trip por lote; los duplicados (E11000) se cuentan y no detienen la inserción.

**Datos crudos.** MongoDB guarda también partidos no finalizados; el collector solo descarta registros sin
`match_id` o sin equipos. El filtrado de partidos válidos lo hace Spark.

**Limpieza en Spark (orden de aplicación).**

| # | Regla | Motivo |
|---|---|---|
| R1 | Requiere `match_id`, local y visitante | Sin esto no hay contribución por equipo. |
| R2 | Solo `estado == "finalizado"` | Programados/suspendidos no tienen resultado definitivo. |
| R3 | Goles enteros, `0 ≤ g ≤ 20` | Detecta errores de scraping (null, negativos, basura). |
| R4 | Normalización de nombres (quita tarjetas rojas `\n2`, sufijo `(Chi)`, espacios; alias de `shared/teams.json`) y descarta local == visitante | Un mismo equipo no puede aparecer con dos claves. |
| R5 | Duplicado por `match_id` | Criterio primario. |
| R6 | Duplicado por `(fecha, local, visitante)` | Mismo partido con distinto id. Los sin fecha no se descartan. |

Los partidos sin fecha **no** se eliminan: cuentan en todas las consultas excepto "partidos por mes" (Q6).

**Contribuciones.** Cada partido → 2 registros `(equipo, (temporada, GF, GC, resultado, puntos))` (`flatMap`).
Q1, Q2, Q3 y Q7 hacen `reduceByKey` sobre este RDD; Q4, Q5, Q6 y Q8 sobre el RDD de partidos.

**Memoria.** El RDD limpio y el de contribuciones se `persist(MEMORY_ONLY)` y se materializan con `count()`
antes de medir: las 8 consultas no vuelven a leer MongoDB ni a re-ejecutar la limpieza.

**Submuestreo.** En Spark, `rdd.sample(False, f, seed=42)` sobre el RDD **limpio** → N/2 y N/4 son muestras
aleatorias reproducibles de partidos válidos de todas las temporadas (tomar los primeros N documentos sesgaba
hacia una temporada). El tamaño exacto varía ±algunos partidos; se reporta el real.

**Particionamiento.** El RDD se crea con 1 partición y, para el experimento 2, se aplica `rdd.repartition(n)`.
Spark corre en `local[4]` (4 hilos) para que 4 particiones puedan ejecutarse en paralelo.

**Límites de medición** (respuesta de `/analyze`, campo `tiempos_seg`):

| Campo | Qué mide |
|---|---|
| `t_mongo_read` | `find()` completo → lista en el driver (E/S de red + BSON). |
| `t_rdd_load` | `parallelize` + `count` (serialización driver → executors). |
| `t_limpieza` | Reglas R1–R6 incluyendo los `count()` del reporte. |
| `t_cache` | Submuestreo + repartition + materialización de caches. |
| `timings_queries_seg.qX` | Cada consulta por separado con datos ya en memoria (cómputo puro). |
