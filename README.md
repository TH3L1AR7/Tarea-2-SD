# Tarea 2 — Procesamiento Distribuido de Datos (Grupo 10)

Flujo: **Soccerway → scraper → collector → MongoDB → Spark (RDD) → MongoDB (`resultados`)**.

## Integrantes

| Nombre | Correo |
|---|---|
| Benjamín Casanova | benjamin.casanova@mail.udp.cl |
| Lucas Echeverría | lucas.echeverria@mail.udp.cl |

## Links

- **Repositorio:** https://giteit.udp.cl/CIT2011/2026-2/seccion-2/grupo-10/-/tree/tarea2
- **Video de demostración:** _(pendiente)_
- **Resultados de los experimentos usados en el informe:** [`experiments/results/20261027-233430/`](experiments/results/20261027-233430/)

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
cp .env.example .env          # Windows cmd: copy .env.example .env
docker compose up -d --build

# 1–2. Recolección + ingesta (raspa Soccerway; tarda varios minutos)
curl -X POST localhost:8001/collect
curl localhost:8001/count            # total, por estado y por temporada

# 3–7. Limpieza + 8 consultas + persistencia en `resultados`
curl -X POST "localhost:8002/analyze?sample_fraction=1.0&partitions=1"
```

Ver resultados guardados:
```bash
docker compose exec mongodb mongosh soccerway --eval "db.resultados.find({}, {resultados:0})"
```

## Detener y reiniciar desde cero

| Comando | Qué hace | Datos de MongoDB | Snapshot del collector |
|---|---|---|---|
| `docker compose stop` | Detiene los contenedores | Se conservan | Se conserva |
| `docker compose down` | Detiene y elimina los contenedores | Se conservan | Se conserva |
| `docker compose down -v` | Además **elimina los volúmenes** | **Se borran** | **Se borra** |

Los datos viven en los volúmenes `mongo_data` y `collector_data`, por lo que sobreviven a `stop`, `down` y
reinicios del equipo. Solo `down -v` los elimina.

Para partir completamente de cero (base vacía y nuevo scraping):
```bash
docker compose down -v
docker compose up -d --build
curl -X POST localhost:8001/collect
```

Para vaciar solo la colección de partidos sin borrar el snapshot (y sin volver a raspar):
```bash
curl -X DELETE localhost:8001/delete     # deja `partidos` vacía
curl -X POST localhost:8001/reingest     # vuelve a cargarla desde el snapshot
```

> Después de `down -v` hay que ejecutar `/collect` de nuevo antes de `/reingest`, `/analyze` o los
> experimentos con `--skip-collect`, porque el snapshot también se borró.

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

Hay dos formas de reproducirlos: con el script (recomendada) o a mano con `curl`. Ambas llaman a los
mismos endpoints con los mismos parámetros; el script solo automatiza las repeticiones, los promedios
y los gráficos.

### Opción A: con script

Requiere `pip install requests matplotlib` en el host.

```bash
python experiments/run_experiments.py                 # raspa 1 vez y corre todo
python experiments/run_experiments.py --skip-collect  # reutiliza el snapshot
```
Tarda ~2 minutos con `--skip-collect`. Genera `experiments/results/<timestamp>/`:

| Archivo | Contenido |
|---|---|
| `sec51_mongodb.csv` | 3 repeticiones de escritura (`/reingest`) y lectura (`/read-benchmark`) + promedio. |
| `limpieza.json` | Registros por etapa de limpieza y descartes por regla. |
| `exp1_volumen.csv/.png` | N, N/2, N/4 con 1 partición (media ± desv. de 3 reps por consulta). |
| `exp2_particiones.csv/.png` | 1, 2, 4 particiones con N completo. |
| `verificacion_particiones.json` | Resultados idénticos entre particiones (sí/no). |

Además, en la carpeta de resultados usada en el informe (`experiments/results/20261027-233430/`) se
guardaron a mano dos archivos de evidencia:

| Archivo | Cómo se obtuvo |
|---|---|
| `collect.json` | Respuesta de `POST /collect`: recolectados por temporada, insertados, descartados y duplicados. |
| `validate.txt` | Salida de `docker compose exec -T spark-service python -m app.validate`. |

Orden de ejecución: (1) 3 × escritura y lectura en MongoDB, (2) 1 corrida de calentamiento descartada
(arranque de JVM y workers Python), (3) Experimento 1, (4) Experimento 2, cada configuración 3 veces,
(5) comparación de resultados entre particiones.

### Opción B: a mano (sin script)

Los comandos funcionan igual en `cmd`, PowerShell (usar `curl.exe`) y bash. "×3" significa ejecutar
**ese mismo comando 3 veces seguidas** y promediar los 3 tiempos; se repite porque una sola medición
varía por la carga del equipo.

| Paso | Comandos distintos | Repeticiones | Ejecuciones |
|---|---|---|---|
| 0. Recolección | 1 (`/collect`) | 1 | 1 |
| 1. Sección 5.1 | 2 (`/reingest`, `/read-benchmark`) | 3 | 6 |
| 2. Calentamiento | 1 | 1 (se descarta) | 1 |
| 3. Experimento 1 | 3 (N, N/2, N/4) | 3 | 9 |
| 4. Experimento 2 | 3 (1, 2, 4 particiones) | 3 | 9 |
| **Total** | | | **26** (25 sin contar la recolección) |

**0. Recolección (una sola vez)**
```bash
curl -X POST localhost:8001/collect      # recolectados, insertados, descartados, duplicados
curl localhost:8001/count                # conteos por estado y temporada
```

**1. Sección 5.1: escritura y lectura en MongoDB (×3 → 6 ejecuciones)**
```bash
curl -X POST localhost:8001/reingest     # t_write_total_seg, t_write_avg_por_doc_seg
curl -X POST localhost:8002/read-benchmark   # t_mongo_read_seg, t_rdd_load_seg, t_total_seg
```

**2. Calentamiento (ejecutar una vez y descartar)**
```bash
curl -X POST "localhost:8002/analyze?persist=false"
```

**3. Experimento 1: volumen, 1 partición (×3 cada uno → 9 ejecuciones)**
```bash
curl -X POST "localhost:8002/analyze?sample_fraction=1.0&partitions=1"
curl -X POST "localhost:8002/analyze?sample_fraction=0.5&partitions=1"
curl -X POST "localhost:8002/analyze?sample_fraction=0.25&partitions=1"
```

**4. Experimento 2: particiones, N completo (×3 cada uno → 9 ejecuciones)**
```bash
curl -X POST "localhost:8002/analyze?sample_fraction=1.0&partitions=1"
curl -X POST "localhost:8002/analyze?sample_fraction=1.0&partitions=2"
curl -X POST "localhost:8002/analyze?sample_fraction=1.0&partitions=4"
```

**Qué anotar de cada respuesta de `/analyze`:**

| Campo | Significado |
|---|---|
| `partidos_analizados` | Tamaño real del dataset usado (N, ≈N/2 o ≈N/4). |
| `limpieza.etapas` / `limpieza.descartes` | Registros por etapa de limpieza. |
| `timings_queries_seg.q1` … `q8` | Tiempo de cada consulta (datos ya en memoria). |
| `tiempos_seg` | Tiempos por fase: `t_mongo_read`, `t_rdd_load`, `t_limpieza`, `t_cache`, `t_queries_total`, `t_total`. |
| `resultados` | Salida de las 8 consultas; debe ser idéntica con 1, 2 y 4 particiones. |

Para ver solo los tiempos en lugar de la respuesta completa:
```bash
curl -s -X POST "localhost:8002/analyze?partitions=4&persist=false" | python -c "import sys,json;d=json.load(sys.stdin);print(d['partidos_analizados'],d['tiempos_seg'],d['timings_queries_seg'])"
```

**Comparación entre ambas opciones**

| | Con script | A mano |
|---|---|---|
| Ejecuciones | 25 (+1 recolección opcional), automáticas | 25 (+1 recolección), una por una |
| Tiempo | ~2 min | ~1–1,5 h (incluye anotar y promediar) |
| Promedios, desviación y gráficos | Automáticos | En planilla |
| Verificación entre particiones | Automática | Comparar `resultados` a mano |

## Decisiones de diseño

**Recolección.** Solo páginas de liga (`liga-de-primera[-AAAA]/resultados/`, temporadas 2017 a la actual): todo es
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
