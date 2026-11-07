# \# Tarea 2 — Procesamiento Distribuido de Datos (Grupo 10)

# 

# Flujo: \*\*Soccerway → scraper → collector → MongoDB → Spark (RDD) → MongoDB (`resultados`)\*\*.

# 

# \## Integrantes

# 

# | Nombre | Correo |

# |---|---|

# | Benjamín Casanova | benjamin.casanova@mail.udp.cl |

# | Lucas Echeverría | lucas.echeverria@mail.udp.cl |

# 

# \## Links

# 

# \- \*\*Repositorio:\*\* https://giteit.udp.cl/CIT2011/2026-2/seccion-2/grupo-10/-/tree/tarea2

# \- \*\*Video de demostración:\*\* \_(pendiente)\_

# \- \*\*Resultados de los experimentos usados en el informe:\*\* \[`experiments/results/20261027-233430/`](experiments/results/20261027-233430/)

# 

# \## Arquitectura

# 

# | Servicio | Puerto | Responsabilidad |

# |---|---|---|

# | `mongodb` | 27017 | Almacenamiento persistente. `partidos` (crudos) y `resultados` (agregados). Volumen `mongo\_data`. |

# | `scraper-service` | 8000 | Scraper de la Tarea 1 (Playwright), adaptado: fecha con año de la temporada, estado del partido, marcador inválido → `null`. |

# | `collector-service` | 8001 | Recolección de todas las temporadas de la Liga de Primera + ingesta en MongoDB con conteo de insertados / descartados / duplicados. Guarda un snapshot JSON (volumen `collector\_data`). |

# | `spark-service` | 8002 | Carga MongoDB → RDD, limpieza, contribuciones por equipo, 8 consultas, guarda resultados. |

# 

# Código Spark: `spark-service/app/pipeline.py` (lógica) y `spark-service/app/main.py` (API).

# 

# \## Requisitos

# 

# \- Docker y Docker Compose v2. Trabajamos en \*\*Windows 11 con Docker Desktop\*\* (backend WSL2), así que ahí es donde lo probamos: hay que tenerlo abierto (con el motor corriendo) antes de usar `docker compose`. En Linux alcanza con Docker Engine y el plugin de Compose.

# \- Python 3.10+ en el host solo para los experimentos: `pip install requests matplotlib`

# 

# \## Ejecución

# 

# ```bash

# cp .env.example .env          # Windows cmd: copy .env.example .env

# docker compose up -d --build

# 

# \# 1–2. Recolección + ingesta (raspa Soccerway; tarda varios minutos)

# curl -X POST localhost:8001/collect

# curl localhost:8001/count            # total, por estado y por temporada

# 

# \# 3–7. Limpieza + 8 consultas + persistencia en `resultados`

# curl -X POST "localhost:8002/analyze?sample\_fraction=1.0\&partitions=1"

# ```

# 

# Ver resultados guardados:

# ```bash

# docker compose exec mongodb mongosh soccerway --eval "db.resultados.find({}, {resultados:0})"

# ```

# 

# \## Detener y reiniciar desde cero

# 

# | Comando | Qué hace | Datos de MongoDB | Snapshot del collector |

# |---|---|---|---|

# | `docker compose stop` | Detiene los contenedores | Se conservan | Se conserva |

# | `docker compose down` | Detiene y elimina los contenedores | Se conservan | Se conserva |

# | `docker compose down -v` | Además \*\*elimina los volúmenes\*\* | \*\*Se borran\*\* | \*\*Se borra\*\* |

# 

# Los datos viven en los volúmenes `mongo\_data` y `collector\_data`, por lo que sobreviven a `stop`, `down` y

# reinicios del equipo. Solo `down -v` los elimina.

# 

# Para partir completamente de cero (base vacía y nuevo scraping):

# ```bash

# docker compose down -v

# docker compose up -d --build

# curl -X POST localhost:8001/collect

# ```

# 

# Para vaciar solo la colección de partidos sin borrar el snapshot (y sin volver a raspar):

# ```bash

# curl -X DELETE localhost:8001/delete     # deja `partidos` vacía

# curl -X POST localhost:8001/reingest     # vuelve a cargarla desde el snapshot

# ```

# 

# > Después de `down -v` hay que ejecutar `/collect` de nuevo antes de `/reingest`, `/analyze` o los

# > experimentos con `--skip-collect`, porque el snapshot también se borró.

# 

# \## Endpoints

# 

# | Método | Ruta | Descripción |

# |---|---|---|

# | GET | `:8000/health`, `:8001/health`, `:8002/health` | Estado de cada servicio. |

# | POST | `:8000/scrape` | Consulta individual al scraper. Cuerpo: `{"query\_type": "Q5", "params": {}}` (Q5 = tabla de posiciones). |

# | POST | `:8001/collect` | Raspa todas las temporadas, guarda snapshot e ingesta. |

# | POST | `:8001/reingest` | Vacía `partidos` y vuelve a insertar el snapshot (mide escritura sin re-raspar). |

# | GET | `:8001/count` | Conteos por estado y temporada. |

# | DELETE | `:8001/delete` | Vacía `partidos`. |

# | POST | `:8002/analyze?sample\_fraction=\&partitions=\&seed=\&persist=` | Pipeline completo. Devuelve reporte de limpieza, tiempos y resultados. Con `persist=false` no guarda en `resultados`. |

# | POST | `:8002/read-benchmark` | Solo lectura MongoDB → RDD (sección 5.1). |

# 

# \## Validación de corrección

# 

# ```bash

# docker compose exec spark-service python -m app.validate

# ```

# Ejecuta las reglas de limpieza y las 8 consultas sobre 12 partidos conocidos (5 válidos + 7 sucios),

# compara contra valores calculados a mano y verifica que los resultados sean idénticos con 1, 2 y 4 particiones.

# 

# \## Experimentos

# 

# Hay dos formas de reproducirlos: con el script (recomendada) o a mano con `curl`. Ambas llaman a los

# mismos endpoints con los mismos parámetros; el script solo automatiza las repeticiones, los promedios

# y los gráficos.

# 

# \### Opción A: con script

# 

# Requiere `pip install requests matplotlib` en el host.

# 

# ```bash

# python experiments/run\_experiments.py                 # raspa 1 vez y corre todo

# python experiments/run\_experiments.py --skip-collect  # reutiliza el snapshot

# ```

# Tarda \~2 minutos con `--skip-collect`. Genera `experiments/results/<timestamp>/`:

# 

# | Archivo | Contenido |

# |---|---|

# | `sec51\_mongodb.csv` | 3 repeticiones de escritura (`/reingest`) y lectura (`/read-benchmark`) + promedio. |

# | `limpieza.json` | Registros por etapa de limpieza y descartes por regla. |

# | `exp1\_volumen.csv/.png` | N, N/2, N/4 con 1 partición (media ± desv. de 3 reps por consulta). |

# | `exp2\_particiones.csv/.png` | 1, 2, 4 particiones con N completo. |

# | `verificacion\_particiones.json` | Resultados idénticos entre particiones (sí/no). |

# 

# Además, en la carpeta de resultados usada en el informe (`experiments/results/20261027-233430/`) se

# guardaron a mano dos archivos de evidencia:

# 

# | Archivo | Cómo se obtuvo |

# |---|---|

# | `collect.json` | Respuesta de `POST /collect`: recolectados por temporada, insertados, descartados y duplicados. |

# | `validate.txt` | Salida de `docker compose exec -T spark-service python -m app.validate`. |

# 

# Orden de ejecución: (1) 3 × escritura y lectura en MongoDB, (2) 1 corrida de calentamiento descartada

# (arranque de JVM y workers Python), (3) Experimento 1, (4) Experimento 2, cada configuración 3 veces,

# (5) comparación de resultados entre particiones.

# 

# \### Opción B: a mano (sin script)

# 

# Los comandos funcionan igual en `cmd`, PowerShell (usar `curl.exe`) y bash. "×3" significa ejecutar

# \*\*ese mismo comando 3 veces seguidas\*\* y promediar los 3 tiempos; se repite porque una sola medición

# varía por la carga del equipo.

# 

# | Paso | Comandos distintos | Repeticiones | Ejecuciones |

# |---|---|---|---|

# | 0. Recolección | 1 (`/collect`) | 1 | 1 |

# | 1. Sección 5.1 | 2 (`/reingest`, `/read-benchmark`) | 3 | 6 |

# | 2. Calentamiento | 1 | 1 (se descarta) | 1 |

# | 3. Experimento 1 | 3 (N, N/2, N/4) | 3 | 9 |

# | 4. Experimento 2 | 3 (1, 2, 4 particiones) | 3 | 9 |

# | \*\*Total\*\* | | | \*\*26\*\* (25 sin contar la recolección) |

# 

# \*\*0. Recolección (una sola vez)\*\*

# ```bash

# curl -X POST localhost:8001/collect      # recolectados, insertados, descartados, duplicados

# curl localhost:8001/count                # conteos por estado y temporada

# ```

# 

# \*\*1. Sección 5.1: escritura y lectura en MongoDB (×3 → 6 ejecuciones)\*\*

# ```bash

# curl -X POST localhost:8001/reingest     # t\_write\_total\_seg, t\_write\_avg\_por\_doc\_seg

# curl -X POST localhost:8002/read-benchmark   # t\_mongo\_read\_seg, t\_rdd\_load\_seg, t\_total\_seg

# ```

# 

# \*\*2. Calentamiento (ejecutar una vez y descartar)\*\*

# ```bash

# curl -X POST "localhost:8002/analyze?persist=false"

# ```

# 

# \*\*3. Experimento 1: volumen, 1 partición (×3 cada uno → 9 ejecuciones)\*\*

# ```bash

# curl -X POST "localhost:8002/analyze?sample\_fraction=1.0\&partitions=1"

# curl -X POST "localhost:8002/analyze?sample\_fraction=0.5\&partitions=1"

# curl -X POST "localhost:8002/analyze?sample\_fraction=0.25\&partitions=1"

# ```

# 

# \*\*4. Experimento 2: particiones, N completo (×3 cada uno → 9 ejecuciones)\*\*

# ```bash

# curl -X POST "localhost:8002/analyze?sample\_fraction=1.0\&partitions=1"

# curl -X POST "localhost:8002/analyze?sample\_fraction=1.0\&partitions=2"

# curl -X POST "localhost:8002/analyze?sample\_fraction=1.0\&partitions=4"

# ```

# 

# \*\*Qué anotar de cada respuesta de `/analyze`:\*\*

# 

# | Campo | Significado |

# |---|---|

# | `partidos\_analizados` | Tamaño real del dataset usado (N, ≈N/2 o ≈N/4). |

# | `limpieza.etapas` / `limpieza.descartes` | Registros por etapa de limpieza. |

# | `timings\_queries\_seg.q1` … `q8` | Tiempo de cada consulta (datos ya en memoria). |

# | `tiempos\_seg` | Tiempos por fase: `t\_mongo\_read`, `t\_rdd\_load`, `t\_limpieza`, `t\_cache`, `t\_queries\_total`, `t\_total`. |

# | `resultados` | Salida de las 8 consultas; debe ser idéntica con 1, 2 y 4 particiones. |

# 

# Para ver solo los tiempos en lugar de la respuesta completa:

# ```bash

# curl -s -X POST "localhost:8002/analyze?partitions=4\&persist=false" | python -c "import sys,json;d=json.load(sys.stdin);print(d\['partidos\_analizados'],d\['tiempos\_seg'],d\['timings\_queries\_seg'])"

# ```

# 

# \*\*Comparación entre ambas opciones\*\*

# 

# | | Con script | A mano |

# |---|---|---|

# | Ejecuciones | 25 (+1 recolección opcional), automáticas | 25 (+1 recolección), una por una |

# | Tiempo | \~2 min | \~1–1,5 h (incluye anotar y promediar) |

# | Promedios, desviación y gráficos | Automáticos | En planilla |

# | Verificación entre particiones | Automática | Comparar `resultados` a mano |

# 

# \## Decisiones de diseño

# 

# \*\*Recolección.\*\* Solo páginas de liga (`liga-de-primera\[-AAAA]/resultados/`, temporadas 2017 a la actual): todo es

# Primera División y la temporada se toma de la URL. Las páginas por equipo de la T1 mezclaban copas internacionales.

# Soccerway muestra solo una parte de los partidos de cada temporada (unos 105 de 240), por lo que el total ronda los 1.050 partidos; está explicado en las limitaciones del informe.

# 

# \*\*Esquema del documento (`partidos`).\*\*

# ```json

# { "\_id": "<match\_id>", "match\_id": "...", "fecha": "2024-03-01", "fecha\_raw": "01.03. 20:00",

# &#x20; "temporada": "2024", "competicion": "Primera División",

# &#x20; "equipo\_local": "...", "equipo\_visitante": "...", "goles\_local": 2, "goles\_visitante": 1,

# &#x20; "goles\_raw": \["2", "1"], "estado": "finalizado|programado|suspendido|reprogramado",

# &#x20; "estado\_raw": "...", "url": "..." }

# ```

# Los campos `\*\_raw` guardan el texto original para auditar la limpieza.

# 

# \*\*Deduplicación e índices.\*\* `\_id = match\_id` + índice único en `match\_id` → una re-ejecución no duplica.

# Índice secundario `(temporada, estado)` para conteos. Inserción con `insert\_many(ordered=False)`: un solo

# round-trip por lote; los duplicados (E11000) se cuentan y no detienen la inserción.

# 

# \*\*Datos crudos.\*\* MongoDB guarda también partidos no finalizados; el collector solo descarta registros sin

# `match\_id` o sin equipos. El filtrado de partidos válidos lo hace Spark.

# 

# \*\*Limpieza en Spark (orden de aplicación).\*\*

# 

# | # | Regla | Motivo |

# |---|---|---|

# | R1 | Requiere `match\_id`, local y visitante | Sin esto no hay contribución por equipo. |

# | R2 | Solo `estado == "finalizado"` | Programados/suspendidos no tienen resultado definitivo. |

# | R3 | Goles enteros, `0 ≤ g ≤ 20` | Detecta errores de scraping (null, negativos, basura). |

# | R4 | Normalización de nombres (quita tarjetas rojas `\\n2`, sufijo `(Chi)`, espacios; alias de `shared/teams.json`) y descarta local == visitante | Un mismo equipo no puede aparecer con dos claves. |

# | R5 | Duplicado por `match\_id` | Criterio primario. |

# | R6 | Duplicado por `(fecha, local, visitante)` | Mismo partido con distinto id. Los sin fecha no se descartan. |

# 

# Los partidos sin fecha \*\*no\*\* se eliminan: cuentan en todas las consultas excepto "partidos por mes" (Q6).

# 

# \*\*Contribuciones.\*\* Cada partido → 2 registros `(equipo, (temporada, GF, GC, resultado, puntos))` (`flatMap`).

# Q1, Q2, Q3 y Q7 hacen `reduceByKey` sobre este RDD; Q4, Q5, Q6 y Q8 sobre el RDD de partidos.

# 

# \*\*Memoria.\*\* El RDD limpio y el de contribuciones se `persist(MEMORY\_ONLY)` y se materializan con `count()`

# antes de medir: las 8 consultas no vuelven a leer MongoDB ni a re-ejecutar la limpieza.

# 

# \*\*Submuestreo.\*\* En Spark, `rdd.sample(False, f, seed=42)` sobre el RDD \*\*limpio\*\* → N/2 y N/4 son muestras

# aleatorias reproducibles de partidos válidos de todas las temporadas (tomar los primeros N documentos sesgaba

# hacia una temporada). El tamaño exacto varía ±algunos partidos; se reporta el real.

# 

# \*\*Particionamiento.\*\* El RDD se crea con 1 partición y, para el experimento 2, se aplica `rdd.repartition(n)`.

# Spark corre en `local\[4]` (4 hilos) para que 4 particiones puedan ejecutarse en paralelo.

# 

# \*\*Límites de medición\*\* (respuesta de `/analyze`, campo `tiempos\_seg`):

# 

# | Campo | Qué mide |

# |---|---|

# | `t\_mongo\_read` | `find()` completo → lista en el driver (E/S de red + BSON). |

# | `t\_rdd\_load` | `parallelize` + `count` (serialización driver → executors). |

# | `t\_limpieza` | Reglas R1–R6 incluyendo los `count()` del reporte. |

# | `t\_cache` | Submuestreo + repartition + materialización de caches. |

# | `timings\_queries\_seg.qX` | Cada consulta por separado con datos ya en memoria (cómputo puro). |

# 

# \## Uso de herramientas de IA

# 

# Como en la Tarea 1, se utilizó \*\*Claude\*\* (Anthropic) como herramienta de apoyo para el desarrollo y la depuración del proyecto:

# 

# \- \*\*Desarrollo:\*\* apoyo en la escritura y revisión del código (scraper, collector, pipeline de Spark, validación y script de experimentos) y de los archivos de despliegue.

# \- \*\*Depuración:\*\* diagnóstico de errores, por ejemplo el botón "Mostrar más partidos" que no cargaba filas, las fechas sin año, los nombres de equipos inconsistentes y problemas de Docker y Git.

# 

# 

# \## Historial de Git

# 

# Por un `push --force` accidental se perdió parte del historial. Los commits iniciales (20 al 24 de octubre) se restauraron, y el commit `275436f` (27/10) consolida el estado del proyecto en ese momento. Los commits posteriores muestran las correcciones, la validación y los experimentos.

# 

# \## Contribución por integrante

# 

# El código (scraper, collector, pipeline de Spark, validación y experimentos) y el informe los hicimos entre los dos. Todos los commits salieron de la cuenta de Benjamín, pero Lucas no hizo commits y aun así aportó harto en la tarea, así que el historial de Git no refleja su trabajo.

# 

# | Integrante | Participación |

# |---|---|

# | Benjamín Casanova | Código e informe en conjunto; hizo todos los commits. |

# | Lucas Echeverría | Código e informe en conjunto; no hizo commits. |

