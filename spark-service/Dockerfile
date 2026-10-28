FROM python:3.12-slim

# PySpark requiere Java
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
# Los workers de PySpark deben poder importar app.pipeline
ENV PYTHONPATH=/app \
    PYSPARK_PYTHON=python3

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
