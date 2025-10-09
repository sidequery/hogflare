FROM python:3.13-slim

RUN pip install --no-cache-dir fastapi uvicorn duckdb

WORKDIR /app

COPY scripts/fake_pipeline.py /app/fake_pipeline.py

EXPOSE 8080

CMD ["uvicorn", "fake_pipeline:app", "--host", "0.0.0.0", "--port", "8080"]
