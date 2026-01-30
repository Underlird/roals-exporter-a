FROM python:3.11.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# System-Pakete installieren
RUN apt-get update && \
    apt-get install -y --no-install-recommends jq tzdata && \
    rm -rf /var/lib/apt/lists/*

# FIX: Python-Dependencies installieren (requests fehlte)
RUN pip install --no-cache-dir requests

COPY app/ /app/

CMD ["python", "-u", "/app/exporter.py"]

