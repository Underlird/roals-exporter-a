FROM python:3.11.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# System-Pakete: jq für Debugging, tzdata für ZoneInfo-Support
RUN apt-get update && \
    apt-get install -y --no-install-recommends jq tzdata && \
    rm -rf /var/lib/apt/lists/*

# Python-Dependencies: requests ist Pflicht für den neuen Exporter
RUN pip install --no-cache-dir requests

COPY app/ /app/

CMD ["python", "-u", "/app/exporter.py"]

