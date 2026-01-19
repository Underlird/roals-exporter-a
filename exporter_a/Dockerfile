FROM python:3.11.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends jq tzdata && \
    rm -rf /var/lib/apt/lists/*

COPY app/ /app/

CMD ["python", "-u", "/app/exporter.py"]
