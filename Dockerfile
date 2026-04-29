# ── Stage 1: Build React frontend ─────────────────────────────────────────────
FROM node:20-slim AS frontend-builder

WORKDIR /build/frontend
COPY ui/frontend/package*.json ./
RUN npm install --silent

COPY ui/frontend/ ./
RUN npm run build


# ── Stage 2: Python runtime ────────────────────────────────────────────────────
FROM python:3.11-slim

LABEL org.opencontainers.image.title="Dremio Load"
LABEL org.opencontainers.image.description="Batch data ingestion into Dremio / Iceberg — GUI included"
LABEL org.opencontainers.image.source="https://github.com/dremio-community/dremio-load"
LABEL org.opencontainers.image.authors="Mark Shainman"
LABEL org.opencontainers.image.licenses="Apache-2.0"

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        unixodbc-dev \
        libaio1t64 \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY core/       ./core/
COPY sources/    ./sources/
COPY ui/backend/ ./ui/backend/
COPY main.py     .

# Copy built frontend
COPY --from=frontend-builder /build/frontend/dist ./ui/frontend/dist

# Create data directory for SQLite and config
RUN mkdir -p /data
ENV LOAD_DB_PATH=/data/load_ui.db

EXPOSE 7071

CMD ["python", "main.py", "--config", "/data/config.yml", "--port", "7071"]
