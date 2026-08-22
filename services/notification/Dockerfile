FROM python:3.13-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

# Сначала — только манифесты воркспейса: слой с зависимостями кэшируется
# и не пересобирается при правках кода сервиса.
COPY pyproject.toml uv.lock ./
COPY packages/events/pyproject.toml packages/events/README.md ./packages/events/
COPY services/notification/pyproject.toml ./services/notification/

RUN uv sync --frozen --no-dev --package microservices-notification --no-install-workspace

COPY packages/events ./packages/events
RUN uv sync --frozen --no-dev --package microservices-notification

COPY services/notification ./notification

CMD ["uv", "run", "python", "-m", "notification.main"]
