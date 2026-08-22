from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import APIRouter, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from starlette.middleware import Middleware

from ms_events import setup_logging
from ms_events.middleware import RequestIdMiddleware
from notification.core.db import db_ping
from notification.core.exceptions import init_exception_handlers
from notification.router import notifications_router
from notification.settings import settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    setup_logging('notification', level=settings.LOG_LEVEL)
    yield


middleware = [
    Middleware(
        CORSMiddleware,  # type: ignore[arg-type]
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
    ),
    Middleware(RequestIdMiddleware),
]

app = FastAPI(
    title='Notification API',
    version='1.0.0',
    description='Notification microservice: уведомления по событиям саги.',
    middleware=middleware,
    lifespan=lifespan,
)

Instrumentator().instrument(app).expose(app)

init_exception_handlers(app)

api_v1 = APIRouter(prefix='/api/v1')
api_v1.include_router(notifications_router)
app.include_router(api_v1)


@app.get('/health', tags=['system'])
def health() -> dict[str, str]:
    return {'status': 'ok'}


@app.get('/ready', tags=['system'])
async def ready() -> dict[str, str]:
    """Готовность сервиса: БД доступна."""
    if not await db_ping():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail='database is not available',
        )
    return {'status': 'ready'}
