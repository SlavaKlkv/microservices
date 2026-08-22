import os
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from ms_events import setup_logging
from ms_events.middleware import RequestIdMiddleware
from orders_history.core.db import db_ping
from orders_history.core.exceptions import init_exception_handlers
from orders_history.core.middleware import DBErrorMiddleware
from orders_history.router import orders_history_router
from prometheus_fastapi_instrumentator import Instrumentator
from starlette.middleware import Middleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging('orders-history', level=os.getenv('LOG_LEVEL', 'INFO'))
    yield


middleware = [
    Middleware(
        CORSMiddleware,  # type: ignore[arg-type]
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
    ),
    Middleware(RequestIdMiddleware),  # type: ignore[arg-type]
    Middleware(DBErrorMiddleware),  # type: ignore[arg-type]
]


app = FastAPI(
    title='Orders history API',
    version='1.0.0',
    description='Orders history microservice для хранения истории заказов.',
    middleware=middleware,
    lifespan=lifespan,
)

Instrumentator().instrument(app).expose(app)

init_exception_handlers(app)

api_v1 = APIRouter(prefix='/api/v1')

api_v1.include_router(orders_history_router)

app.include_router(api_v1)


@app.get('/health', tags=['system'])
def health():
    return {'status': 'ok'}


@app.get('/ready', tags=['system'])
async def ready():
    """Готовность сервиса: БД доступна."""
    if not await db_ping():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail='database is not available',
        )
    return {'status': 'ready'}
