from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from ms_events import setup_logging
from ms_events.middleware import RequestIdMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from starlette.middleware import Middleware

from auth.core.db import db_ping
from auth.core.exceptions import init_exception_handlers
from auth.core.middleware.exc_middleware import DBErrorMiddleware
from auth.core.middleware.jwt_middleware import JWTAuthMiddleware
from auth.routers.auth_router import auth_router
from auth.routers.users_router import users_router
from auth.settings import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging('auth', level=settings.LOG_LEVEL)
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
    Middleware(JWTAuthMiddleware),  # type: ignore[arg-type]
]


app = FastAPI(
    title='Auth API',
    version='1.0.0',
    description=(
        'Auth microservice для аутентификации и управления пользователями.'
    ),
    middleware=middleware,
    lifespan=lifespan,
)

Instrumentator().instrument(app).expose(app)

init_exception_handlers(app)

api_v1 = APIRouter(prefix='/api/v1')
for router in (
    auth_router,
    users_router,
):
    api_v1.include_router(router)

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
