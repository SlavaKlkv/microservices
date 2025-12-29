from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from starlette.middleware import Middleware

from core.exceptions import init_exception_handlers
from core.middleware.exc_middleware import DBErrorMiddleware
from core.middleware.jwt_middleware import JWTAuthMiddleware
from routers.auth import auth_router
from routers.users import users_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


middleware = [
    Middleware(
        CORSMiddleware,  # type: ignore[arg-type]
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
    ),
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
