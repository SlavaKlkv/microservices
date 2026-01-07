from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from orders_history.core.exceptions import init_exception_handlers
from orders_history.core.middleware import DBErrorMiddleware
from orders_history.router import orders_history_router
from prometheus_fastapi_instrumentator import Instrumentator
from starlette.middleware import Middleware


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
