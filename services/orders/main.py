from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from orders.core.exceptions import init_exception_handlers
from orders.core.middleware import DBErrorMiddleware
from orders.router import orders_router
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
    title='Orders API',
    version='1.0.0',
    description=('Orders microservice для управления заказами.'),
    middleware=middleware,
    lifespan=lifespan,
)

Instrumentator().instrument(app).expose(app)

init_exception_handlers(app)

api_v1 = APIRouter(prefix='/api/v1')

api_v1.include_router(orders_router)

app.include_router(api_v1)


@app.get('/health', tags=['system'])
def health():
    return {'status': 'ok'}
