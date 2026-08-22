"""ASGI-middleware, общий для всех HTTP-сервисов."""

import uuid
from collections.abc import Awaitable, Callable

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

REQUEST_ID_HEADER = 'X-Request-ID'


class RequestIdMiddleware(BaseHTTPMiddleware):
    """Пробрасывает X-Request-ID и кладёт его в контекст логов.

    Тот же идентификатор дальше едет в событиях как ``correlation_id``,
    поэтому HTTP-запрос и цепочку саги можно связать в логах.
    """

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request_id = request.headers.get(REQUEST_ID_HEADER) or str(
            uuid.uuid4()
        )
        request.state.request_id = request_id

        structlog.contextvars.bind_contextvars(request_id=request_id)
        try:
            response = await call_next(request)
        finally:
            structlog.contextvars.unbind_contextvars('request_id')

        response.headers[REQUEST_ID_HEADER] = request_id
        return response
