from __future__ import annotations

from fastapi import status
import structlog
from orders.core.exceptions import (
    IntegrityConflictException,
    OrderAlreadyExistsException,
    OrderNotFoundException,
    _json_error,
)
from sqlalchemy.exc import DBAPIError, IntegrityError
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = structlog.get_logger(__name__)


class DBErrorMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        try:
            return await call_next(request)

        except OrderNotFoundException as exc:
            return _json_error(status.HTTP_404_NOT_FOUND, str(exc.detail))

        except (
            OrderAlreadyExistsException,
            IntegrityConflictException,
        ) as exc:
            return _json_error(status.HTTP_409_CONFLICT, str(exc.detail))

        except IntegrityError as exc:
            code = getattr(getattr(exc, 'orig', None), 'pgcode', None)

            http_status = status.HTTP_400_BAD_REQUEST
            message = 'Нарушение целостности данных'

            if code == '23505':
                http_status = status.HTTP_409_CONFLICT
                message = 'Нарушение уникальности'
            elif code == '23503':
                http_status = status.HTTP_400_BAD_REQUEST
                message = 'Нарушение внешнего ключа'
            elif code == '23502':
                http_status = status.HTTP_400_BAD_REQUEST
                message = 'Обязательное поле не заполнено'
            elif code == '23514':
                http_status = status.HTTP_400_BAD_REQUEST
                message = 'Нарушение ограничения CHECK'

            return _json_error(http_status, message)

        except DBAPIError as exc:
            logger.exception(
                'DBAPIError',
                path=str(request.url.path),
                method=request.method,
            )
            return _json_error(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                'Ошибка базы данных',
            )

        except Exception as exc:
            logger.exception(
                'Unhandled exception',
                path=str(request.url.path),
                method=request.method,
            )
            return _json_error(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                'Произошла непредвиденная ошибка',
            )
