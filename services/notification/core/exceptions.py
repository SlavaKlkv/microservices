from typing import Any

import structlog
from fastapi import HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy.exc import DBAPIError, IntegrityError
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = structlog.get_logger()


class NotificationSendFailed(HTTPException):
    def __init__(self, detail: str = 'Не удалось отправить уведомление'):
        super().__init__(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=detail,
        )


class UnsupportedNotificationEvent(HTTPException):
    def __init__(self, detail: str = 'Неподдерживаемый тип события'):
        super().__init__(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=detail,
        )


class IntegrityConflictException(HTTPException):
    def __init__(self, detail: str = 'Нарушение целостности данных'):
        super().__init__(
            status_code=status.HTTP_409_CONFLICT,
            detail=detail,
        )


def _sanitize_errors(errors: Any) -> Any:
    if isinstance(errors, dict):
        return {k: _sanitize_errors(v) for k, v in errors.items()}
    if isinstance(errors, list):
        return [_sanitize_errors(e) for e in errors]
    if isinstance(errors, tuple):
        return tuple(_sanitize_errors(e) for e in errors)
    if isinstance(errors, BaseException):
        return str(errors)
    return errors


def _json_error(
    status_code: int, detail: str, *, errors: Any | None = None
) -> JSONResponse:
    payload: dict[str, Any] = {'detail': detail}
    if errors is not None:
        payload['errors'] = errors
    return JSONResponse(
        status_code=status_code, content=jsonable_encoder(payload)
    )


def init_exception_handlers(app):
    """
    Регистрирует глобальные обработчики исключений.
    Единый формат ошибок: {"detail": "...", "errors": [...] (опционально)}.
    """

    def _pg_err_info(exc: IntegrityError) -> tuple[int, str]:
        """
        Определяет соответствие кода SQLSTATE PostgreSQL HTTP-статусу
            и сообщению об ошибке.
        Часто встречающиеся SQLSTATE-коды:
        23505 — нарушение уникальности (unique_violation),
        23503 — нарушение внешнего ключа (foreign_key_violation),
        23502 — значение NULL в обязательном поле (not_null_violation),
        23514 — нарушение ограничения CHECK (check_violation).
        """
        code = getattr(getattr(exc, 'orig', None), 'pgcode', None)
        status_code = status.HTTP_400_BAD_REQUEST
        message = 'Нарушение целостности данных'
        if code == '23505':
            status_code = status.HTTP_409_CONFLICT
            message = 'Нарушение уникальности'
        elif code == '23503':
            status_code = status.HTTP_400_BAD_REQUEST
            message = 'Нарушение внешнего ключа'
        elif code == '23502':
            status_code = status.HTTP_400_BAD_REQUEST
            message = 'Обязательное поле не заполнено'
        elif code == '23514':
            status_code = status.HTTP_400_BAD_REQUEST
            message = 'Нарушение ограничения CHECK'
        return status_code, message

    @app.exception_handler(UnsupportedNotificationEvent)
    async def unsupported_event_handler(
        request: Request, exc: UnsupportedNotificationEvent
    ):
        return _json_error(exc.status_code, str(exc.detail))

    @app.exception_handler(NotificationSendFailed)
    async def notification_send_failed_handler(
        request: Request, exc: NotificationSendFailed
    ):
        return _json_error(exc.status_code, str(exc.detail))

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        return _json_error(exc.status_code, str(exc.detail))

    @app.exception_handler(RequestValidationError)
    async def request_validation_handler(
        request: Request, exc: RequestValidationError
    ):
        return _json_error(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            'Некорректные данные запроса',
            errors=_sanitize_errors(exc.errors()),
        )

    @app.exception_handler(ValidationError)
    async def pydantic_validation_handler(
        request: Request, exc: ValidationError
    ):
        return _json_error(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            'Ошибка валидации данных',
            errors=_sanitize_errors(exc.errors()),
        )

    @app.exception_handler(StarletteHTTPException)
    async def starlette_http_exception_handler(
        request: Request, exc: StarletteHTTPException
    ):
        return _json_error(exc.status_code, str(exc.detail))

    @app.exception_handler(IntegrityError)
    async def integrity_error_handler(request: Request, exc: IntegrityError):
        status_code, message = _pg_err_info(exc)
        return _json_error(status_code, message)

    @app.exception_handler(DBAPIError)
    async def dbapi_error_handler(request: Request, exc: DBAPIError):
        return _json_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR, 'Ошибка базы данных'
        )

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        return _json_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            'Произошла непредвиденная ошибка',
        )
