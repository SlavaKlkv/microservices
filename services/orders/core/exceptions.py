import logging
from typing import Any

from fastapi import HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy.exc import DBAPIError, IntegrityError
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger(__name__)


class OrderNotFoundException(HTTPException):
    def __init__(self, order_id: int | None = None):
        msg = (
            'Заказ не найден'
            if order_id is None
            else f'Заказ с ID {order_id} не найден'
        )
        super().__init__(status_code=status.HTTP_404_NOT_FOUND, detail=msg)


class OrderAlreadyExistsException(HTTPException):
    def __init__(self, field: str = 'id'):
        super().__init__(
            status_code=status.HTTP_409_CONFLICT,
            detail=f'Заказ с таким {field} уже существует',
        )


class IntegrityConflictException(HTTPException):
    def __init__(self, detail: str = 'Нарушение целостности данных'):
        super().__init__(
            status_code=status.HTTP_409_CONFLICT,
            detail=detail,
        )


class PermissionDenied(HTTPException):
    def __init__(self, detail: str = 'Доступ запрещён'):
        super().__init__(status_code=status.HTTP_403_FORBIDDEN, detail=detail)


class TooManyAttempts(HTTPException):
    def __init__(
        self, detail: str = 'Слишком много попыток, попробуйте позже'
    ):
        super().__init__(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=detail
        )


# ============================== Регистрация обработчиков =====================


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
    # --- helpers for DB errors (PostgreSQL SQLSTATE) ---
    def _pg_err_info(exc: IntegrityError) -> tuple[int, str]:
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

    @app.exception_handler(PermissionDenied)
    async def permission_denied_handler(
        request: Request, exc: PermissionDenied
    ):
        return _json_error(exc.status_code, str(exc.detail))

    @app.exception_handler(TooManyAttempts)
    async def too_many_attempts_handler(
        request: Request, exc: TooManyAttempts
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
