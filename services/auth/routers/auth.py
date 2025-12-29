from __future__ import annotations

from fastapi import APIRouter, Depends, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession

from core.db import get_session
from schemas.auth import (
    AuthResponse,
    LoginRequest,
    Message,
    RefreshRequest,
    RegisterRequest,
    RevokeRequest,
    TokenPair,
    to_auth_user,
)
from schemas.users import User, UserCreate
from service.auth_service import AuthService

auth_router = APIRouter(prefix='/auth', tags=['auth'])


@auth_router.post(
    '/login',
    response_model=TokenPair,
    status_code=status.HTTP_200_OK,
    summary='Вход через form-data (используется для Swagger Authorize)',
)
async def login(
    form: OAuth2PasswordRequestForm = Depends(),
    session: AsyncSession = Depends(get_session),
) -> TokenPair:
    """Авторизация через OAuth2PasswordRequestForm.
    Поле username может содержать username или email.
    """
    service = AuthService(session)
    user = await service.authenticate(form.username, form.password)
    return service.issue_tokens(user)


@auth_router.post(
    '/login_json',
    response_model=AuthResponse,
    status_code=status.HTTP_200_OK,
    summary='Вход по JSON: username/email + password и '
    'получение access/refresh токенов',
)
async def login_json(
    body: LoginRequest, session: AsyncSession = Depends(get_session)
) -> AuthResponse:
    service = AuthService(session)
    user = await service.authenticate(body.login, body.password)
    tokens = service.issue_tokens(user)
    return AuthResponse(user=to_auth_user(user), tokens=tokens)


@auth_router.post(
    '/register',
    response_model=User,
    status_code=status.HTTP_201_CREATED,
    summary='Регистрация пользователя',
)
async def register(
    payload: RegisterRequest,
    session: AsyncSession = Depends(get_session),
) -> User:
    service = AuthService(session)
    user_payload = UserCreate(**payload.model_dump())
    return await service.register(user_payload)


@auth_router.post(
    '/refresh',
    response_model=TokenPair,
    status_code=status.HTTP_200_OK,
    summary='Обновление токенов по refresh',
)
async def refresh(
    body: RefreshRequest,
    session: AsyncSession = Depends(get_session),
) -> TokenPair:
    service = AuthService(session)
    return await service.refresh(body.refresh_token)


@auth_router.post(
    '/logout',
    response_model=Message,
    summary='Отзыв токена',
    description='Аннулирует refresh-токен, делая его недействительным.',
)
async def revoke_token(
    request: RevokeRequest,
    session: AsyncSession = Depends(get_session),
) -> Message:
    service = AuthService(session)
    await service.revoke_refresh_token(request.refresh_token)
    return Message(message='refresh-токен отозван')
