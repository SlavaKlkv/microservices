from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from auth.core.db import get_session
from auth.core.security import current_subject
from auth.schemas.users_schemas import (
    User,
    UserCreate,
    UserDeleteResponse,
    UsersList,
    UserUpdate,
)
from auth.service.users_service import UserService

users_router = APIRouter(
    prefix='/users',
    tags=['users'],
    dependencies=[Depends(current_subject)],
)


async def get_user_service(
    session: AsyncSession = Depends(get_session),
) -> UserService:
    return UserService(session)


@users_router.get(
    '/{user_id}', response_model=User, summary='Получить пользователя по ID'
)
async def get_user(
    user_id: int,
    service: UserService = Depends(get_user_service),
) -> User:
    return await service.get_user(user_id)


@users_router.get(
    '/',
    response_model=UsersList,
    summary='Получить пользователей: по списку id или всех',
)
async def get_users(
    ids: list[int] | None = Query(
        default=None, description='Список ID для фильтрации'
    ),
    service: UserService = Depends(get_user_service),
) -> UsersList:
    if ids:
        return await service.get_users_by_ids(ids)
    return await service.get_all_users()


@users_router.post(
    '/',
    response_model=User,
    status_code=201,
    summary='Создать одного пользователя',
)
async def create_user(
    payload: UserCreate,
    service: UserService = Depends(get_user_service),
) -> User:
    return await service.create_user(payload)


@users_router.post(
    '/bulk',
    response_model=UsersList,
    status_code=201,
    summary='Создать несколько пользователей',
)
async def create_users(
    payloads: list[UserCreate],
    service: UserService = Depends(get_user_service),
) -> UsersList:
    return await service.create_users(payloads)


@users_router.patch(
    '/{user_id}', response_model=User, summary='Обновить пользователя'
)
async def update_user(
    user_id: int,
    payload: UserUpdate,
    service: UserService = Depends(get_user_service),
) -> User:
    return await service.update_user(user_id, payload)


@users_router.delete(
    '/{user_id}',
    response_model=UserDeleteResponse,
    summary='Удалить пользователя и вернуть его данные',
)
async def delete_user(
    user_id: int,
    service: UserService = Depends(get_user_service),
) -> UserDeleteResponse:
    return await service.delete_user(user_id)
