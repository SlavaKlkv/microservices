from __future__ import annotations

from typing import Iterable, Sequence

from pydantic import EmailStr
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import User
from schemas.users import (
    User as UserSchema,
)
from schemas.users import (
    UserCreate,
    UserDeleteResponse,
    UsersList,
    UserUpdate,
)


class UserRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    # ------------------------- helpers -------------------------

    @staticmethod
    def _to_schema(obj: User) -> UserSchema:
        return UserSchema.model_validate(obj)

    # ------------------------- queries -------------------------

    async def get_raw_by_email(self, email: EmailStr | str) -> User | None:
        stmt = (
            select(User)
            .where(
                func.lower(func.trim(User.email))
                == func.lower(func.trim(email))
            )
            .limit(1)
        )
        res = await self.session.execute(stmt)
        return res.scalar_one_or_none()

    async def get_raw_by_username(self, username: str) -> User | None:
        stmt = (
            select(User)
            .where(
                func.lower(func.trim(User.username))
                == func.lower(func.trim(username))
            )
            .limit(1)
        )
        res = await self.session.execute(stmt)
        return res.scalar_one_or_none()

    async def get_by_username(self, username: str) -> UserSchema | None:
        obj = await self.get_raw_by_username(username)
        return self._to_schema(obj) if obj else None

    async def get_by_id(self, user_id: int) -> UserSchema | None:
        res = await self.session.execute(
            select(User).where(User.id == user_id).limit(1)
        )
        obj = res.scalar_one_or_none()
        return self._to_schema(obj) if obj else None

    async def get_several_by_ids(self, ids: Iterable[int]) -> UsersList:
        id_list = list(ids)
        if not id_list:
            return UsersList(users=[])

        res = await self.session.execute(
            select(User).where(User.id.in_(id_list))
        )
        objs: Sequence[User] = res.scalars().all()

        users = [self._to_schema(o) for o in objs]
        return UsersList(users=users)

    async def get_all(self) -> UsersList:
        res = await self.session.execute(select(User).order_by(User.id.asc()))
        objs: Sequence[User] = res.scalars().all()
        users = [self._to_schema(o) for o in objs]
        return UsersList(users=users)

    # ------------------------- mutations -------------------------

    async def create(self, payload: UserCreate, pwd_hash: str) -> UserSchema:
        obj = User(
            username=payload.username,
            email=payload.email,
            full_name=payload.full_name,
            hashed_password=pwd_hash,
        )
        self.session.add(obj)
        await self.session.flush()
        await self.session.refresh(obj)
        return self._to_schema(obj)

    async def create_several(
        self, payloads: list[tuple[UserCreate, str]]
    ) -> UsersList:
        created: list[UserSchema] = []
        for payload, pwd_hash in payloads:
            obj = User(
                username=payload.username,
                email=payload.email,
                full_name=payload.full_name,
                hashed_password=pwd_hash,
            )
            self.session.add(obj)
            await self.session.flush()
            await self.session.refresh(obj)
            created.append(self._to_schema(obj))
        return UsersList(users=created)

    async def update(
        self,
        user_id: int,
        payload: UserUpdate,
        hashed_password: str | None = None,
    ) -> UserSchema | None:
        obj_res = await self.session.execute(
            select(User).where(User.id == user_id).limit(1)
        )
        obj = obj_res.scalar_one_or_none()
        if obj is None:
            return None

        data = payload.model_dump(exclude_unset=True)

        data.pop('password', None)
        if hashed_password is not None:
            data['hashed_password'] = hashed_password

        for key, value in data.items():
            setattr(obj, key, value)

        await self.session.flush()
        await self.session.refresh(obj)
        return self._to_schema(obj)

    async def delete(self, user_id: int) -> UserDeleteResponse | None:
        res = await self.session.execute(
            select(User).where(User.id == user_id).limit(1)
        )
        obj = res.scalar_one_or_none()
        if obj is None:
            return None

        await self.session.delete(obj)
        return UserDeleteResponse(
            id=obj.id,
            username=obj.username,
            email=obj.email,
            full_name=obj.full_name,
            is_active=obj.is_active,
            is_verified=obj.is_verified,
            roles=obj.roles,
            created_at=obj.created_at,
            updated_at=obj.updated_at,
        )
