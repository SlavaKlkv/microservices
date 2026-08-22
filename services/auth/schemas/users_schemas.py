from datetime import datetime

from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    field_validator,
    model_validator,
)

from auth.core.constants import (
    FULL_NAME_MAX_LENGTH,
    FULL_NAME_MIN_LENGTH,
    LOGIN_MAX_LENGTH,
    LOGIN_MIN_LENGTH,
    PASSWORD_MAX_LENGTH,
    PASSWORD_MIN_LENGTH,
    USERNAME_RE,
)
from auth.core.validators import (
    validate_full_name_value,
    validate_password_value,
    validate_username_value,
)


class UserBase(BaseModel):
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,  # повторная валидация при изменении полей
    )

    username: str = Field(
        ...,
        min_length=LOGIN_MIN_LENGTH,
        max_length=LOGIN_MAX_LENGTH,
        description='Уникальный логин (3–30)',
    )
    email: EmailStr = Field(..., description='Корректный e-mail')
    full_name: str | None = Field(
        default=None,
        min_length=FULL_NAME_MIN_LENGTH,
        max_length=FULL_NAME_MAX_LENGTH,
        description='Имя и фамилия (опционально)',
    )

    @field_validator('username')
    @classmethod
    def validate_username(cls, v: str) -> str:
        return validate_username_value(v, USERNAME_RE)

    @field_validator('full_name')
    @classmethod
    def validate_full_name(cls, v: str | None) -> str | None:
        return validate_full_name_value(v)


class UserCreate(UserBase):
    password: str = Field(
        ...,
        min_length=PASSWORD_MIN_LENGTH,
        max_length=PASSWORD_MAX_LENGTH,
        description='Пароль, содержащий цифры и латинские буквы (1-128)',
        examples=['string1'],
    )

    @field_validator('password')
    @classmethod
    def validate_password(cls, v: str) -> str:
        return validate_password_value(v)


class UsersCreate(BaseModel):
    users: list[UserCreate]


class UserUpdate(BaseModel):
    username: str | None = None
    email: EmailStr | None = None
    full_name: str | None = None
    password: str | None = Field(
        default=None,
        min_length=PASSWORD_MIN_LENGTH,
        max_length=PASSWORD_MAX_LENGTH,
        examples=['string1'],
    )
    is_active: bool | None = None
    is_verified: bool | None = None
    roles: list[str] | None = None

    @field_validator('username')
    @classmethod
    def validate_username(cls, v: str | None) -> str | None:
        if v is None:
            return v
        return validate_username_value(v, USERNAME_RE)

    @field_validator('full_name')
    @classmethod
    def validate_full_name(cls, v: str | None) -> str | None:
        return validate_full_name_value(v)

    @field_validator('password')
    @classmethod
    def validate_password(cls, v: str | None) -> str | None:
        if v is None:
            return v
        return validate_password_value(v)

    @model_validator(mode='after')
    def check_any_field_set(self) -> 'UserUpdate':
        if all(
            getattr(self, name) is None
            for name in (
                'username',
                'email',
                'full_name',
                'password',
                'is_active',
                'is_verified',
                'roles',
            )
        ):
            raise ValueError(
                'должно быть передано хотя бы одно поле для обновления'
            )
        return self


class User(UserBase):
    id: int = Field(..., ge=1)
    is_active: bool = True
    is_verified: bool = False
    roles: list[str] = Field(default_factory=list)

    password_changed_at: datetime | None = None
    last_login_at: datetime | None = None

    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class UsersList(BaseModel):
    users: list[User]


class UserDeleteResponse(User):
    deleted: bool = True
