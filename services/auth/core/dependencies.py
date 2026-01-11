from dataclasses import dataclass

import jwt
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from auth.settings import settings

bearer_scheme = HTTPBearer()


@dataclass(frozen=True, slots=True)
class CurrentUser:
    id: int


def decode_access_token(token: str) -> dict:
    try:
        payload = jwt.decode(
            token,
            key=settings.AUTH_JWT_SECRET,
            algorithms=[settings.ALGORITHM],
            options={'verify_aud': False},
        )
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Token expired',
        )
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Invalid token',
        )


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
) -> CurrentUser:
    token = credentials.credentials

    payload = decode_access_token(token)

    sub = payload.get('sub')
    if sub is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Token missing subject',
        )

    try:
        user_id = int(sub)
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Invalid subject in token',
        )

    token_type = payload.get('type') or payload.get('token_type')
    if token_type and token_type != 'access':
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Invalid token type',
        )

    return CurrentUser(id=user_id)
