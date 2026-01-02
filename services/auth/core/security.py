import bcrypt
import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from auth.core.exceptions import TokenExpired, TokenInvalid
from auth.settings import settings

AUTH_JWT_SECRET = settings.AUTH_JWT_SECRET
ALGORITHM = settings.ALGORITHM

oauth2_scheme = OAuth2PasswordBearer(tokenUrl='/api/v1/auth/login')


def hash_password(password: str) -> str:
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')


def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))


def current_subject(token: str = Depends(oauth2_scheme)) -> str:
    """
    Проверяет и декодирует JWT, возвращает sub (идентификатор пользователя).
    """
    try:
        payload = jwt.decode(token, AUTH_JWT_SECRET, algorithms=[ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise TokenExpired()
    except jwt.PyJWTError:
        raise TokenInvalid()
    sub = payload.get('sub')
    if not sub:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Некорректный токен',
        )
    return sub
