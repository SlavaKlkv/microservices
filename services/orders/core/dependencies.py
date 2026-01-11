from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import httpx
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer


@dataclass(frozen=True, slots=True)
class CurrentUser:
    id: int


bearer_scheme = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
) -> CurrentUser:
    token = credentials.credentials

    auth_base = os.getenv('AUTH_SERVICE_URL', 'http://127.0.0.1:8000')
    url = f'{auth_base.rstrip("/")}/api/v1/auth/identity'

    timeout = httpx.Timeout(connect=3.0, read=5.0, write=5.0, pool=5.0)

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(
                url,
                headers={'Authorization': f'Bearer {token}'},
            )
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f'Auth service unavailable: {e.__class__.__name__}: {e}',
        ) from e

    if resp.status_code in (
        status.HTTP_401_UNAUTHORIZED,
        status.HTTP_403_FORBIDDEN,
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Invalid credentials',
        )

    if resp.status_code >= 400:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f'Auth service error: HTTP {resp.status_code}',
        )

    payload: Any = resp.json()

    if isinstance(payload, dict) and isinstance(payload.get('data'), dict):
        payload = payload['data']

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail='Auth service returned unexpected payload',
        )

    user_id = payload.get('user_id')
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail='Auth service payload missing user_id',
        )

    try:
        return CurrentUser(id=int(user_id))
    except (TypeError, ValueError) as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail='Auth service returned non-integer user_id',
        ) from e
