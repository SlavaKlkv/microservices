from fastapi import APIRouter

from routers.auth import auth_router
from routers.users import users_router

auth_users = APIRouter(prefix='/auth_users')

for router in (
    auth_router,
    users_router,
):
    auth_users.include_router(router)
