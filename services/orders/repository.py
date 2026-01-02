from orders.models import Order
from orders.schemas import OrderDelete
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession


class OrderRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_id(self, order_id: int) -> Order | None:
        stmt = select(Order).where(Order.id == order_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_all(
        self,
        *,
        limit: int,
        offset: int,
    ) -> tuple[list[Order], int]:
        stmt = select(Order).order_by(Order.id).limit(limit).offset(offset)
        result = await self.session.execute(stmt)
        items: list[Order] = list(result.scalars().all())

        count_stmt = select(func.count(Order.id))
        count_result = await self.session.execute(count_stmt)
        total: int = int(count_result.scalar_one())

        return items, total

    async def create(self, order: Order) -> Order:
        self.session.add(order)
        await self.session.flush()
        return order

    async def update(self, order: Order) -> Order:
        await self.session.flush()
        return order

    async def delete(self, obj: Order) -> OrderDelete:
        deleted = OrderDelete.model_validate(obj)
        await self.session.delete(obj)
        await self.session.flush()
        return deleted
