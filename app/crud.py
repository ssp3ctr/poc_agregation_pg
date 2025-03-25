from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import StockBalance, StatusChangeCount


async def get_all_balances(db: AsyncSession):
    result = await db.execute(select(StockBalance))
    return result.scalars().all()


async def get_balance_by_product(db: AsyncSession, product_id: int):
    result = await db.execute(select(StockBalance).where(StockBalance.product_id == product_id))
    return result.scalars().all()


async def get_balance_by_warehouse(db: AsyncSession, warehouse_id: int):
    result = await db.execute(select(StockBalance).where(StockBalance.warehouse_id == warehouse_id))
    return result.scalars().all()


async def get_balance_by_product_and_warehouse(db: AsyncSession, product_id: int, warehouse_id: int):
    result = await db.execute(
        select(StockBalance).where(
            StockBalance.product_id == product_id,
            StockBalance.warehouse_id == warehouse_id
        )
    )
    return result.scalars().first()


async def get_states_change_count(db: AsyncSession):
    result = await db.execute(
        select(StatusChangeCount)
    )
    return result.scalars().first()
