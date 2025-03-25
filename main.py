import asyncpg
import asyncio
import os
import json
from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.schemas import StockBalanceSchema, StatusChanges
from app.crud import (
    get_all_balances,
    get_balance_by_product,
    get_balance_by_warehouse,
    get_balance_by_product_and_warehouse,
    get_states_change_count
)

app = FastAPI()
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:password@db:5432/warehouse_db")
status_listener_connection: asyncpg.Connection | None = None
listener_task: asyncio.Task | None = None


async def _keep_alive():
    while True:
        await asyncio.sleep(3600)


async def handle_status_notification(payload: str):
    data = json.loads(payload)
    print(
        f"🔔 Статус изменился: товар {data['product_id']} "
        f"(склад {data['warehouse_id']}): "
        f"{data['old_status']} → {data['new_status']}")


def notification_callback(connection, pid, channel, payload):
    asyncio.create_task(handle_status_notification(payload))


@app.get("/balances", response_model=list[StockBalanceSchema])
async def read_all_balances(db: AsyncSession = Depends(get_db)):
    return await get_all_balances(db)


@app.get("/status-change-count", response_model=StatusChanges)
async def read_all_balances(db: AsyncSession = Depends(get_db)):
    return await get_states_change_count(db)


@app.get("/balances/product/{product_id}", response_model=list[StockBalanceSchema])
async def read_balance_by_product(product_id: int, db: AsyncSession = Depends(get_db)):
    return await get_balance_by_product(db, product_id)


@app.get("/balances/warehouse/{warehouse_id}", response_model=list[StockBalanceSchema])
async def read_balance_by_warehouse(warehouse_id: int, db: AsyncSession = Depends(get_db)):
    return await get_balance_by_warehouse(db, warehouse_id)


@app.get("/balances/product/{product_id}/warehouse/{warehouse_id}")
async def read_balance_by_product_and_warehouse(product_id: int, warehouse_id: int, db: AsyncSession = Depends(get_db)):
    balance = await get_balance_by_product_and_warehouse(db, product_id, warehouse_id)
    return {"product_id": product_id, "warehouse_id": warehouse_id, "stock_balance": balance.stock_balance if balance else 0}


@app.on_event("startup")
async def on_startup():
    global status_listener_connection, listener_task
    status_listener_connection = await asyncpg.connect(DATABASE_URL.replace("postgresql+asyncpg", "postgresql"))
    await status_listener_connection.add_listener("status_channel", notification_callback)
    print("🛰️ Слушаю изменения статусов...")

    listener_task = asyncio.create_task(_keep_alive())


@app.on_event("shutdown")
async def on_shutdown():
    global status_listener_connection, listener_task

    if listener_task:
        listener_task.cancel()
        try:
            await listener_task
        except asyncio.CancelledError:
            pass

    if status_listener_connection is not None:
        await status_listener_connection.close()
        print("❌ Слушатель отключён.")


