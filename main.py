from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.crud import (
    get_all_balances,
    get_balance_by_product,
    get_balance_by_warehouse,
    get_balance_by_product_and_warehouse
)

app = FastAPI()


@app.get("/balances", response_model=list)
async def read_all_balances(db: AsyncSession = Depends(get_db)):
    return await get_all_balances(db)


@app.get("/balances/product/{product_id}", response_model=list)
async def read_balance_by_product(product_id: int, db: AsyncSession = Depends(get_db)):
    return await get_balance_by_product(db, product_id)


@app.get("/balances/warehouse/{warehouse_id}", response_model=list)
async def read_balance_by_warehouse(warehouse_id: int, db: AsyncSession = Depends(get_db)):
    return await get_balance_by_warehouse(db, warehouse_id)


@app.get("/balances/product/{product_id}/warehouse/{warehouse_id}", response_model=dict)
async def read_balance_by_product_and_warehouse(product_id: int, warehouse_id: int, db: AsyncSession = Depends(get_db)):
    balance = await get_balance_by_product_and_warehouse(db, product_id, warehouse_id)
    return {"product_id": product_id, "warehouse_id": warehouse_id, "stock_balance": balance.stock_balance if balance else 0}
