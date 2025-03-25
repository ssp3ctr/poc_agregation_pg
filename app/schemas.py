from pydantic import BaseModel


class StockBalanceSchema(BaseModel):
    product_id: int
    warehouse_id: int
    stock_balance: int

    class Config:
        orm_mode = True


class StatusChanges(BaseModel):
    count: int
