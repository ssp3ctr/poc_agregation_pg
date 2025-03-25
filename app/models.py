from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer

Base = declarative_base()


class StockBalance(Base):
    __tablename__ = "stock_balance"

    product_id = Column(Integer, nullable=False, primary_key=True)
    warehouse_id = Column(Integer, nullable=False, primary_key=True)
    stock_balance = Column(Integer, nullable=False, default=0)


class StatusChangeCount(Base):
    __tablename__ = "status_change_counter"

    id = Column(Integer, nullable=False, primary_key=True)
    count = Column(Integer,nullable=False)
