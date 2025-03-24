import asyncio
import random
import time
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, TIMESTAMP, String, text
from sqlalchemy.future import select
from faker import Faker
from tqdm import tqdm
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:password@db:5432/warehouse_db")

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()
fake = Faker()


class WarehouseOperations(Base):
    __tablename__ = "warehouse_operations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(Integer, nullable=False)
    warehouse_id = Column(Integer, nullable=False)
    quantity = Column(Integer, nullable=False)
    operation_time = Column(TIMESTAMP, server_default=text("NOW()"))


class StockBalance(Base):
    __tablename__ = "stock_balance"

    product_id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, primary_key=True)
    stock_balance = Column(Integer, default=0)


class Products_Status(Base):
    __tablename__ = "products_status"

    product_id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, primary_key=True)
    status = Column(String, default=0)


async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

        # Функция для обновления остатков
        await conn.execute(text("""
            CREATE OR REPLACE FUNCTION update_stock_balance() RETURNS TRIGGER AS $$
            BEGIN
                INSERT INTO stock_balance (product_id, warehouse_id, stock_balance)
                VALUES (NEW.product_id, NEW.warehouse_id, NEW.quantity)
                ON CONFLICT (product_id, warehouse_id) 
                DO UPDATE SET stock_balance = stock_balance.stock_balance + EXCLUDED.stock_balance;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """))

        # Триггер для обновления остатков
        await conn.execute(text("""
            CREATE TRIGGER stock_update_trigger
            AFTER INSERT ON warehouse_operations
            FOR EACH ROW
            EXECUTE FUNCTION update_stock_balance();
        """))


        # Функция для обновления статуса
        await conn.execute(text("""
            CREATE OR REPLACE FUNCTION public.update_product_status()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $function$
            DECLARE
                total_balance INTEGER;
                new_status TEXT;
            BEGIN
                SELECT SUM(stock_balance)
                INTO total_balance
                FROM stock_balance
                WHERE product_id = NEW.product_id;
            
                IF total_balance IS NULL OR total_balance <= 0 THEN
                    new_status := 'Закончился';
                ELSIF total_balance < 10 THEN
                    new_status := 'Заканчивается';
                ELSE
                    new_status := 'Доступен';
                END IF;
            
                -- Вставляем или обновляем, но обновляем только если статус изменился
                INSERT INTO products_status (product_id, status)
                VALUES (NEW.product_id, new_status)
                ON CONFLICT (product_id) DO UPDATE
                SET status = EXCLUDED.status
                WHERE products_status.status IS DISTINCT FROM EXCLUDED.status;
            
                RETURN NEW;
            END;
            $function$;
        """))

        # Триггер для обновления статуса товара
        await conn.execute(text("""
            CREATE TRIGGER products_status_trigger
            AFTER INSERT ON warehouse_operations
            FOR EACH ROW
            EXECUTE FUNCTION update_products_status();
        """))

    print("✅ Таблицы и триггеры созданы!")


async def generate_test_data():
    async with SessionLocal() as session:
        total_records = 1_000_000
        product_ids = list(range(1, 11))
        warehouse_ids = list(range(1, 11))

        start_time = time.time()

        batch_size = 5_000
        for _ in tqdm(range(total_records // batch_size), desc="📦 Генерация данных"):
            batch = [
                WarehouseOperations(
                    product_id=random.choice(product_ids),
                    warehouse_id=random.choice(warehouse_ids),
                    quantity=random.randint(-5, 5)
                )
                for _ in range(batch_size)
            ]
            session.add_all(batch)
            await session.commit()

        end_time = time.time()
        print(f"✅ Данные загружены за {end_time - start_time:.2f} секунд")


async def check_stock_balances():
    async with SessionLocal() as session:
        print("📊 Проверяем остатки товаров по складам:")
        result = await session.execute(select(StockBalance))
        balances = result.scalars().all()  # получаем ORM-объекты

        if not balances:
            print("⚠️ Нет данных в stock_balance")
            return

        for row in balances[:10]:
            print(f"Товар {row.product_id} на складе {row.warehouse_id} -> {row.stock_balance} шт.")


async def main():
    await create_tables()
    await generate_test_data()
    await check_stock_balances()


if __name__ == "__main__":
    asyncio.run(main())
