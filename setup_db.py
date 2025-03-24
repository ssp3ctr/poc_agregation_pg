import asyncio
import asyncpg
import random
import json
import time
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, TIMESTAMP, String, text, insert
from sqlalchemy.future import select
from faker import Faker
from tqdm import tqdm
import os
from asyncpg.exceptions import PostgresError, DeadlockDetectedError
import random as rnd

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


class ProductsStatus(Base):
    __tablename__ = "products_status"

    product_id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, primary_key=True)
    status = Column(String, default=0)


class StatusChangeCounter(Base):
    __tablename__ = "status_change_counter"

    id = Column(Integer, primary_key=True, default=1)
    count = Column(Integer, default=0)


async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

        # Одна функция-триггер: обновляет и остатки, и статус
        await conn.execute(text("""
            CREATE OR REPLACE FUNCTION update_stock_and_status() RETURNS TRIGGER AS $$
            DECLARE
                total_balance INTEGER;
                new_status TEXT;
            BEGIN
                -- Обновляем остатки
                INSERT INTO stock_balance (product_id, warehouse_id, stock_balance)
                VALUES (NEW.product_id, NEW.warehouse_id, NEW.quantity)
                ON CONFLICT (product_id, warehouse_id) 
                DO UPDATE SET stock_balance = stock_balance.stock_balance + EXCLUDED.stock_balance;

                -- Считаем общий остаток по товару
                SELECT SUM(stock_balance)
                INTO total_balance
                FROM stock_balance
                WHERE product_id = NEW.product_id AND warehouse_id = NEW.warehouse_id;

                -- Вычисляем статус
                IF total_balance IS NULL OR total_balance <= 0 THEN
                    new_status := 'Закончился';
                ELSIF total_balance < 10 THEN
                    new_status := 'Заканчивается';
                ELSE
                    new_status := 'Доступен';
                END IF;

                -- Вставляем/обновляем статус, только если изменился
                INSERT INTO products_status (product_id, warehouse_id, status)
                VALUES (NEW.product_id, NEW.warehouse_id, new_status)
                ON CONFLICT (product_id, warehouse_id) DO UPDATE
                SET status = EXCLUDED.status
                WHERE products_status.status IS DISTINCT FROM EXCLUDED.status;

                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """))

        # Один триггер на вставку в warehouse_operations
        await conn.execute(text("""
            CREATE TRIGGER trg_update_stock_and_status
            AFTER INSERT ON warehouse_operations
            FOR EACH ROW
            EXECUTE FUNCTION update_stock_and_status();
        """))

        await conn.execute(text("""
            CREATE OR REPLACE FUNCTION notify_status_change() RETURNS TRIGGER AS $$
            DECLARE
                payload TEXT;
            BEGIN
                IF NEW.status IS DISTINCT FROM OLD.status THEN
                    payload := json_build_object(
                        'product_id', NEW.product_id,
                        'warehouse_id', NEW.warehouse_id,
                        'old_status', OLD.status,
                        'new_status', NEW.status
                    )::text;
            
                    -- Отправляем уведомление
                    PERFORM pg_notify('status_channel', payload);
            
                    -- ✅ Увеличиваем счётчик
                    UPDATE status_change_counter
                    SET count = count + 1
                    WHERE id = 1;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """))

        await conn.execute(text("""
            DROP TRIGGER IF EXISTS trg_notify_status_change ON products_status;
        """))

        await conn.execute(text("""
            CREATE TRIGGER trg_notify_status_change
            AFTER UPDATE ON products_status
            FOR EACH ROW
            WHEN (OLD.status IS DISTINCT FROM NEW.status)
            EXECUTE FUNCTION notify_status_change();
        """))

        await conn.execute(text("""
            INSERT INTO status_change_counter (id, count)
            VALUES (1, 0)
            ON CONFLICT (id) DO NOTHING;
        """))

    print("✅ Таблицы и триггер созданы!")


async def generate_test_data():
    async with SessionLocal() as session:
        total_records = 500_000
        product_ids = list(range(1, 11))
        warehouse_ids = list(range(1, 11))

        start_time = time.time()
        batch_size = 5_000

        for _ in tqdm(range(total_records // batch_size), desc="📦 Генерация данных"):
            values = [
                {
                    "product_id": random.choice(product_ids),
                    "warehouse_id": random.choice(warehouse_ids),
                    "quantity": random.randint(-5, 5),
                }
                for _ in range(batch_size)
            ]
            values.sort(key=lambda x: (x['product_id'], x['warehouse_id']))

            for attempt in range(3):
                try:
                    await session.execute(text("SET LOCAL lock_timeout = '2000ms'"))
                    await session.execute(insert(WarehouseOperations), values)
                    await session.commit()
                    break  # успешно — выходим из цикла retry
                except (DeadlockDetectedError, PostgresError) as e:
                    await session.rollback()
                    print(f"⛔ Блокировка (попытка {attempt + 1}/3): {e}")
                    await asyncio.sleep(rnd.uniform(0.1, 0.3))  # подождать и попробовать снова
                except Exception as e:
                    await session.rollback()
                    print(f"❌ Неизвестная ошибка: {e}")
                    break  # не retry, если ошибка другая

        end_time = time.time()
        print(f"✅ Данные insert загружены за {end_time - start_time:.2f} секунд")


async def generate_test_data2():
    async with SessionLocal() as session:
        total_records = 500_000
        product_ids = list(range(6, 11))
        warehouse_ids = list(range(6, 11))

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

            batch.sort(key=lambda x: (x.product_id, x.warehouse_id))

            for attempt in range(3):
                try:
                    await session.execute(text("SET LOCAL lock_timeout = '2000ms'"))
                    session.add_all(batch)  # ✅ УБРАН await
                    await session.commit()
                    break
                except (DeadlockDetectedError, PostgresError) as e:
                    await session.rollback()
                    print(f"⛔ Блокировка (попытка {attempt + 1}/3): {e}")
                    await asyncio.sleep(rnd.uniform(0.1, 0.3))
                except Exception as e:
                    await session.rollback()
                    print(f"❌ Неизвестная ошибка: {e}")
                    break

        end_time = time.time()
        print(f"✅ Данные add_all загружены за {end_time - start_time:.2f} секунд")


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

    # add_all
    #await generate_test_data2()
    #await check_stock_balances()

    # insert
    await generate_test_data()
    await check_stock_balances()


if __name__ == "__main__":
    asyncio.run(main())
