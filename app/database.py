from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:password@db:5432/warehouse_db")

# Создаём асинхронный движок
engine = create_async_engine(DATABASE_URL, echo=False, future=True)

# Фабрика сессий
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


# Генератор сессий с корректным закрытием соединений
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
        await session.close()  # Корректное закрытие соединения
