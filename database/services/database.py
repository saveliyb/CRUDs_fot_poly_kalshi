import os
# import asyncio
import logging
from contextlib import asynccontextmanager
# from threading import main_thread
from typing import AsyncIterator #, Optional

import dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine
from sqlalchemy.orm import sessionmaker
# from sqlalchemy import text


if __name__ == '__main__':
    # Настройка логирования
    logging.basicConfig(
        format="{levelname}:{name}:{message}",
        style="{",
        level=logging.INFO,
    )
    logging.getLogger("sqlalchemy.pool").setLevel(logging.DEBUG)


class Database:
    def __init__(self):
        self.engine = self.create_db_engine()
        self.session_factory = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    @staticmethod
    def create_db_engine() -> AsyncEngine:
        """Создает асинхронный engine для SQLAlchemy"""
        dotenv.load_dotenv(dotenv.find_dotenv())

        db_user = os.getenv('DB_USER')
        if not db_user:
            raise ValueError("DB_USER не указан в .env файле")

        db_password = os.getenv('DB_PASSWORD', '')
        db_host = os.getenv('DB_HOST', 'localhost')
        db_name = os.getenv('DB_NAME')
        if not db_name:
            raise ValueError("DB_NAME не указан в .env файле")

        db_port_str = os.getenv('DB_PORT')
        db_port = 5432 if db_port_str is None else int(db_port_str)

        db_url = (
            f"postgresql+asyncpg://"
            f"{db_user}:{db_password}@{db_host}:{db_port}/"
            f"{db_name}"
        )

        return create_async_engine(
            db_url,
            pool_size=int(os.getenv("DB_POOL_SIZE", 10)),
            max_overflow=int(os.getenv("DB_MAX_OVERFLOW", 60)),
            pool_recycle=int(os.getenv("DB_POOL_RECYCLE", 1800)),
            pool_pre_ping=bool(os.getenv("DB_POOL_PRE_PING", 1)),
            echo=False,
            pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", 30)),
        )

    async def close(self) -> None:
        """Закрывает engine и освобождает ресурсы"""
        if hasattr(self, 'engine'):
            await self.engine.dispose()
            del self.engine

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Контекстный менеджер для сессий"""
        async with self.session_factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()


db = Database()

# async def main():
#     db = Database()
#     try:
#         async with db.session() as session:
#             # Пример выполнения запроса
#             result = await session.execute(text("SELECT version()"))
#             print("PostgreSQL version:", result.scalar())
#     except Exception as e:
#         logging.error(f"Database error: {e}")
#     finally:
#         await db.close()
#         logging.info("Database connection closed")
#
#
# if __name__ == '__main__':
#     asyncio.run(main())
