import asyncio
from database.models.base import Base
from database.services.database import db


async def async_create_tables():
    """Асинхронно создает все таблицы в базе данных"""
    try:
        async with db.engine.begin() as conn:
            # Создаем все таблицы
            await conn.run_sync(Base.metadata.create_all)
            print("Таблицы успешно созданы")

            # Получаем список созданных таблиц (исправленная версия)
            metadata = Base.metadata
            tables = list(metadata.tables.keys())
            print(f"Созданные таблицы: {tables}")

    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")
    finally:
        await db.close()


if __name__ == '__main__':
    asyncio.run(async_create_tables())