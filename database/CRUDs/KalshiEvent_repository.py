from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import AsyncSession
from database.models.KalshiEvent import KalshiEvent
from sqlalchemy import update, delete, select
from typing import Optional, List, Dict
import logging


async def create_kalshi_events_bulk(
        session: AsyncSession,
        events_data: List[Dict],
        batch_size: int = 100
        ) -> bool:
    """
    Асинхронно создает множество записей KalshiEvent в базе данных пакетным способом.

    :param:
        session (AsyncSession): Асинхронная сессия SQLAlchemy для работы с БД
        events_data (List[Dict]): Список словарей с данными для создания событий
        batch_size (int): Размер пакета для групповой вставки (по умолчанию 100)

    :return
        bool: True если все записи успешно добавлены, False при возникновении ошибки"""
    if not events_data:
        return True

    try:
        mapper = inspect(KalshiEvent)
        valid_columns = {columns.key for columns in mapper.attrs}

        events = []
        for data in events_data:
            filtered_data = {k: v for k, v in data.items() if k in valid_columns}
            events.append(KalshiEvent(**filtered_data))

        for i in range(0, len(events), batch_size):
            session.add_all(events[i:i+batch_size])
            await session.flush()

        await session.commit()
        return True
    except Exception as e:
        await session.rollback()
        logging.error(f"Bulk insert failed {str(e)}")
        return False



async def create_kalshi_event(session: AsyncSession, event_data: dict) -> bool:
    """
    Асинхронно создает запись о событии Kalshi в базе данных.

    Принимает данные события, фильтрует недопустимые поля и сохраняет в БД.
    В случае ошибки автоматически откатывает транзакцию.

    :param
        session (AsyncSession): Асинхронная сессия SQLAlchemy для работы с БД.
        event_data (dict): Словарь с данными события. Ключи должны соответствовать
            колонкам модели KalshiEvent.

    :return
        bool:
            - True если событие успешно создано
            - False если произошла ошибка
    """
    try:
        mapper = inspect(KalshiEvent)
        valid_columns = {column.key for column in mapper.attrs}
        filtered_data = {k: v for k, v in event_data.items() if k in valid_columns}

        event = KalshiEvent(**filtered_data)
        session.add(event)

        await session.commit()
        logging.debug(f"Successfully created event {event.ticker}")
        return True

    except Exception as e:
        logging.error(f"Error creating event: {str(e)}")
        await session.rollback()
        return False


async def update_kalshi_event(session: AsyncSession, event_id: int, event_data: dict) -> bool:
    """Асинхронно обновляет событие Kalshi

    :param
        session: Асинхронная сессия SQLAlchemy
        event_id: ID события для обновления
        event_data: Словарь с полями для обновления

    :return
        bool: True если обновление прошло успешно
    """
    try:
        mapper = inspect(KalshiEvent)
        valid_columns = {column.key for column in mapper.attrs}
        filtered_data = {k: v for k, v in event_data.items() if k in valid_columns}

        stmt = (
            update(KalshiEvent)
            .where(KalshiEvent.id == event_id)
            .values(**filtered_data)
        )

        result = await session.execute(stmt)
        await session.commit()

        if result.rowcount > 0:
            logging.debug(f"KalshiEvent {event_id} updated successfully")
            return True
        logging.debug(f"KalshiEvent {event_id} not found")
        return False

    except Exception as e:
        logging.error(f"Error updating KalshiEvent: {e}")
        await session.rollback()
        return False


async def delete_kalshi_event(session: AsyncSession, event_id: int) -> bool:
    """Асинхронно удаляет событие Kalshi"""
    try:
        stmt = delete(KalshiEvent).where(KalshiEvent.id == event_id)
        result = await session.execute(stmt)
        await session.commit()

        if result.rowcount > 0:
            logging.debug(f"KalshiEvent {event_id} deleted successfully")
            return True
        logging.debug(f"KalshiEvent {event_id} not found")
        return False

    except Exception as e:
        logging.error(f"Error deleting KalshiEvent: {e}")
        await session.rollback()
        return False


async def get_kalshi_event_by_ticker(session: AsyncSession, ticker: str) -> Optional[KalshiEvent]:
    """Асинхронно получает событие Kalshi по тикеру"""
    try:
        stmt = select(KalshiEvent).where(KalshiEvent.event_ticker == ticker)
        result = await session.execute(stmt)
        event = result.scalar_one_or_none()

        if event:
            logging.debug(f"KalshiEvent found: {event}")
            return event
        logging.debug("KalshiEvent not found")
        return None

    except Exception as e:
        logging.error(f"Error retrieving KalshiEvent: {e}")
        return None