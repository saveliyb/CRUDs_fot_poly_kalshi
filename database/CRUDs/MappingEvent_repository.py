from sqlalchemy import inspect, result_tuple
from sqlalchemy.ext.asyncio import AsyncSession
import json
from database.models import KalshiEvent, PolyMarketEvent
from database.models.MappingEvent import MappingEvent
from sqlalchemy import update, delete, select, distinct
from typing import Optional, List, Sequence
import logging
from sqlalchemy.orm import joinedload


async def create_mapping_event(
        session: AsyncSession,
        kalshi_id: int,
        polymarket_id: int,
        polymarket_outcome: str
) -> Optional[MappingEvent]:
    """
    Создает новую связь между событиями Kalshi и Polymarket.
    Автоматически находит clobTokenId и kalshi_ticker.

    Args:
        session: Асинхронная сессия SQLAlchemy
        kalshi_id: ID события в Kalshi
        polymarket_id: ID события в Polymarket
        polymarket_outcome: Название исхода в Polymarket

    Returns:
        Созданный MappingEvent или None в случае ошибки

    Raises:
        ValueError: Если событие не найдено или outcome не существует
        json.JSONDecodeError: Если не удалось распарсить JSON данные
    """
    try:
        # Получаем событие Polymarket
        pm_stmt = select(PolyMarketEvent).where(PolyMarketEvent.id == polymarket_id)
        pm_result = await session.execute(pm_stmt)
        polymarket_event = pm_result.scalar_one_or_none()

        if not polymarket_event:
            raise ValueError(f"PolyMarketEvent with id {polymarket_id} not found")

        # Получаем событие Kalshi
        kalshi_stmt = select(KalshiEvent).where(KalshiEvent.id == kalshi_id)
        kalshi_result = await session.execute(kalshi_stmt)
        kalshi_event = kalshi_result.scalar_one_or_none()

        if not kalshi_event:
            raise ValueError(f"KalshiEvent with id {kalshi_id} not found")

        # Парсим JSON данные Polymarket
        try:
            outcomes = json.loads(polymarket_event.outcomes)
            clob_token_ids = json.loads(polymarket_event.clobTokenIds)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in PolyMarketEvent data: {str(e)}")

        # Проверяем соответствие массивов
        if len(outcomes) != len(clob_token_ids):
            raise ValueError("Mismatch between outcomes and clobTokenIds arrays")

        # Ищем нужный outcome
        try:
            outcome_index = outcomes.index(polymarket_outcome)
        except ValueError:
            available_outcomes = ", ".join(outcomes)
            raise ValueError(
                f"Outcome '{polymarket_outcome}' not found. "
                f"Available outcomes: {available_outcomes}"
            )

        # Создаем маппинг
        mapping = MappingEvent(
            kalshi_id=kalshi_id,
            polymarket_id=polymarket_id,
            polymarket_outcome=polymarket_outcome,
            polymarket_clobTokenId=clob_token_ids[outcome_index],
            kalshi_ticker=kalshi_event.ticker  # Добавляем ticker из KalshiEvent
        )

        session.add(mapping)
        await session.commit()
        logging.info(
            f"Created mapping {mapping.id} for "
            f"Kalshi:{kalshi_id}({kalshi_event.ticker}) ↔ "
            f"Polymarket:{polymarket_id}({polymarket_outcome})"
        )
        return mapping

    except ValueError as e:
        logging.warning(f"Validation error creating mapping: {str(e)}")
        await session.rollback()
        return None

    except Exception as e:
        logging.error(f"Unexpected error creating mapping: {str(e)}", exc_info=True)
        await session.rollback()
        return None

async def get_mapping_by_ids(
        session: AsyncSession,
        kalshi_id: int,
        polymarket_id: int
) -> Optional[MappingEvent]:
    """Получает связь по ID событий"""
    try:
        stmt = (
            select(MappingEvent)
            .where(
                MappingEvent.kalshi_id == kalshi_id,
                MappingEvent.polymarket_id == polymarket_id
            )
            .options(
                joinedload(MappingEvent.polymarket_event),
                joinedload(MappingEvent.kalshi_event)
            )
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()
    except Exception as e:
        logging.error(f"Error getting mapping: {str(e)}")
        return None

async def get_mapping_by_kalshi_id(
        session: AsyncSession,
        kalshi_id: int
        ) -> Sequence[MappingEvent]:
    """Получает все связи для события Kalshi"""
    try:
        stmt = select(MappingEvent).where(
            MappingEvent.kalshi_event == kalshi_id
        )
        result = await session.execute(stmt)
        return result.scalars().all()
    except Exception as e:
        logging.error(f"Error getting mapping by Kalshi ID: {str(e)}")
        return []

async def get_mapping_by_polymarket_id(
        session: AsyncSession,
        polymarket_id: int
        )-> Sequence[MappingEvent]:
    """Получает все связи для события Polymarket"""
    try:
        stmt = select(MappingEvent).where(
            MappingEvent.polymarket_event == polymarket_id
        )
        result = await session.execute(stmt)
        return result.scalars().all()
    except Exception as e:
        logging.error(f"Error getting mapping by Polymarket ID: {str(e)}")
        return []


async def update_mapping(
        session: AsyncSession,
        mapping_id: int,
        kalshi_id: Optional[int] = None,
        polymarket_id: Optional[int] = None
        ) -> bool:
    """Обновляет связь между событиями"""
    try:
        update_data = {}
        if kalshi_id is not None:
            update_data["kalshi_id"] = kalshi_id
        if polymarket_id is not None:
            update_data["polymarket_id"] = polymarket_id

        if not update_data:
            return False

        stmt = (
            update(MappingEvent)
            .where(MappingEvent.id==mapping_id)
            .values(**update_data)
        )

        result = await session.execute(stmt)
        await session.commit()

        if result.rowcount > 0:
            logging.debug(f"Mapping {mapping_id} updated")
            return True
        return False
    except Exception as e:
        logging.error(f"Error updating mapping: {str(e)}")
        await session.rollback()
        return False

async def delete_mapping(
        session: AsyncSession,
        mapping_id: int
        ) -> bool:
    try:
        stmt = delete(MappingEvent).where(MappingEvent.id == mapping_id)
        result = await session.execute(stmt)
        await session.commit()

        if result.rowcount > 0:
            logging.debug(f"Mapping {mapping_id} deleted")
            return True
        return False
    except Exception as e:
        logging.error(f"Error deleting mapping: {str(e)}")
        await session.rollback()
        return False

async def delete_mapping_be_ecent_ids(
        session: AsyncSession,
        kalshi_id: int,
        polymarket_id: int
        ) -> bool:
    try:
        stmt =delete(MappingEvent).where(
            MappingEvent.kalshi_id == kalshi_id,
            MappingEvent.polymarket_id == polymarket_id
        )
        result = await session.execute(stmt)
        await session.commit()

        if result.rowcount > 0:
            logging.debug(f"Mapping between {kalshi_id} and {polymarket_id} deleted")
            return True
        return False
    except Exception as e:
        logging.error(f"Error deleting mapping by event ID: {str(e)}")
        await session.rollback()
        return False

async def get_related_kalshi_events(
        session: AsyncSession,
        polymarket_id: int
        ) -> List[KalshiEvent]:
    """Получает все связанные Kalshi события для Polymarket события"""
    try:
        stmt = select(KalshiEvent).join(
            MappingEvent,
            KalshiEvent.id == MappingEvent.kalshi_id
        ).where(MappingEvent.polymarket_id == polymarket_id)
        result = await session.execute(stmt)
        return result.scalars().all()
    except Exception as e:
        logging.debug(f"Error getting related kalshi events: {str(e)}")
        return []

async def get_related_polymarket_events(
        session: AsyncSession,
        kalshi_id: int
        ) -> List[PolyMarketEvent]:
    """Получает все связанные Polymarket события для Kalshi события"""
    try:
        stmt = select(PolyMarketEvent).join(
            MappingEvent,
            PolyMarketEvent.id == MappingEvent.polymarket_id
        ).where(
            MappingEvent.kalshi_id == kalshi_id
        )
        result = await session.execute(stmt)
        return result.scalars().all()
    except Exception as e:
        logging.debug(f"Error getting related Polymarket events: {str(e)}")
        return []


async def get_all_kalshi_tickers(session: AsyncSession) -> List[str]:
    """
    Получает все уникальные kalshi_ticker из таблицы mapping_events

    Args:
        session: Асинхронная сессия SQLAlchemy

    Returns:
        Список уникальных тикеров Kalshi
    """
    try:
        stmt = select(distinct(MappingEvent.kalshi_ticker))
        result = await session.execute(stmt)
        tickers = result.scalars().all()
        return list(tickers) if tickers else []
    except Exception as e:
        logging.error(f"Error getting kalshi tickers: {str(e)}")
        return []


async def get_all_polymarket_clob_token_ids(session: AsyncSession) -> List[str]:
    """
    Получает все уникальные polymarket_clobTokenId из таблицы mapping_events

    Args:
        session: Асинхронная сессия SQLAlchemy

    Returns:
        Список уникальных clobTokenId Polymarket
    """
    try:
        stmt = select(distinct(MappingEvent.polymarket_clobTokenId))
        result = await session.execute(stmt)
        token_ids = result.scalars().all()
        return list(token_ids) if token_ids else []
    except Exception as e:
        logging.error(f"Error getting polymarket clobTokenIds: {str(e)}")
        return []