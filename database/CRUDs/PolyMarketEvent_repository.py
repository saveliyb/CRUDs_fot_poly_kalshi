from sqlalchemy import inspect, update, delete, select
from database.models.PolyMarketEvent import PolyMarketEvent
from typing import Optional, Dict, Any, List
import logging

import json
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
import sqlalchemy


async def create_polymarket_events_bulk(
        session: AsyncSession,
        events_data: List[Dict],
        batch_size: int = 100
) -> bool:
    """
    Final robust solution with complete datetime handling
    """
    if not events_data:
        return True

    try:
        # Get column information including exact SQL type
        mapper = inspect(PolyMarketEvent)
        columns_info = {
            col.key: {
                'type': col.type.python_type,
                'sql_type': str(col.type),
                'is_datetime': isinstance(col.type, (sqlalchemy.DateTime, sqlalchemy.Date)),
                'is_tz_aware': 'WITH TIME ZONE' in str(col.type)
            }
            for col in mapper.columns
            if col.key != 'id'
        }

        events = []
        for data in events_data:
            filtered_data = {}
            for key, value in data.items():
                if key not in columns_info:
                    continue

                col_info = columns_info[key]

                # Handle None values
                if value is None:
                    filtered_data[key] = None
                    continue

                try:
                    # Special handling for datetime fields
                    if col_info['is_datetime']:
                        if isinstance(value, str):
                            # Parse string to datetime
                            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))

                            # Convert to timezone-naive if needed
                            if not col_info['is_tz_aware']:
                                if dt.tzinfo is not None:
                                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                            else:
                                if dt.tzinfo is None:
                                    dt = dt.replace(tzinfo=timezone.utc)

                            filtered_data[key] = dt
                        elif isinstance(value, datetime):
                            # Convert existing datetime objects
                            if not col_info['is_tz_aware']:
                                if value.tzinfo is not None:
                                    filtered_data[key] = value.astimezone(timezone.utc).replace(tzinfo=None)
                                else:
                                    filtered_data[key] = value
                            else:
                                if value.tzinfo is None:
                                    filtered_data[key] = value.replace(tzinfo=timezone.utc)
                                else:
                                    filtered_data[key] = value
                        else:
                            filtered_data[key] = None

                    # Handle numeric types
                    elif col_info['type'] is float:
                        filtered_data[key] = float(value)
                    elif col_info['type'] is int:
                        filtered_data[key] = int(float(value)) if value else 0

                    # Handle boolean types
                    elif col_info['type'] is bool:
                        filtered_data[key] = bool(value)

                    # Handle JSON serializable types
                    elif isinstance(value, (list, dict)):
                        filtered_data[key] = json.dumps(value)

                    # Default string conversion
                    else:
                        filtered_data[key] = str(value)

                except (ValueError, TypeError) as e:
                    logging.warning(f"Conversion failed for {key}={value}: {str(e)}")
                    filtered_data[key] = None

            if filtered_data:
                events.append(PolyMarketEvent(**filtered_data))

        # Batch insert
        for i in range(0, len(events), batch_size):
            session.add_all(events[i:i + batch_size])
            await session.flush()

        await session.commit()
        return True

    except Exception as e:
        await session.rollback()
        logging.error(f"Bulk insert failed: {str(e)}", exc_info=True)
        return False



async def update_poly_market_event(
        session: AsyncSession,
        condition_id: str,
        event_data: Dict[str, Any]
) -> bool:
    """Асинхронно обновляет событие PolyMarket"""
    try:
        # Фильтруем данные
        mapper = inspect(PolyMarketEvent)
        valid_columns = {column.key for column in mapper.attrs}
        filtered_data = {k: v for k, v in event_data.items() if k in valid_columns}

        if not filtered_data:
            logging.warning("No valid fields to update")
            return False

        stmt = (
            update(PolyMarketEvent)
            .where(PolyMarketEvent.condition_id == condition_id)
            .values(**filtered_data)
        )

        result = await session.execute(stmt)
        await session.commit()

        if result.rowcount > 0:
            logging.info(f"Event {condition_id} updated successfully")
            return True

        logging.warning(f"Event {condition_id} not found")
        return False

    except Exception as e:
        logging.error(f"Error updating event: {str(e)}")
        await session.rollback()
        return False


async def delete_poly_market_event(
        session: AsyncSession,
        condition_id: str
) -> bool:
    """Асинхронно удаляет событие PolyMarket"""
    try:
        stmt = (
            delete(PolyMarketEvent)
            .where(PolyMarketEvent.condition_id == condition_id)
        )

        result = await session.execute(stmt)
        await session.commit()

        if result.rowcount > 0:
            logging.info(f"Event {condition_id} deleted successfully")
            return True

        logging.warning(f"Event {condition_id} not found")
        return False

    except Exception as e:
        logging.error(f"Error deleting event: {str(e)}")
        await session.rollback()
        return False


async def get_poly_market_event(
        session: AsyncSession,
        condition_id: str
) -> Optional[PolyMarketEvent]:
    """Асинхронно получает событие PolyMarket по condition_id"""
    try:
        stmt = (
            select(PolyMarketEvent)
            .where(PolyMarketEvent.condition_id == condition_id)
        )

        result = await session.execute(stmt)
        event = result.scalar_one_or_none()

        if event:
            logging.debug(f"Retrieved event {condition_id}")
            return event

        logging.debug(f"Event {condition_id} not found")
        return None

    except Exception as e:
        logging.error(f"Error retrieving event: {str(e)}")
        return None