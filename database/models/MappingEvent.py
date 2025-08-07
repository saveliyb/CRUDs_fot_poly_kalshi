from database.models.base import Base
from sqlalchemy import ForeignKey, Column, Integer, String
from sqlalchemy.orm import relationship


class MappingEvent(Base):
    __tablename__='mapping_events'
    id = Column(Integer, primary_key=True, autoincrement=True)
    kalshi_id = Column(Integer, ForeignKey("kalshi_events.id"), index=True)
    polymarket_id = Column(Integer, ForeignKey("polymarket_events.id"), index=True)
    polymarket_outcome = Column(String, nullable=False)
    polymarket_clobTokenId = Column(String, nullable=False)
    kalshi_ticker = Column(String, nullable=False)
    # Обратные ссылкив
    kalshi_event = relationship("KalshiEvent", back_populates="mappings")
    polymarket_event = relationship("PolyMarketEvent", back_populates="mappings")
