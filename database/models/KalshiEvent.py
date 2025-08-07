from database.models.base import Base
from sqlalchemy import Column, Integer, String, Boolean, Text
from sqlalchemy.orm import relationship


class KalshiEvent(Base):
    __tablename__ = 'kalshi_events'
    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, unique=True)
    event_ticker = Column(String)
    series_ticker = Column(String)
    sub_title = Column(String)
    subtitle = Column(String)
    title = Column(Text)
    collateral_return_type = Column(String)
    mutually_exclusive = Column(Boolean)
    category = Column(Text)
    market_type = Column(String)
    yes_sub_title = Column(String)
    no_sub_title = Column(String)
    open_time = Column(String)
    close_time = Column(String)
    expected_expiration_time = Column(String)
    expiration_time = Column(String)
    latest_expiration_time = Column(String)
    settlement_timer_seconds = Column(Integer)
    status = Column(String)
    response_price_units = Column(String)
    notional_value = Column(Integer)
    tick_size = Column(Integer)
    yes_bid = Column(Integer)
    yes_ask = Column(Integer)
    no_bid = Column(Integer)
    no_ask = Column(Integer)
    last_price = Column(Integer)
    previous_yes_bid = Column(Integer)
    previous_yes_ask = Column(Integer)
    previous_price = Column(Integer)
    open_interest = Column(Integer)
    result = Column(String)
    can_close_early = Column(Boolean)
    expiration_value = Column(String)
    # category = Column(String)
    risk_limit_cents = Column(Integer)
    rules_primary = Column(Text)
    rules_secondary = Column(Text)

    mappings = relationship(
        "MappingEvent",
        back_populates="kalshi_event",
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<KalshiEvent(id={self.id}, event_ticker='{self.event_ticker}')>"


if __name__ == "__main__":
    def main():
        """Создрание таблицы"""

        def create_tables(db_url):
            from sqlalchemy import create_engine
            # try:
            engine = create_engine(db_url)
            Base.metadata.create_all(engine)
            # print("Tables created successfully")
            # except Exception as e:
            #     print(f"Error creating tables: '{e}'")

        # Загружаем переменные окружения из test.env
        load_dotenv(dotenv_path=r"/home/saveliy/PycharmProjects/kalshiPolySport/.env")
        db_name = os.getenv("db_name")
        db_user = os.getenv("db_user")
        db_password = os.getenv("db_password")
        db_host = os.getenv("db_host")
        db_port = os.getenv("db_port")
        print(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

        # Создаём таблицы
        create_tables(db_url)


    from dotenv import load_dotenv
    # from database.CRUDs.database import create_tables
    import os

    # import psycopg2
    # from psycopg2 import OperationalError

    main()
