from database.models.base import Base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, Float, Date
from sqlalchemy.orm import relationship


class PolyMarketEvent(Base):
    __tablename__ = 'polymarket_events'
    id = Column(Integer, primary_key=True, autoincrement=True)
    conditionId = Column(String, unique=True)
    slug = Column(String)
    ticker = Column(String)
    startDate = Column(String)
    endDate = Column(String)
    description = Column(Text)
    outcomes = Column(Text)
    # outcomePrices = Column(Text)
    outcomePrices = Column(Text)
    # clobTokenIds = Column(Text)
    clobTokenIds = Column(Text)
    volume = Column(Float)
    active = Column(Boolean)
    closed = Column(Boolean)
    enableOrderBook = Column(Boolean)
    # enableorderbook = Column(Boolean)
    orderPriceMinTickSize = Column(Float)
    # orderpriceminticksize = Column(Float)
    orderMinSize = Column(Integer)
    # orderminsize = Column(Integer)
    acceptingOrders = Column(Boolean)
    # acceptingorders = Column(Boolean)
    negRisk = Column(Boolean)
    # negrisk = Column(Boolean)
    negRiskMarketID = Column(String)
    # negriskmarketid = Column(String)
    negRiskRequestID = Column(String)
    # negriskrequestid = Column(String)
    ready = Column(Boolean)
    clobRewardsAssetAddress = Column(String)
    # clobrewardsassetaddress = Column(String)
    clobRewardsRewardsAmount = Column(Integer)
    # clobrewardsrewardsamount = Column(Integer)
    clobRewardsRewardsDailyRate = Column(Integer)
    # clobrewardsrewardsdailyrate = Column(Integer)
    clobRewardsStartDate = Column(Date)
    # clobrewardsstartdate = Column(Date)
    clobRewardsEndDate = Column(String)
    # clobrewardsenddate = Column(String)
    rewardsMinSize = Column(Integer)
    # rewardsminsize = Column(Integer)
    rewardsMaxSpread = Column(Float)
    # rewardsmaxspread = Column(Float)
    automaticallyActive = Column(Boolean)
    # automaticallyactive = Column(Boolean)
    clearBookOnStart = Column(Boolean)
    # clearbookonstart = Column(Boolean)
    tags = Column(Text)  # JSON string of tags
    cyom = Column(Boolean)
    showAllOutcomes = Column(Boolean)
    # showalloutcomes = Column(Boolean)
    enableNegRisk = Column(Boolean)
    # enablenegrisk = Column(Boolean)
    startTime = Column(DateTime)
    # starttime = Column(DateTime)
    negRiskAugmented = Column(Boolean)
    # negriskaugmented = Column(Boolean)
    countryName = Column(String)
    # countryname = Column(String)
    electionType = Column(String)
    # electiontype = Column(String)
    pendingDeployment = Column(Boolean)
    # pendingdeployment = Column(Boolean)  # TODO переписать что бы под виндовс регистр был мелкий, под лиукнс CamelCase

    mappings = relationship(
        "MappingEvent",
        back_populates="polymarket_event",
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<PolyMarketEvent(id={self.id}, condition_id='{self.condition_id}')>"


if __name__ == "__main__":
    def main():
        """Создание таблицы"""

        def create_tables(db_url):
            from sqlalchemy import create_engine
            try:
                engine = create_engine(db_url)
                Base.metadata.create_all(engine)
                print("Tables created successfully")
            except Exception as e:
                print(f"Error creating tables: '{e}'")

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
    import os

    main()
