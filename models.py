from sqlalchemy import Column, Integer, String, DateTime
from database import Base
import datetime

class GameCoinUser(Base):
    __tablename__ = "gamecoins"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    rut = Column(String, unique=True, index=True, nullable=False)
    name = Column(String, nullable=False, default="Cliente")
    surname = Column(String, nullable=False, default="")
    saldo = Column(Integer, default=0, nullable=False)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
