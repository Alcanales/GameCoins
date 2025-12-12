from sqlalchemy import Column, Integer, String, DateTime
from database import Base
import datetime

class GameCoinUser(Base):
    __tablename__ = "gamecoins"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    name = Column(String, default="")
    rut = Column(String, default="")
    saldo = Column(Integer, default=0)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
