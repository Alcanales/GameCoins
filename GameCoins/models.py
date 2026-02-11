from sqlalchemy import Column, Integer, String
from .database import Base

class GameCoinUser(Base):
    __tablename__ = "game_coin_users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    saldo = Column(Integer, default=0)
    name = Column(String, nullable=True)
    surname = Column(String, nullable=True)
    historico_canjeado = Column(Integer, default=0)

class SystemConfig(Base):
    __tablename__ = "system_configs"
    key = Column(String, primary_key=True, index=True)
    value = Column(String)
