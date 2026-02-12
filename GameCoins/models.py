# GameCoins/models.py
from sqlalchemy import Column, Integer, String, Float, DateTime, func
from .database import Base

class GameCoinUser(Base):
    __tablename__ = "game_coin_users"
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    saldo = Column(Integer, default=0) # Dinero actual para gastar
    name = Column(String, nullable=True)
    surname = Column(String, nullable=True)
    historico_acumulado = Column(Integer, default=0) # Total ganado en la vida
    historico_canjeado = Column(Integer, default=0)  # Total gastado en la vida
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

class PriceCache(Base):
    __tablename__ = "price_cache"
    scryfall_id = Column(String, primary_key=True, index=True)
    name = Column(String)
    price_usd = Column(Float)
    source = Column(String, default="CardKingdom")
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

class SystemConfig(Base):
    __tablename__ = "system_configs"
    key = Column(String, primary_key=True)
    value = Column(String)