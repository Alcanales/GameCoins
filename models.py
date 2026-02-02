from sqlalchemy import Column, String, Integer
from database import Base # Importación absoluta corregida

class GameCoinUser(Base):
    __tablename__ = "gamecoin_users"
    email = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=True)
    saldo = Column(Integer, default=0)
    historico_canjeado = Column(Integer, default=0)

class SystemConfig(Base):
    __tablename__ = "system_config"
    key = Column(String, primary_key=True, index=True)
    value = Column(String)