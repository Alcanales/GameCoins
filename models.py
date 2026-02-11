from sqlalchemy import Column, String, Integer
from database import Base

class GameCoinUser(Base):
    __tablename__ = "gamecoins" 
    
    email = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=True)     
    surname = Column(String, nullable=True) 
    saldo = Column(Integer, default=0, index=True)
    historico_canjeado = Column(Integer, default=0)

class SystemConfig(Base):
    __tablename__ = "system_config"
    key = Column(String, primary_key=True, index=True)
    value = Column(String)