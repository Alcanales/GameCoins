from sqlalchemy import Column, Integer, String, Float, DateTime, func
from .database import Base

class GameCoinUser(Base):
    __tablename__ = "gamecoins"  
    
    # Columnas existentes en tu captura
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    rut = Column(String, nullable=True)      
    name = Column(String, nullable=True)
    surname = Column(String, nullable=True)
    saldo = Column(Integer, default=0)
    
    # Fechas
    created_at = Column(DateTime, server_default=func.now()) # Agregado según captura
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    historico_canjeado = Column(Integer, default=0)

    historico_acumulado = Column(Integer, default=0) 

class SystemConfig(Base):
    __tablename__ = "system_configs"
    key = Column(String, primary_key=True)
    value = Column(String)