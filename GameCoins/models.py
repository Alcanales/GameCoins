from sqlalchemy import Column, Integer, String, DateTime, func
from .database import Base 

class GameCoinUser(Base):
    """
    Representa la tabla 'gamecoins' según el esquema real en DBeaver.
    """
    __tablename__ = "gamecoins"
    
    # Columnas identificadas en la captura de DBeaver
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    name = Column(String, nullable=True)
    surname = Column(String, nullable=True)
    saldo = Column(Integer, default=0)
    
    # Timestamps (Icono de reloj en DBeaver)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Contadores históricos (Integer/123 en DBeaver)
    historico_canjeado = Column(Integer, default=0)
    historico_acumulado = Column(Integer, default=0) 

class SystemConfig(Base):
  
    __tablename__ = "system_configs"
    key = Column(String, primary_key=True)
    value = Column(String)