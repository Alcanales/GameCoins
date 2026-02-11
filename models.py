from sqlalchemy import Column, String, Integer
from database import Base

class GameCoinUser(Base):
    __tablename__ = "gamecoins" 
    
    email = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=True)     
    surname = Column(String, nullable=True) 
    [cite_start]saldo = Column(Integer, default=0, index=True) 
    historico_canjeado = Column(Integer, default=0)

class SystemConfig(Base):
    [cite_start]"""Tabla para almacenar credenciales de Jumpseller (La Bóveda) [cite: 7]"""
    __tablename__ = "system_config"
    key = Column(String, primary_key=True, index=True)
    value = Column(String)