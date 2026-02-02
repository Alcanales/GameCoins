from sqlalchemy import Column, String, Integer
from database import Base

class GameCoinUser(Base):
    __tablename__ = "gamecoin_users"
    
    email = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=True)
    saldo = Column(Integer, default=0)
    historico_canjeado = Column(Integer, default=0)

class SystemConfig(Base):
    """
    La Bóveda: Almacena credenciales dinámicas (Tokens de API de Jumpseller)
    que pueden rotar sin redeployar la aplicación.
    """
    __tablename__ = "system_config"
    
    key = Column(String, primary_key=True, index=True)
    value = Column(String)