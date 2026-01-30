from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.sql import func
from database import Base

class GameCoinUser(Base):
    __tablename__ = "users"
    
    email = Column(String, primary_key=True, index=True)
    rut = Column(String, nullable=True)  
    name = Column(String, nullable=True)  
    saldo = Column(Integer, default=0)
    historico_canjeado = Column(Integer, default=0)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), default=func.now())