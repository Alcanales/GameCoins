from sqlalchemy import Column, Integer, String, DateTime, func, ForeignKey
from sqlalchemy.orm import relationship
from .database import Base 

class GamePointUser(Base):
    __tablename__ = "gampoints"
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    name = Column(String, nullable=True)
    surname = Column(String, nullable=True)
    saldo = Column(Integer, default=0)
    
    historico_canjeado = Column(Integer, default=0)
    historico_acumulado = Column(Integer, default=0) 
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    transactions = relationship("GamePointTransaction", back_populates="user")

class GamePointTransaction(Base):
    __tablename__ = "point_transactions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("gampoints.id"))
    
    amount = Column(Integer)
    operation = Column(String) 
    source = Column(String) 
    description = Column(String, nullable=True)
    
    created_at = Column(DateTime, server_default=func.now())

    user = relationship("GamePointUser", back_populates="transactions")

class SystemConfig(Base):
    __tablename__ = "system_configs"
    key = Column(String, primary_key=True)
    value = Column(String)
