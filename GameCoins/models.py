from sqlalchemy import Column, Integer, String, DateTime, func, ForeignKey
from sqlalchemy.orm import relationship
from .database import Base 

class GamePointUser(Base):
    __tablename__ = "gampoints"
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    name = Column(String, nullable=True)
    surname = Column(String, nullable=True)
    
    # El "Saldo Actual" es la verdad absoluta para el frontend
    saldo = Column(Integer, default=0)
    
    # Métricas históricas
    historico_canjeado = Column(Integer, default=0)
    historico_acumulado = Column(Integer, default=0) 
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # Relación con el historial
    transactions = relationship("GamePointTransaction", back_populates="user")

class GamePointTransaction(Base):
    """
    Bitácora de movimientos para auditoría.
    """
    __tablename__ = "point_transactions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("gampoints.id"))
    
    amount = Column(Integer) # Cantidad de puntos
    operation = Column(String) # 'CREDIT' (Suma) o 'DEBIT' (Resta)
    source = Column(String) # 'MANUAL', 'SYNC', 'CANJE'
    description = Column(String, nullable=True)
    
    created_at = Column(DateTime, server_default=func.now())

    user = relationship("GamePointUser", back_populates="transactions")

class SystemConfig(Base):
    __tablename__ = "system_configs"
    key = Column(String, primary_key=True)
    value = Column(String)