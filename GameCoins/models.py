from sqlalchemy import Column, String, Numeric, DateTime, BigInteger
from sqlalchemy.sql import func
from database import Base

class Gampoint(Base):
    __tablename__ = "gampoints"
    
    email = Column(String, primary_key=True, index=True, nullable=False)
    jumpseller_id = Column(BigInteger, unique=True, index=True, nullable=True)
    name = Column(String, nullable=True)
    surname = Column(String, nullable=True)
    saldo = Column(Numeric(12, 2), default=0.00)
    historico_canjeado = Column(Numeric(12, 2), default=0.00)
    historico_acumulado = Column(Numeric(12, 2), default=0.00)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    created_at = Column(DateTime, server_default=func.now())