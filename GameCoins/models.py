from sqlalchemy import Column, String, BigInteger, Numeric, DateTime, func
from .database import Base

class Gampoint(Base):
    __tablename__ = "gampoints"
    email = Column(String, primary_key=True, index=True)
    jumpseller_id = Column(BigInteger, nullable=True)
    name = Column(String, nullable=True)
    surname = Column(String, nullable=True)
    saldo = Column(Numeric(12, 2), default=0.0)
    historico_canjeado = Column(Numeric(12, 2), default=0.0)
    historico_acumulado = Column(Numeric(12, 2), default=0.0)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    created_at = Column(DateTime, server_default=func.now())