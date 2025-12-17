from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func
from database import Base
import datetime

class GameCoinUser(Base):
    __tablename__ = "gamecoins"

    id = Column(Integer, primary_key=True, index=True)    
    email = Column(String(255), unique=True, index=True, nullable=False)
    rut = Column(String(50), unique=True, index=True, nullable=False)
    name = Column(String(100), nullable=False, default="Cliente")
    surname = Column(String(100), nullable=False, default="")
    saldo = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<User {self.email} - Saldo: {self.saldo}>"
