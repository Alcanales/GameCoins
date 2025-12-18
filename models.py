import datetime
from sqlalchemy import String, Integer, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from database import Base

class GameCoinUser(Base):
    __tablename__ = "gamecoins"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)    
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    rut: Mapped[str] = mapped_column(String(50), unique=True, index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False, default="Cliente")
    surname: Mapped[str] = mapped_column(String(100), nullable=False, default="")
    saldo: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    def __repr__(self):
        return f"<User {self.email} - Saldo: {self.saldo}>"
