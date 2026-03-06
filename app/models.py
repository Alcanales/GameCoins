from sqlalchemy import Column, String, BigInteger, Numeric, DateTime, Integer, JSON, Float, func
from .database import Base


class Gampoint(Base):
    __tablename__ = "gampoints"

    email                = Column(String,  primary_key=True, index=True)
    jumpseller_id        = Column(BigInteger, nullable=True)
    name                 = Column(String,  nullable=True)
    surname              = Column(String,  nullable=True)
    saldo                = Column(Numeric(12, 2), default=0.0)
    historico_canjeado   = Column(Numeric(12, 2), default=0.0)
    historico_acumulado  = Column(Numeric(12, 2), default=0.0)
    updated_at           = Column(DateTime, server_default=func.now(), onupdate=func.now())
    created_at           = Column(DateTime, server_default=func.now())


class BuylistOrder(Base):
    """
    Guarda cada cotización comprometida por un vendedor externo.
    """
    __tablename__ = "buylist_orders"

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    rut                 = Column(String,  nullable=False)
    email               = Column(String,  nullable=False, index=True)
    payment_preference  = Column(String,  nullable=False)   # credito | cash | mixto
    items               = Column(JSON,    nullable=False)    # lista de cartas con precios
    total_credito       = Column(Numeric(12, 2), default=0)
    total_cash          = Column(Numeric(12, 2), default=0)
    status              = Column(String,  default="pending") # pending | reviewed | closed
    created_at          = Column(DateTime, server_default=func.now())


class StapleCard(Base):
    """
    Lista de cartas por tier de demanda:
      - normal    → stock mínimo MIN_STOCK_NORMAL (default 4)
      - alta      → stock mínimo MIN_STOCK_ALTA   (default 8)
      - muy_alta  → siempre comprar, sin límite de stock
    """
    __tablename__ = "staple_cards"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    # Nombre normalizado en minúsculas para búsqueda
    name_normalized = Column(String, nullable=False, unique=True, index=True)
    # Nombre original tal como viene de Manabox / Jumpseller
    name_display    = Column(String, nullable=False)
    # Tier: "normal" | "alta" | "muy_alta"
    tier            = Column(String, default="alta", nullable=False)
    # Stock mínimo personalizado (None = usa el default según tier)
    min_stock_override = Column(Integer, nullable=True)
    # Precio mínimo de venta personalizado en CLP (None = calculado automático)
    min_price_override = Column(Numeric(12, 2), nullable=True)
    # Factor de margen mínimo
    margin_factor   = Column(Float, default=2.5)
    added_by        = Column(String, nullable=True)
    created_at      = Column(DateTime, server_default=func.now())
    updated_at      = Column(DateTime, server_default=func.now(), onupdate=func.now())
