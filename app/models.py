from sqlalchemy import Column, String, BigInteger, Numeric, DateTime, Integer, JSON, Float, func, UniqueConstraint
from sqlalchemy.ext.mutable import MutableList
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
    """Guarda cada cotización comprometida por un vendedor externo."""
    __tablename__ = "buylist_orders"

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    rut                 = Column(String,  nullable=False)
    email               = Column(String,  nullable=False, index=True)
    payment_preference  = Column(String,  nullable=False)
    items               = Column(MutableList.as_mutable(JSON), nullable=False)
    total_credito       = Column(Numeric(12, 2), default=0)
    total_cash          = Column(Numeric(12, 2), default=0)
    status              = Column(String,  default="pending")
    created_at          = Column(DateTime, server_default=func.now())
    updated_at          = Column(DateTime, server_default=func.now(), onupdate=func.now())


class StapleCard(Base):
    """
    Lista de cartas por tier de demanda:
      normal / alta / muy_alta
    """
    __tablename__ = "staple_cards"

    id                 = Column(Integer,        primary_key=True, autoincrement=True)
    name_normalized    = Column(String,         nullable=False, unique=True, index=True)
    name_display       = Column(String,         nullable=False)
    tier               = Column(String,         default="alta", nullable=False)
    min_stock_override = Column(Integer,        nullable=True)
    min_price_override = Column(Numeric(12, 2), nullable=True)
    margin_factor      = Column(Float,          default=2.5)
    added_by           = Column(String,         nullable=True)
    created_at         = Column(DateTime,       server_default=func.now())
    updated_at         = Column(DateTime,       server_default=func.now(), onupdate=func.now())


class CardCatalog(Base):
    """
    Catálogo persistente: nombre canónico → product IDs de Jumpseller.
    """
    __tablename__ = "card_catalog"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    name_normalized = Column(String,  nullable=False, unique=True, index=True)
    name_display    = Column(String,  nullable=False)
    js_product_ids  = Column(MutableList.as_mutable(JSON), default=list)
    js_variants     = Column(MutableList.as_mutable(JSON), default=list)
    scryfall_ids    = Column(MutableList.as_mutable(JSON), default=list)
    total_stock     = Column(Integer,  default=0)
    last_synced     = Column(DateTime, nullable=True)
    created_at      = Column(DateTime, server_default=func.now())
    updated_at      = Column(DateTime, server_default=func.now(), onupdate=func.now())


class CKPrice(Base):
    """
    Precio mínimo de buylist CardKingdom por nombre canónico.
    Job diario via _sync_ck_prices().
    """
    __tablename__ = "ck_prices"

    name_canonical  = Column(String,         primary_key=True, index=True)
    name_raw        = Column(String,         nullable=False)
    min_buy_price   = Column(Numeric(10, 4), nullable=False)
    nicho_threshold = Column(Numeric(10, 4), nullable=False)
    updated_at      = Column(DateTime,       server_default=func.now(), onupdate=func.now())


class CashbackRecord(Base):
    """
    Idempotencia del cashback 2%: INSERT ON CONFLICT DO NOTHING previene doble acreditación.
    """
    __tablename__ = "cashback_records"

    order_id        = Column(BigInteger,     primary_key=True)
    email           = Column(String,         nullable=False, index=True)
    amount_qp       = Column(Numeric(12, 2), nullable=False)
    order_total_clp = Column(Numeric(12, 2), nullable=False)
    created_at      = Column(DateTime,       server_default=func.now())


class CanjeRecord(Base):
    """
    Historial de canjes QuestPoints → cupón Jumpseller.

    Una fila por canje EXITOSO — solo se inserta en Paso 7a de process_canje()
    cuando el cupón fue creado en Jumpseller. Canje revertido = sin fila.

    PROPÓSITO:
    - Cliente: último canje vía GET /api/public/last_canje/{email}
    - Admin:   historial vía GET /api/admin/canje_history/{email}

    CAMPOS:
    - email:           usuario que canjeó
    - amount_qp:       QP debitados (= valor del cupón en CLP)
    - coupon_code:     código QP-XXXXXX
    - monto_original:  QP solicitados (puede > amount_qp si se aplicó cart-cap QA-02)
    - cart_total:      total del carrito al momento del canje
    - adjusted:        1 si se aplicó cart-cap, 0 si el monto fue exacto
    - created_at:      timestamp UTC
    """
    __tablename__ = "canje_records"

    id             = Column(Integer,        primary_key=True, autoincrement=True)
    email          = Column(String,         nullable=False, index=True)
    amount_qp      = Column(Numeric(12, 2), nullable=False)
    coupon_code    = Column(String,         nullable=False)
    monto_original = Column(Numeric(12, 2), nullable=True)
    cart_total     = Column(Numeric(12, 2), nullable=True)
    adjusted       = Column(Integer,        default=0)
    created_at     = Column(DateTime,       server_default=func.now(), index=True)


# ─────────────────────────────────────────────────────────────────────────────
# MIGRACIONES SQL — solo si la BD ya existe y la tabla no fue creada
# SQLAlchemy crea tablas nuevas automáticamente vía Base.metadata.create_all()
# ─────────────────────────────────────────────────────────────────────────────
#
# buylist_orders.updated_at (FIX M-06):
#   ALTER TABLE buylist_orders ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();
#   UPDATE buylist_orders SET updated_at = created_at WHERE updated_at IS NULL;
#
# ck_prices:
#   CREATE TABLE IF NOT EXISTS ck_prices (
#       name_canonical TEXT PRIMARY KEY, name_raw TEXT NOT NULL,
#       min_buy_price NUMERIC(10,4) NOT NULL, nicho_threshold NUMERIC(10,4) NOT NULL,
#       updated_at TIMESTAMP DEFAULT NOW()
#   );
#   -- Si ya existe pero le falta nicho_threshold:
#   ALTER TABLE ck_prices ADD COLUMN IF NOT EXISTS nicho_threshold NUMERIC(10,4);
#   UPDATE ck_prices SET nicho_threshold = min_buy_price * 1.5 WHERE nicho_threshold IS NULL;
#
# canje_records (NUEVO v5.5):
#   CREATE TABLE IF NOT EXISTS canje_records (
#       id SERIAL PRIMARY KEY, email TEXT NOT NULL,
#       amount_qp NUMERIC(12,2) NOT NULL, coupon_code TEXT NOT NULL,
#       monto_original NUMERIC(12,2), cart_total NUMERIC(12,2),
#       adjusted INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT NOW()
#   );
#   CREATE INDEX IF NOT EXISTS ix_canje_records_email   ON canje_records (email);
#   CREATE INDEX IF NOT EXISTS ix_canje_records_created ON canje_records (created_at);
# ─────────────────────────────────────────────────────────────────────────────
