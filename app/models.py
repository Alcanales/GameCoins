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
    """
    Guarda cada cotización comprometida por un vendedor externo.
    """
    __tablename__ = "buylist_orders"

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    rut                 = Column(String,  nullable=False)
    email               = Column(String,  nullable=False, index=True)
    payment_preference  = Column(String,  nullable=False)   # credito | cash | mixto
    # FIX A6: MutableList.as_mutable() → SQLAlchemy detecta .append()/.remove()
    # y marca el campo como dirty sin necesitar reasignación explícita.
    items               = Column(MutableList.as_mutable(JSON), nullable=False)
    total_credito       = Column(Numeric(12, 2), default=0)
    total_cash          = Column(Numeric(12, 2), default=0)
    status              = Column(String,  default="pending") # pending | reviewed | closed | cancelled
    created_at          = Column(DateTime, server_default=func.now())
    # FIX M-06: updated_at para auditoría de cambios de estado.
    # server_default garantiza que filas existentes sin la columna reciban NOW().
    # onupdate=func.now() actualiza automáticamente al hacer db.commit() con cambios.
    # Migración requerida en BD existente (ver script SQL al pie de este archivo).
    updated_at          = Column(DateTime, server_default=func.now(), onupdate=func.now())


class StapleCard(Base):
    """
    Lista de cartas por tier de demanda:
      - normal    → stock mínimo MIN_STOCK_NORMAL (default 4)
      - alta      → stock mínimo MIN_STOCK_ALTA   (default 8)
      - muy_alta  → siempre comprar, sin límite de stock
    """
    __tablename__ = "staple_cards"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    name_normalized = Column(String, nullable=False, unique=True, index=True)
    name_display    = Column(String, nullable=False)
    tier            = Column(String, default="alta", nullable=False)
    min_stock_override = Column(Integer, nullable=True)
    min_price_override = Column(Numeric(12, 2), nullable=True)
    margin_factor   = Column(Float, default=2.5)
    added_by        = Column(String, nullable=True)
    created_at      = Column(DateTime, server_default=func.now())
    updated_at      = Column(DateTime, server_default=func.now(), onupdate=func.now())


class CardCatalog(Base):
    """
    Catálogo persistente: nombre canónico → todos los product IDs de Jumpseller.

    Resuelve el problema de que el caché RAM se pierde al reiniciar:
    - Cada fila = una carta única identificada por su nombre canónico
    - js_product_ids: lista de IDs de productos JS (todas las ediciones/versiones)
    - js_variants: snapshot de las variantes con stock y precio
    - total_stock: suma de stock de todas las variantes (cacheado)
    - last_synced: cuándo se sincronizó desde la API de JS

    Lookup flow (analyze_buylist / stock_check):
      canonical = _canonical(csv_name)
      catalog_entry = catalog_map[canonical]     # O(1), cargado en RAM al arrancar
      product_ids   = catalog_entry.js_product_ids
      total_stock   = sum(js_by_id[pid]["stock"] for pid in product_ids if pid in js_by_id)
    """
    __tablename__ = "card_catalog"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    name_normalized = Column(String, nullable=False, unique=True, index=True)  # clave canónica
    name_display    = Column(String, nullable=False)                            # nombre para mostrar
    # FIX A6: MutableList.as_mutable() en todas las columnas JSON que son listas.
    # Sin esto, SQLAlchemy no detecta mutaciones in-place (.append(), [i]=x)
    # y los cambios se pierden silenciosamente sin error.
    js_product_ids  = Column(MutableList.as_mutable(JSON), default=list)
    js_variants     = Column(MutableList.as_mutable(JSON), default=list)
    scryfall_ids    = Column(MutableList.as_mutable(JSON), default=list)
    total_stock     = Column(Integer, default=0)
    last_synced     = Column(DateTime, nullable=True)
    created_at      = Column(DateTime, server_default=func.now())
    updated_at      = Column(DateTime, server_default=func.now(), onupdate=func.now())


class CKPrice(Base):
    """
    Precio mínimo de buylist de CardKingdom por nombre canónico de carta.

    REGLA DE NEGOCIO:
    - Se guarda el precio MÁS BAJO encontrado en la pricelist CK para ese nombre,
      ignorando edición, variante y foil (is_foil=True se descarta al importar).
    - Una carta es "De Nicho" si su precio en el CSV supera min_buy_price × STAKE_MULTIPLIER.
    - Si la carta no existe en esta tabla, _compute_card_price() usa fallback por tipo.

    ACTUALIZACIÓN:
    - Un job diario en background llama a _sync_ck_prices() una vez al día.
    - El job descarga la pricelist completa de CK y hace upsert masivo (INSERT ON CONFLICT DO UPDATE).
    - Render free tier: el job corre en el mismo proceso, sin celery ni cron externo.
    """
    __tablename__ = "ck_prices"

    name_canonical  = Column(String, primary_key=True, index=True)   # _canonical(name_raw)
    name_raw        = Column(String, nullable=False)                  # nombre tal como viene de CK
    min_buy_price   = Column(Numeric(10, 4), nullable=False)          # precio mínimo USD (buy_price NM)
    updated_at      = Column(DateTime, server_default=func.now(), onupdate=func.now())
    """
    Registro de idempotencia para el cashback del 2%.

    PROPÓSITO:
    ──────────
    Jumpseller puede re-enviar el mismo webhook varias veces (retries por timeout,
    reconexión, etc.). Sin esta tabla, una orden podría acreditar cashback 2, 3 o
    más veces. Cada fila vincula un order_id de Jumpseller a un único evento de
    cashback ejecutado — si ya existe la fila se descarta el duplicado.

    CAMPO order_id:
    ────────────────
    Es el `order.id` del payload de Jumpseller (BigInteger). Se usa como PK
    para garantizar unicidad por constraint de BD, no solo por lógica de app.
    Si el INSERT falla por violación de PK, la excepción es capturada y el
    cashback no se acredita de nuevo — comportamiento correcto.

    CAMPO order_total_clp:
    ───────────────────────
    Precio final pagado (después de todos los descuentos, incluido el cupón QP).
    Se guarda para auditoría y para poder reconciliar el cashback otorgado.
    """
    __tablename__ = "cashback_records"

    order_id         = Column(BigInteger, primary_key=True)   # JS order.id — clave de idempotencia
    email            = Column(String,     nullable=False, index=True)
    amount_qp        = Column(Numeric(12, 2), nullable=False)  # QP acreditados
    order_total_clp  = Column(Numeric(12, 2), nullable=False)  # total final de la orden
    created_at       = Column(DateTime, server_default=func.now())


# ─────────────────────────────────────────────────────────────────────────────
# MIGRACIÓN SQL REQUERIDA — BuylistOrder.updated_at (FIX M-06)
# Ejecutar UNA VEZ en la base de datos de producción (Render PostgreSQL):
#
#   ALTER TABLE buylist_orders
#     ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();
#
#   -- Rellenar filas existentes con la misma fecha de creación:
#   UPDATE buylist_orders
#     SET updated_at = created_at
#   WHERE updated_at IS NULL;
#
# Nota: ADD COLUMN IF NOT EXISTS es idempotente — seguro de re-ejecutar.
# ─────────────────────────────────────────────────────────────────────────────
#
# NUEVA TABLA — ck_prices (job diario de CardKingdom):
# SQLAlchemy la crea automáticamente vía Base.metadata.create_all() al arrancar.
# No requiere migración manual si la BD es nueva o si aún no existe la tabla.
# Si la BD ya existe sin la tabla, ejecutar:
#
#   CREATE TABLE IF NOT EXISTS ck_prices (
#       name_canonical  TEXT    PRIMARY KEY,
#       name_raw        TEXT    NOT NULL,
#       min_buy_price   NUMERIC(10,4) NOT NULL,
#       updated_at      TIMESTAMP DEFAULT NOW()
#   );
# ─────────────────────────────────────────────────────────────────────────────
