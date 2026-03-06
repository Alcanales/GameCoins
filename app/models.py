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
    js_product_ids  = Column(JSON,   default=list)   # [12345, 12346, ...]
    js_variants     = Column(JSON,   default=list)   # [{id, name, stock, price}, ...]
    # scryfall_ids: lista de UUIDs de Scryfall (una por edición/impresión)
    # Formato: [{"scryfall_id": "uuid", "set_code": "M10", "set_name": "Magic 2010",
    #            "collector_number": "152", "lang": "en"}]
    scryfall_ids    = Column(JSON,   default=list)
    total_stock     = Column(Integer, default=0)
    last_synced     = Column(DateTime, nullable=True)
    created_at      = Column(DateTime, server_default=func.now())
    updated_at      = Column(DateTime, server_default=func.now(), onupdate=func.now())
