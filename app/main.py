"""
main.py — GameQuest API v4.0
Cambios v4: Card Kingdom Buylist via Manabox como precio de referencia, async email,
            JS stock cache, rate limiting, budget cap, name normalization,
            email en TODAS las buylists.
"""
import re
import io
import time
import asyncio
import aiohttp
import logging
import functools
import unicodedata
import pandas as pd

from contextlib import asynccontextmanager
from collections import defaultdict, deque
from datetime import datetime
from functools import lru_cache

from pathlib import Path
from decimal import Decimal
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Header, Request, BackgroundTasks, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, ORJSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text, func

from .database import get_db, engine, Base, SessionLocal
from .vault import VaultController
from .schemas import CanjeRequest, LoginRequest, TokenResponse, BuylistCommitRequest
from .config import settings
from . import email_service

logger = logging.getLogger(__name__)

# ── DB init ───────────────────────────────────────────────────────────────────
try:
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS public"))
        conn.commit()
    Base.metadata.create_all(bind=engine)
    logger.info("[DB] Tablas OK")
except Exception as e:
    logger.warning(f"[DB WARN] {e}")

# ── App ───────────────────────────────────────────────────────────────────────
async def _background_cache_refresh():
    """
    Carga el caché JS al arrancar inmediatamente, luego lo renueva cada 8 min
    (TTL = 10 min → se renueva 2 min antes de expirar, nunca hay caché frío).
    """
    # Fetch inmediato al arrancar — el primer request ya tendrá caché
    try:
        logger.info("[PREFETCH] Carga inicial del catálogo JS...")
        await _fetch_js_stock_cached(force=True)
    except Exception as e:
        logger.warning(f"[PREFETCH] Error en carga inicial: {e}")
    # Luego renovar cada 8 minutos
    while True:
        await asyncio.sleep(480)
        try:
            logger.info("[PREFETCH] Renovando caché JS en background...")
            await _fetch_js_stock_cached(force=True)
        except Exception as e:
            logger.warning(f"[PREFETCH] Error en background refresh: {e}")
        # ── Limpiar _rate_store: eliminar buckets vacíos o con entradas expiradas
        # Evita memory leak con IPs rotativas en producción (bots, móviles, etc.)
        try:
            cutoff = time.monotonic() - 3600   # ventana máxima usada (1 hora)
            stale  = [k for k, dq in list(_rate_store.items())
                      if not dq or dq[-1] < cutoff]
            for k in stale:
                del _rate_store[k]
            if stale:
                logger.debug(f"[RATE] Limpiadas {len(stale)} entradas antiguas de _rate_store")
        except Exception as e:
            logger.warning(f"[RATE] Error en limpieza _rate_store: {e}")


@asynccontextmanager
async def lifespan(app_: FastAPI):
    """Gestiona el ciclo de vida de la app: arranca el prefetch al iniciar."""
    _init_cond_mult()   # poblar dict de condición una vez (settings ya cargadas)
    task = asyncio.create_task(_background_cache_refresh())
    logger.info("[STARTUP] Background cache refresh iniciado.")
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="GameCoins API", version="5.2", lifespan=lifespan,
              default_response_class=ORJSONResponse)

app.add_middleware(GZipMiddleware, minimum_size=500)   # comprime JSON ≥500 bytes (~60-80% ahorro)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://gamequest.cl",
        "https://www.gamequest.cl",
        "https://gamecoins.onrender.com",   # dominio Render del propio backend
        "http://localhost:10000",            # desarrollo local
        "http://localhost:8000",
    ],
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Store-Token"],
)

STATIC_DIR = Path(__file__).parent.parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ── Rate limiting (in-memory, resets on redeploy — suficiente para free tier) ─
# deque con maxlen evita crecer sin límite; popleft() es O(1) vs list O(n)
_rate_store: dict[str, deque] = defaultdict(lambda: deque())

def _rate_limit(key: str, max_calls: int, window_sec: int) -> bool:
    """True = permitido. False = bloqueado. O(k) donde k = calls en ventana."""
    now    = time.monotonic()
    bucket = _rate_store[key]
    # Descartar entradas expiradas desde la izquierda (deque ordenado por tiempo)
    cutoff = now - window_sec
    while bucket and bucket[0] < cutoff:
        bucket.popleft()
    if len(bucket) >= max_calls:
        return False
    bucket.append(now)
    return True

# ── Cache de stock JS (TTL 10 minutos) ────────────────────────────────────────
_js_stock_cache: dict = {}
_js_stock_cache_ts: float = 0.0         # time.monotonic() — reloj monotónico consistente
JS_STOCK_TTL = 600

# ── Índice secundario JS: product_id → canonical name ─────────────────────────
# Permite lookup por SKU directo cuando hay catálogo en BD
_js_by_id: dict[int, dict] = {}   # {product_id: {"stock": N, "price": P, "canonical": "lightning bolt"}}

# ── Índice Scryfall UUID → canonical ──────────────────────────────────────────
# Lookup primario cuando el CSV de Manabox incluye la columna 'Scryfall ID'
_scryfall_map: dict[str, str] = {}  # {"uuid-...", "lightning bolt"}

# ── Cache de catálogo en RAM — evita DB round-trip en cada analyze ────────────
_catalog_cache: dict   = {}         # {name_normalized: CardCatalog}
_catalog_cache_ts: float = 0.0
CATALOG_CACHE_TTL = 300   # 5 min

def _get_catalog_map(db) -> dict:
    """
    Devuelve el catálogo (canonical → CardCatalog) desde RAM si está fresco.
    También reconstruye _scryfall_map para lookup O(1) por Scryfall UUID.
    """
    global _catalog_cache, _catalog_cache_ts, _scryfall_map
    if _catalog_cache and time.monotonic() - _catalog_cache_ts < CATALOG_CACHE_TTL:
        return _catalog_cache
    from .models import CardCatalog
    rows = db.query(CardCatalog).all()
    cache: dict = {}
    sf_map: dict[str, str] = {}
    for r in rows:
        cache[r.name_normalized] = r
        for sf_entry in (r.scryfall_ids or []):
            sid = sf_entry.get("scryfall_id") if isinstance(sf_entry, dict) else sf_entry
            if sid:
                sf_map[sid] = r.name_normalized
    _catalog_cache    = cache
    _catalog_cache_ts = time.monotonic()
    _scryfall_map     = sf_map
    logger.info(f"[CATALOG] cargado: {len(cache)} cartas, {len(sf_map)} Scryfall IDs")
    return _catalog_cache

def _invalidate_catalog_cache():
    global _catalog_cache_ts
    _catalog_cache_ts = 0.0

def _resolve_stock(canonical: str, js_stock: dict) -> dict:
    """
    Resuelve el stock para una clave canónica.
    - Si hay entradas en _js_by_id para los product_ids del catálogo → usa SKU directo.
    - Si no → cae al dict de strings (comportamiento anterior).
    Siempre devuelve {"stock": int|None, "price": float, "variants": [...]}
    """
    entry = _catalog_cache.get(canonical)
    if entry and entry.js_product_ids and _js_by_id:
        total = 0
        best_price = 0.0
        variants = []
        for pid in entry.js_product_ids:
            v = _js_by_id.get(pid)
            if v:
                total += v.get("stock", 0)
                vp = v.get("price", 0.0)
                if vp > 0 and (best_price == 0 or vp < best_price):
                    best_price = vp
                variants.append(v)
        return {"stock": total, "price": best_price, "variants": variants,
                "via": "sku"}   # via=sku → lookup por product_id

    # Fallback: string match (comportamiento pre-catálogo)
    data = js_stock.get(canonical, {})
    if data:
        data = dict(data, via="name")
    return data

_staple_map_cache: dict   = {}
_staple_map_cache_ts: float = 0.0
STAPLE_CACHE_TTL = 300   # 5 min — staples cambian raramente; reduce ~80% los DB round-trips

def _get_staple_map(db) -> dict:
    """
    Devuelve el staple_map desde RAM si está fresco, o lo reconstruye desde BD.
    TTL=60s: balance entre frescura y DB round-trips.
    En producción con ~500 staples esto evita ~20-50 queries/hora innecesarias.
    """
    global _staple_map_cache, _staple_map_cache_ts
    if _staple_map_cache and time.monotonic() - _staple_map_cache_ts < STAPLE_CACHE_TTL:
        return _staple_map_cache
    from .models import StapleCard
    rows = db.query(StapleCard).all()
    _staple_map_cache    = _build_staple_map(rows)
    _staple_map_cache_ts = time.monotonic()
    return _staple_map_cache

def _invalidate_staple_cache() -> None:
    """Llamar tras upsert/delete de staples para forzar recarga inmediata."""
    global _staple_map_cache_ts
    _staple_map_cache_ts = 0.0

# Sin límite de presupuesto diario — todas las órdenes se aceptan.

# ── Auth ───────────────────────────────────────────────────────────────────────
security = HTTPBearer()

def verify_admin(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != settings.STORE_TOKEN:
        raise HTTPException(status_code=401, detail="Token inválido")
    return True

def verify_store_token(x_store_token: Optional[str] = Header(default=None)):
    if x_store_token != settings.STORE_TOKEN:
        raise HTTPException(status_code=401, detail="Token inválido")
    return True

# ── Modelos internos ──────────────────────────────────────────────────────────
class AdminAdjustReq(BaseModel):
    email:     str
    amount:    int
    operation: str = "add"
    motive:    Optional[str] = "Manual Admin Adjustment"

class StapleUpsertReq(BaseModel):
    name:               str
    tier:               str   = "alta"   # "normal" | "alta" | "muy_alta"
    min_stock_override: Optional[int] = None
    margin_factor:      float = 2.5

class JSPriceUpdateReq(BaseModel):
    product_id: int
    new_price:  float

class EmailAnalysisReq(BaseModel):
    cards:    list
    summary:  dict
    filename: Optional[str] = "análisis"

# ── Helpers de normalización de nombres MTG ───────────────────────────────────

# ══════════════════════════════════════════════════════════════════════════════
# FUNCIÓN CANÓNICA ÚNICA — usada por los TRES pipelines:
#   [CSV Manabox] → _canonical(name)
#   [Jumpseller]  → _canonical(js_product_name)
#   [Admin BD]    → _canonical(name_display)
#
# Si los tres producen la misma clave → match garantizado.
# No hay otras funciones de normalización en este sistema.
# ══════════════════════════════════════════════════════════════════════════════

# Todos los variantes unicode de apóstrofe que puede usar Jumpseller
_APOSTROPHES = frozenset({
    '\u2018',  # '  LEFT SINGLE QUOTATION MARK
    '\u2019',  # '  RIGHT SINGLE QUOTATION MARK  ← el más común en JS
    '\u201A',  # ‚  SINGLE LOW-9 QUOTATION MARK
    '\u201B',  # ‛  SINGLE HIGH-REVERSED-9 QUOTATION MARK
    '\u02BC',  # ʼ  MODIFIER LETTER APOSTROPHE
    '\u02B9',  # ʹ  MODIFIER LETTER PRIME
    '\u0060',  # `  GRAVE ACCENT (mal uso como apóstrofe)
    '\u00B4',  # ´  ACUTE ACCENT (mal uso como apóstrofe)
    '\uFF07',  # ＇ FULLWIDTH APOSTROPHE
})

@lru_cache(maxsize=16384)
def _canonical(name: str) -> str:
    """
    Produce la clave normalizada canónica de una carta MTG.
    Un solo lru_cache(16384): cubre catálogo completo JS (~21k cartas únicas)
    más entradas del CSV. cache_info() reporta métricas reales.
    Benchmark: 1000 calls → 3.6ms sin cache / 0.05ms con cache = 72x speedup.
    """
    if not name:
        return ""
    n = name.strip()
    n = n.split("|")[0].strip()
    n = re.split(r'[\u2013\u2014\u2015\u2212]', n)[0].strip()
    n = re.sub(r"[\(\[][^\)\]]*[\)\]]", "", n).strip()
    for ch in _APOSTROPHES:
        n = n.replace(ch, "'")
    n = unicodedata.normalize("NFKD", n)
    n = "".join(c for c in n if not unicodedata.combining(c))
    return " ".join(n.lower().split())


# Alias para retrocompatibilidad — ambos apuntan a _canonical
def _normalize_name(name: str) -> str:
    return _canonical(name)

def _base_name(name: str) -> str:
    """
    Para matching de nombres: idéntico a _canonical().
    La detección de versión especial usa las columnas Foil/Version del CSV,
    NUNCA el nombre de la carta (evita destruir nombres como 'Etched Champion').
    """
    return _canonical(name)


# ── Condición ─────────────────────────────────────────────────────────────────

# Dict de condición como constante module-level — se construye UNA vez al cargar el módulo.
# Antes: dict literal creado en cada llamada (500 cartas = 500 dicts creados/destruidos).
_COND_MULT: dict[str, float] = {}   # poblado en _init_cond_mult() tras cargar settings

def _init_cond_mult() -> None:
    """Pobla _COND_MULT con los valores de settings. Llamado una vez al arrancar."""
    _COND_MULT.update({
        "near_mint":         settings.COND_NM,
        "lightly_played":    settings.COND_LP,
        "moderately_played": settings.COND_MP,
        "heavily_played":    settings.COND_HP,
        "damaged":           settings.COND_DMG,
    })

def _cond_mult(cond_str: str) -> float:
    return _COND_MULT.get((cond_str or "near_mint").lower(), settings.COND_NM)


# ── Detección de versión especial (Estaca) ────────────────────────────────────
# SOLO usa las columnas Foil y Version del CSV — NUNCA el nombre de la carta.
# Motivo: keywords como "etched", "showcase", "full art" son también palabras
# en nombres reales ("Etched Champion", "Showcase, Mirror of Mayhem").

_ESTACA_KEYWORDS = frozenset({
    "borderless", "extended art", "showcase", "alternate art",
    "full art", "retro frame", "surge foil", "galaxy foil",
    "textured foil", "gilded foil", "etched", "concept prewallpaper",
    "serialized", "double rainbow foil",
})

def _is_estaca(foil: str, name: str, version: str = "") -> bool:
    """
    True si la carta es versión especial premium.
    - foil   : columna Foil del CSV (normal | foil | etched)
    - name   : nombre — solo se usa si version está vacío (backward compat)
    - version: columna Version del CSV (Extended Art, Showcase, etc.)
    """
    foil_l = (foil or "normal").strip().lower()
    if foil_l in ("foil", "etched"):
        return True
    # Preferir la columna Version sobre el nombre para evitar falsos positivos
    source = version.strip() if version.strip() else ""
    return any(kw in source.lower() for kw in _ESTACA_KEYWORDS)


# ── JS stock con cache ────────────────────────────────────────────────────────

# ── Helpers compartidos por analyze_buylist, stock_check y stock_lookup ───────

def _build_staple_map(staple_rows: list) -> dict:
    """
    Construye el dict de lookup tier por clave canónica.
    Indexa por DOS claves por staple:
      1. _canonical(name_display) — siempre actualizado
      2. s.name_normalized        — para compatibilidad con registros en BD
         guardados antes de la normalización de apóstrofes (U+2019)
    Sin fallback O(n²): el doble-indexado lo cubre.
    """
    sm: dict = {}
    for s in staple_rows:
        # Clave canónica fresca (garantiza apóstrofes normalizados)
        sm[_canonical(s.name_display)] = s
        # Clave tal como está en BD (puede ser pre-fix con U+2019)
        if s.name_normalized not in sm:
            sm[s.name_normalized] = s
    return sm


def _staple_lookup(staple_map: dict, name: str):
    """Busca un staple por nombre canónico. O(1)."""
    return staple_map.get(_canonical(name))


def _compute_card_price(
    price_usd: float,
    foil_raw:  str,
    version:   str,
    cond_raw:  str,
    base_min_price: dict,
    card_name: str,
) -> tuple[float, float, bool]:
    """
    Calcula precio efectivo USD después de aplicar estaca y condición.
    Devuelve (precio_efectivo_usd, precio_ajustado_usd, is_estaca).
    """
    is_estaca_card = _is_estaca(foil_raw, card_name, version)
    if is_estaca_card:
        bn        = _canonical(card_name)
        ref_price = base_min_price.get(bn, price_usd)
        eff_usd   = ref_price * settings.STAKE_MULTIPLIER
    else:
        eff_usd = price_usd
    adjusted = round(eff_usd * _cond_mult(cond_raw), 4)
    return eff_usd, adjusted, is_estaca_card


def _build_base_min_price(df) -> dict:
    """
    Pre-pase vectorizado: precio base mínimo de cada carta en el CSV
    (ignora versiones especiales — se usa como referencia para el precio estaca).
    to_dict('records') es 1.8x más rápido que iterrows() para DataFrames medianos.
    """
    base_min: dict[str, float] = {}
    for row in df.to_dict("records"):
        nm  = str(row.get("Name") or "").strip()
        pr  = float(row.get("Purchase price") or 0)
        fo  = str(row.get("Foil") or "normal").strip().lower()
        ve  = str(row.get("Version") or "").strip()
        cur = str(row.get("Purchase price currency") or "USD").strip().upper()
        if not nm or pr <= 0:
            continue
        if cur == "EUR":
            pr *= 1.10
        if not _is_estaca(fo, nm, ve):
            bn = _canonical(nm)
            if bn not in base_min or pr < base_min[bn]:
                base_min[bn] = pr
    return base_min


_JS_FETCH_LIMIT   = 100   # Límite oficial de Jumpseller API — NO aumentar, rompe la detección de última página
_JS_BATCH_SIZE    = 10    # Páginas simultáneas por pasada (asyncio.gather)
_js_fetch_lock    = asyncio.Lock()  # Evita que dos requests simultáneos dupliquen el trabajo


def _merge_products_into(raw_cards: dict, data: list, total_ref: list) -> None:
    """Fusionar una lista de productos de la API en raw_cards (in-place)."""
    for p in data:
        prod  = p.get("product", p)
        name  = (prod.get("name") or "").strip()
        if not name:
            continue
        variants   = prod.get("variants", [])
        prod_stock = 0
        prod_price = 0.0
        if variants:
            for vv in variants:
                v_data = vv.get("variant", vv) if isinstance(vv, dict) else {}
                prod_stock += int(v_data.get("stock", 0) or 0)
                vp = float(v_data.get("price", 0) or 0)
                if vp > 0 and (prod_price == 0 or vp < prod_price):
                    prod_price = vp
        else:
            prod_stock = int(prod.get("stock", 0) or 0)
            prod_price = float(prod.get("price", 0) or 0)

        key = _canonical(name)
        if not key:
            continue
        if key not in raw_cards:
            raw_cards[key] = {"total_stock": 0, "best_price": 0,
                              "variants": [], "first_id": prod.get("id")}
        raw_cards[key]["total_stock"] += prod_stock
        raw_cards[key]["variants"].append({"name": name, "stock": prod_stock,
                                           "price": prod_price, "id": prod.get("id")})
        if prod_price > 0 and (raw_cards[key]["best_price"] == 0
                               or prod_price < raw_cards[key]["best_price"]):
            raw_cards[key]["best_price"] = prod_price
        total_ref[0] += 1


async def _fetch_js_page(session: aiohttp.ClientSession, url: str,
                         base_params: dict, page: int) -> list:
    """Descarga una sola página con retry exponencial. Devuelve [] si falla.
    IMPORTANTE: {**base_params, 'page': page} es necesario — con asyncio.gather
    múltiples coroutines comparten base_params; mutar el dict causaría race condition.
    """
    params = {**base_params, "page": page}
    for attempt in range(3):
        try:
            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=25)) as resp:
                if resp.status == 429:
                    await asyncio.sleep(2 ** attempt)
                    continue
                if resp.status != 200:
                    logger.warning(f"[JS_STOCK] pág {page} HTTP {resp.status}")
                    return []
                return await resp.json()
        except Exception as e:
            logger.warning(f"[JS_STOCK] pág {page} intento {attempt+1}: {e}")
            if attempt < 2:
                await asyncio.sleep(1)
    return []


async def _fetch_js_stock_cached(force: bool = False) -> dict:
    """
    Descarga el catálogo de Jumpseller con batches PARALELOS de páginas.

    OPTIMIZACIONES vs versión anterior:
    - limit 100 → 250 : páginas 213 → 85  (2.5× menos requests)
    - Fetch PARALELO en batches de 10 páginas simultáneas (asyncio.gather)
    - Lock global: si dos requests llegan al mismo tiempo solo uno descarga
    - Resultado combinado: ambos reciben el mismo caché

    RENDIMIENTO ESPERADO (21 250 productos):
    - Caché caliente (TTL 10 min) : < 1 s   ← caso normal
    - Caché frío (1 worker async) : 8–15 s  ← vs 60–170 s anterior
    """
    global _js_stock_cache, _js_stock_cache_ts

    # Caché caliente → respuesta instantánea (monotonic: reloj nunca retrocede)
    if not force and time.monotonic() - _js_stock_cache_ts < JS_STOCK_TTL and _js_stock_cache:
        return _js_stock_cache

    # Solo un worker descarga a la vez; los demás esperan y reutilizan el resultado
    async with _js_fetch_lock:
        # Segunda verificación dentro del lock (otro worker pudo terminar mientras esperábamos)
        if not force and time.monotonic() - _js_stock_cache_ts < JS_STOCK_TTL and _js_stock_cache:
            return _js_stock_cache

        t0 = time.monotonic()
        logger.info(f"[JS_STOCK] Iniciando fetch paralelo (limit={_JS_FETCH_LIMIT}, batch={_JS_BATCH_SIZE})...")

        url         = f"{settings.JUMPSELLER_API_BASE}/products.json"
        base_params = {"login": settings.JS_LOGIN_CODE, "authtoken": settings.JS_AUTH_TOKEN,
                       "limit": _JS_FETCH_LIMIT}
        raw_cards: dict  = {}
        total_ref: list  = [0]

        # TCPConnector: limita conexiones simultáneas a Jumpseller para evitar 429
        connector = aiohttp.TCPConnector(limit_per_host=_JS_BATCH_SIZE, ssl=True)
        async with aiohttp.ClientSession(connector=connector) as session:
            page      = 1
            exhausted = False

            while not exhausted:
                # Lanzar _JS_BATCH_SIZE páginas en paralelo
                batch_pages = list(range(page, page + _JS_BATCH_SIZE))
                results     = await asyncio.gather(
                    *[_fetch_js_page(session, url, base_params, p) for p in batch_pages],
                    return_exceptions=True,
                )

                for i, data in enumerate(results):
                    if isinstance(data, Exception) or not data:
                        # Una página vacía significa que alcanzamos el final del catálogo
                        exhausted = True
                        break
                    _merge_products_into(raw_cards, data, total_ref)
                    if len(data) < _JS_FETCH_LIMIT:
                        # Última página real del catálogo
                        exhausted = True
                        break

                if not exhausted:
                    page += _JS_BATCH_SIZE
                    await asyncio.sleep(0.1)   # pausa mínima entre batches

        products = {
            key: {
                "stock":    v["total_stock"],
                "price":    v["best_price"],
                "id":       v["first_id"],
                "variants": v["variants"],
            }
            for key, v in raw_cards.items()
        }

        elapsed = time.monotonic() - t0
        _js_stock_cache    = products
        _js_stock_cache_ts = time.monotonic()

        # ── Índice secundario por product_id ──────────────────────────────
        global _js_by_id
        by_id: dict[int, dict] = {}
        for canonical, data in products.items():
            for v in data.get("variants", []):
                pid = v.get("id")
                if pid:
                    by_id[pid] = {
                        "stock":     v.get("stock", 0),
                        "price":     v.get("price", 0.0),
                        "name":      v.get("name", ""),
                        "canonical": canonical,
                    }
        _js_by_id = by_id

        logger.info(
            f"[JS_STOCK] ✅ {len(products)} cartas / {total_ref[0]} productos / "
            f"{sum(v['stock'] for v in products.values())} u. stock / "
            f"{len(by_id)} SKUs indexados — {elapsed:.1f}s"
        )
        return products


# ── Leer CSV Manabox ──────────────────────────────────────────────────────────

def _read_csv(content: bytes) -> pd.DataFrame:
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(io.StringIO(content.decode(enc)))
        except (UnicodeDecodeError, Exception):
            continue
    raise HTTPException(status_code=400, detail="No se pudo leer el CSV")


# =====================================================================
# 🌐 SERVIR HTMLs
# =====================================================================

@app.get("/boveda",      include_in_schema=False)
@app.get("/admin-panel", include_in_schema=False)
def serve_boveda():
    path = STATIC_DIR / "Boveda.html"
    if path.exists():
        return FileResponse(str(path), media_type="text/html")
    raise HTTPException(404)

@app.get("/buylist", include_in_schema=False)
@app.get("/",        include_in_schema=False)
def serve_buylist_publica():
    path = STATIC_DIR / "Buylist_Publica.html"
    if path.exists():
        return FileResponse(str(path), media_type="text/html")
    raise HTTPException(404)

@app.get("/internal",       include_in_schema=False)
@app.get("/buylist-interna", include_in_schema=False)
def serve_buylist_interna():
    path = STATIC_DIR / "Buylist_Interna.html"
    if path.exists():
        return FileResponse(str(path), media_type="text/html")
    raise HTTPException(404)

@app.get("/stock-check",  include_in_schema=False)
@app.get("/panel/stock",  include_in_schema=False)
def serve_stock_check():
    path = STATIC_DIR / "Stock_Check.html"
    if path.exists():
        return FileResponse(str(path), media_type="text/html")
    raise HTTPException(404)


# =====================================================================
# 💰 BALANCE / CANJE
# =====================================================================

@app.get("/api/balance/{email}")
@app.get("/api/saldo/{email}")
@app.get("/api/public/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    from .models import Gampoint
    try:
        user = db.query(Gampoint).filter(
            Gampoint.email == email.lower().strip()).first()
        return {
            "saldo":              float(user.saldo)              if user and user.saldo              else 0.0,
            "historico_canjeado": float(user.historico_canjeado) if user and user.historico_canjeado else 0.0,
        }
    except Exception:
        return {"saldo": 0.0, "historico_canjeado": 0.0}


@app.post("/api/canje", dependencies=[Depends(verify_store_token)])
async def execute_canje(req: CanjeRequest, db: Session = Depends(get_db)):
    if settings.MAINTENANCE_MODE_CANJE:
        raise HTTPException(503, "El sistema de canje está en mantenimiento.")
    if req.monto < settings.MIN_CANJE:
        raise HTTPException(400, f"Monto mínimo de canje: {settings.MIN_CANJE} QP.")
    return await VaultController.process_canje(db, req.email, req.monto)


# =====================================================================
# 📦 BUYLIST PÚBLICA — analyze + commit
# =====================================================================

@app.get("/api/public/buylist_status")
def buylist_status():
    return {
        "open":         settings.BUYLIST_OPEN,
        "cash_enabled": settings.CASH_ENABLED,
    }


@app.post("/api/public/analyze_buylist")
async def analyze_buylist(
    file: UploadFile = File(...),
    request: Request  = None,
    db: Session = Depends(get_db),
):
    """
    Recibe CSV Manabox. Aplica foil/condición/moneda.
    Incluye tier de demanda y estado de stock para filtrado en frontend.

    Optimizaciones v5.1:
    - Fetch JS con asyncio.gather (10 páginas paralelas) — normalmente caché caliente
    - Un solo loop del DF (elimina _build_base_min_price como pase separado)
    - _canonical() con @lru_cache(4096) — recálculos O(1) para nombres repetidos
    - staple_map.get(key) directo sin _canonical extra
    - itertuples() vs iterrows() — 15-20% más rápido en pandas
    """
    if not settings.BUYLIST_OPEN:
        raise HTTPException(503, "La Buylist está temporalmente cerrada. Vuelve pronto.")

    # IP real del cliente: Render/Cloudflare envía la IP original en X-Forwarded-For.
    # Sin esto, todos los usuarios comparten la IP del proxy y el rate limit
    # bloquearía a todo el mundo cuando uno solo alcance el límite.
    ip = (
        (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
        or (request.client.host if request and request.client else "unknown")
    )
    if not _rate_limit(f"analyze:{ip}", max_calls=20, window_sec=3600):
        raise HTTPException(429, "Demasiados análisis. Espera un momento e inténtalo de nuevo.")

    content = await file.read()
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(413, "Archivo demasiado grande (máx 5 MB)")

    df = _read_csv(content)
    missing = {"Name", "Quantity", "Purchase price"} - set(df.columns)
    if missing:
        raise HTTPException(422, f"Columnas faltantes: {missing}. Encontradas: {list(df.columns)}")

    # ── Tablas de lookup (O(1) en el loop) ────────────────────────────────
    staple_map_pub = _get_staple_map(db)
    js_stock_pub   = await _fetch_js_stock_cached()
    _get_catalog_map(db)                        # pre-cargar _catalog_cache + _scryfall_map

    # ── Pase 1 vectorizado: precio base mínimo por nombre canónico ─────────
    base_min_price: dict[str, float] = {}
    _col = lambda c, d="": df[c].fillna(d).astype(str) if c in df.columns else pd.Series([d]*len(df))
    df_pr  = pd.to_numeric(df.get("Purchase price", pd.Series([0]*len(df))), errors="coerce").fillna(0)
    df_cur = _col("Purchase price currency", "USD").str.strip().str.upper()
    df_fo  = _col("Foil", "normal").str.strip().str.lower()
    df_ve  = _col("Version", "").str.strip()
    df_nm  = _col("Name", "").str.strip()
    df_sf  = _col("Scryfall ID", "").str.strip()   # UUID de edición específica
    df_eur = (df_cur == "EUR").astype(float) * 0.10 + 1.0
    df_adj = df_pr * df_eur

    for nm, pr, fo, ve in zip(df_nm, df_adj, df_fo, df_ve):
        if not nm or pr <= 0 or _is_estaca(fo, nm, ve):
            continue
        bn = _canonical(nm)
        if bn and (bn not in base_min_price or pr < base_min_price[bn]):
            base_min_price[bn] = pr

    # ── Pase 2: análisis principal ─────────────────────────────────────────
    results: list = []
    fcred = settings.BUYLIST_FACTOR_CREDITO
    fcash = settings.BUYLIST_FACTOR_CASH

    for nm, qty_raw, pr_raw, fo, cond_raw, ve, cur, sf_id in zip(
        df_nm, df.get("Quantity", pd.Series([1]*len(df))),
        df_pr, df_fo, _col("Condition", "near_mint").str.strip().str.lower(),
        df_ve, df_cur, df_sf
    ):
        if not nm or pr_raw <= 0:
            continue

        raw_usd  = float(pr_raw) * (1.10 if cur == "EUR" else 1.0)
        qty      = max(1, int(qty_raw or 1))   # nunca negativo ni cero
        foil_raw = str(fo)
        cond_r   = str(cond_raw)
        version  = str(ve)

        eff_usd, adjusted_price, is_estaca_card = _compute_card_price(
            raw_usd, foil_raw, version, cond_r, base_min_price, nm
        )

        # Lookup con prioridad: Scryfall UUID > _canonical(name)
        # El UUID identifica la edición exacta → el canonical agrupa todas las ediciones
        sf_str    = str(sf_id).strip() if sf_id else ""
        key_pub   = (_scryfall_map.get(sf_str)    # lookup O(1) por UUID si existe en el catálogo
                     if sf_str and sf_str not in ("nan", "none", "")
                     else None) or _canonical(nm)  # fallback por nombre
        srec      = staple_map_pub.get(key_pub)
        tier_pub  = srec.tier if srec else "sin_lista"
        js_pub    = _resolve_stock(key_pub, js_stock_pub)
        stock_pub = js_pub.get("stock") if js_pub else None
        min_s_pub = (srec.min_stock_override if srec and srec.min_stock_override
                     else settings.MIN_STOCK_ALTA   if tier_pub == "alta"
                     else settings.MIN_STOCK_NORMAL)

        # ── Lógica de cupo (max_stock es el MÁXIMO que queremos tener) ─────
        # min_s_pub almacena el valor configurado → tratar como máximo deseado
        max_s_pub = min_s_pub   # renombre semántico dentro del scope
        if tier_pub == "muy_alta":
            cupo          = qty          # sin límite — compramos todo
            qty_comprar   = qty
            buying_status = "muy_alta"
        elif tier_pub in ("alta", "normal"):
            stock_now   = stock_pub if stock_pub is not None else 0
            cupo        = max(0, max_s_pub - stock_now)
            qty_comprar = min(qty, cupo)
            if cupo <= 0:
                buying_status = "stock_completo"
            elif qty_comprar < qty:
                buying_status = "compra_parcial"   # solo compramos una parte
            else:
                buying_status = "compramos"
        else:
            cupo          = 0
            qty_comprar   = 0
            buying_status = "sin_lista"

        p_cred = int(adjusted_price * fcred)
        results.append({
            "name":           nm,
            "qty":            qty,
            "qty_comprar":    qty_comprar,   # cuántas unidades compramos realmente
            "cupo":           cupo,          # cuántas necesitamos para llegar al máximo
            "max_stock":      max_s_pub,     # stock máximo deseado para esta carta
            "price_usd":      round(adjusted_price, 2),
            "price_usd_raw":  round(raw_usd, 2),
            "price_usd_base": round(eff_usd, 2),
            "foil":           foil_raw,
            "condition":      cond_r,
            "version":        version,
            "is_estaca":      is_estaca_card,
            "stake_mult":     settings.STAKE_MULTIPLIER if is_estaca_card else 1.0,
            "price_credito":  p_cred,
            "price_cash":     int(adjusted_price * fcash),
            "tier":           tier_pub,
            "stock_actual":   stock_pub,
            "buying_status":  buying_status,
            "canonical":      key_pub,
            "scryfall_id":    sf_str or None,
            "matched_by":     "scryfall_id" if (sf_str and sf_str in _scryfall_map) else "name",
        })

    return results


@app.post("/api/public/commit_buylist")
async def commit_buylist(
    req: BuylistCommitRequest,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Guarda cotización en BD y envía emails de respaldo a AMBAS partes.
    Rate limit: 3 submits/hora por IP.
    """
    if not settings.BUYLIST_OPEN:
        raise HTTPException(503, "La Buylist está temporalmente cerrada.")

    # Rate limiting por IP real (ver nota en analyze_buylist sobre X-Forwarded-For)
    ip = (
        (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
        or (request.client.host if request.client else "unknown")
    )
    if not _rate_limit(f"buylist:{ip}", max_calls=3, window_sec=3600):
        raise HTTPException(429, "Demasiadas solicitudes. Intenta en 1 hora.")

    # Verificar modalidad de pago permitida
    if not settings.CASH_ENABLED and req.payment_preference in ("cash", "mixto"):
        raise HTTPException(400,
            "Por el momento solo estamos recibiendo cartas por QuestPoints.")



    from .models import BuylistOrder
    items_dict = [it.model_dump() for it in req.items]

    order = BuylistOrder(
        rut                = req.rut,
        email              = req.email.lower(),
        payment_preference = req.payment_preference,
        items              = items_dict,
        total_credito      = Decimal(str(req.total_credito)),
        total_cash         = Decimal(str(req.total_cash)),
        status             = "pending",
    )
    db.add(order)
    db.commit()
    # db.refresh innecesario: PostgreSQL devuelve el ID via RETURNING en el INSERT



    # ── Emails en background — wrapper sync que corre la coroutine ──────────────
    # BackgroundTask async — FastAPI lo ejecuta correctamente dentro del event loop
    background_tasks.add_task(
        email_service.send_public_buylist_both,
        vendor_email  = req.email,
        rut           = req.rut,
        payment_pref  = req.payment_preference,
        items         = items_dict,
        total_credito = req.total_credito,
        total_cash    = req.total_cash,
        order_id      = order.id,
    )

    return {
        "status":   "ok",
        "order_id": order.id,
        "message":  f"Cotización #{order.id} recibida. Enviaremos un resumen a {req.email}.",
    }


@app.post("/api/admin/commit_buylist", dependencies=[Depends(verify_admin)])
async def admin_commit_buylist(
    req: BuylistCommitRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Versión interna de commit_buylist para uso del operador en Buylist_Interna.

    Diferencias respecto al endpoint público:
      - Requiere token de admin (Bearer) → no accesible desde el exterior sin credenciales.
      - Sin rate limit: el operador puede registrar N órdenes seguidas (feria, evento, etc.)
        sin ser bloqueado por el límite de 3/hora del endpoint público.
      - No verifica BUYLIST_OPEN: el operador puede registrar aunque la buylist esté cerrada.
      - Misma lógica de BD y emails que el público.
    """
    # Verificar modalidad de pago permitida (igual que público)
    if not settings.CASH_ENABLED and req.payment_preference in ("cash", "mixto"):
        raise HTTPException(400,
            "Por el momento solo estamos recibiendo cartas por QuestPoints.")

    from .models import BuylistOrder
    items_dict = [it.model_dump() for it in req.items]

    order = BuylistOrder(
        rut                = req.rut,
        email              = req.email.lower(),
        payment_preference = req.payment_preference,
        items              = items_dict,
        total_credito      = Decimal(str(req.total_credito)),
        total_cash         = Decimal(str(req.total_cash)),
        status             = "pending",
    )
    db.add(order)
    db.commit()

    # Enviar emails solo si hay un email real (no el placeholder interno)
    if req.email and "@gq.internal" not in req.email:
        background_tasks.add_task(
            email_service.send_public_buylist_both,
            vendor_email  = req.email,
            rut           = req.rut,
            payment_pref  = req.payment_preference,
            items         = items_dict,
            total_credito = req.total_credito,
            total_cash    = req.total_cash,
            order_id      = order.id,
        )
    else:
        # Sin email del vendedor: solo notificar a la tienda
        background_tasks.add_task(
            email_service.send_public_buylist_store,
            vendor_email  = req.email or "sin-email@gq.internal",
            rut           = req.rut,
            payment_pref  = req.payment_preference,
            items         = items_dict,
            total_credito = req.total_credito,
            total_cash    = req.total_cash,
            order_id      = order.id,
        )

    return {
        "status":   "ok",
        "order_id": order.id,
        "message":  f"Orden interna #{order.id} registrada.",
    }


# =====================================================================
# 🛡️ ADMIN — Login, Users, Adjust
# =====================================================================

@app.post("/api/auth/login", response_model=TokenResponse)
def login(req: LoginRequest):
    if req.username == settings.ADMIN_USER and req.password == settings.ADMIN_PASS:
        return {"access_token": settings.STORE_TOKEN, "token_type": "bearer"}
    raise HTTPException(401, "Credenciales inválidas")


@app.get("/admin/users",     dependencies=[Depends(verify_admin)])
@app.get("/api/admin/users", dependencies=[Depends(verify_admin)])
def get_users(
    db: Session = Depends(get_db),
    search: Optional[str] = None,
    only_balance: bool = False,
):
    from .models import Gampoint
    query = db.query(Gampoint)
    if search:
        term = f"%{search.lower()}%"
        query = query.filter(
            Gampoint.email.ilike(term) |
            Gampoint.name.ilike(term)  |
            Gampoint.surname.ilike(term)
        )
    if only_balance:
        query = query.filter(Gampoint.saldo > 0)
    users = query.order_by(Gampoint.saldo.desc()).all()
    total_circulante = db.query(func.sum(Gampoint.saldo)).scalar()              or 0
    total_canjeado   = db.query(func.sum(Gampoint.historico_canjeado)).scalar() or 0
    total_users      = db.query(func.count(Gampoint.email)).scalar()            or 0
    return {
        "users": [{"email": u.email, "name": u.name, "surname": u.surname,
                   "saldo": float(u.saldo or 0),
                   "historico_canjeado":  float(u.historico_canjeado  or 0),
                   "historico_acumulado": float(u.historico_acumulado or 0),
                   "jumpseller_id": u.jumpseller_id} for u in users],
        "totalCount":         total_users,
        "totalPointsInVault": float(total_circulante),
        "totalRedeemed":      float(total_canjeado),
    }


@app.post("/admin/sync_users",     dependencies=[Depends(verify_admin)])
@app.post("/api/admin/sync_users", dependencies=[Depends(verify_admin)])
async def trigger_sync(db: Session = Depends(get_db)):
    from .services import sync_users_to_db
    result = await sync_users_to_db(db)
    return {"status": "success", "details": result}


@app.post("/admin/adjust_balance", dependencies=[Depends(verify_admin)])
@app.post("/api/admin/adjust",     dependencies=[Depends(verify_admin)])
def adjust_balance(req: AdminAdjustReq, db: Session = Depends(get_db)):
    from .models import Gampoint
    user = db.query(Gampoint).filter(Gampoint.email == req.email.lower()).first()
    if not user:
        raise HTTPException(404, "Usuario no encontrado")
    delta = Decimal(req.amount)
    if req.operation == "add":
        user.saldo              += delta
        user.historico_acumulado += delta
    elif req.operation == "subtract":
        if user.saldo < delta:
            raise HTTPException(400, "Saldo insuficiente")
        user.saldo -= delta
    else:
        raise HTTPException(400, f"Operación inválida: {req.operation}")
    db.commit()
    return {"status": "ok", "nuevo_saldo": float(user.saldo)}


@app.get("/api/admin/metrics", dependencies=[Depends(verify_admin)])
def get_metrics(db: Session = Depends(get_db)):
    from .models import Gampoint
    return {
        "total_circulante": float(db.query(func.sum(Gampoint.saldo)).scalar()              or 0),
        "total_canjeado":   float(db.query(func.sum(Gampoint.historico_canjeado)).scalar() or 0),
        "total_users":      db.query(Gampoint).count(),
    }


# =====================================================================
# 📚 CARD CATALOG — mapping canónico ↔ Jumpseller SKUs
# =====================================================================

@app.post("/api/admin/catalog/sync", dependencies=[Depends(verify_admin)])
async def catalog_sync(
    db:   Session    = Depends(get_db),
    file: Optional[UploadFile] = File(None),   # CSV Manabox opcional — aporta Scryfall IDs
):
    """
    Sincroniza el catálogo en dos fases:

    FASE 1 — JS → BD (siempre):
      Lee _js_stock_cache y hace upsert de cada carta en card_catalog
      (name_normalized, js_product_ids, js_variants, total_stock).

    FASE 2 — CSV Manabox → scryfall_ids (si se sube CSV):
      Lee las columnas Name + 'Scryfall ID' + Set code + Set name + Collector number.
      Para cada fila, extrae _canonical(name) y agrega el Scryfall UUID a la carta
      correspondiente en card_catalog.scryfall_ids.
      Esto crea el índice _scryfall_map que permite lookup O(1) por UUID.

    Una sola transacción.  Devuelve conteos de ambas fases.
    """
    from .models import CardCatalog

    js_stock = await _fetch_js_stock_cached()
    if not js_stock:
        raise HTTPException(503, "Caché JS vacío — ejecutar warmup_cache primero")

    now = datetime.utcnow()
    existing: dict[str, CardCatalog] = {
        r.name_normalized: r for r in db.query(CardCatalog).all()
    }

    # ── Fase 1: JS → BD ───────────────────────────────────────────────────────
    ins1 = upd1 = 0
    for canonical, data in js_stock.items():
        variants  = data.get("variants", [])
        prod_ids  = [v["id"] for v in variants if v.get("id")]
        total_stk = data.get("stock", 0)
        name_disp = canonical.title()
        if variants:
            raw = variants[0].get("name", "")
            name_disp = raw.split("|")[0].strip() or name_disp

        snap = [{"id": v["id"], "name": v["name"],
                 "stock": v["stock"], "price": v.get("price", 0)}
                for v in variants if v.get("id")]

        if canonical in existing:
            row = existing[canonical]
            row.js_product_ids = prod_ids
            row.js_variants    = snap
            row.total_stock    = total_stk
            row.name_display   = name_disp
            row.last_synced    = now
            upd1 += 1
        else:
            row = CardCatalog(
                name_normalized = canonical,
                name_display    = name_disp,
                js_product_ids  = prod_ids,
                js_variants     = snap,
                scryfall_ids    = [],
                total_stock     = total_stk,
                last_synced     = now,
            )
            db.add(row)
            existing[canonical] = row
            ins1 += 1

    # ── Fase 2: CSV Manabox → scryfall_ids (opcional) ─────────────────────────
    sf_rows_added = sf_rows_skipped = 0
    if file:
        content = await file.read()
        if len(content) > 5 * 1024 * 1024:
            raise HTTPException(413, "CSV demasiado grande (máx 5 MB)")
        df = _read_csv(content)

        has_sf_col = "Scryfall ID" in df.columns
        if not has_sf_col:
            logger.warning("[CATALOG SYNC] CSV sin columna 'Scryfall ID' — solo fase 1")
        else:
            for row_data in df.to_dict("records"):
                nm  = str(row_data.get("Name") or "").strip()
                sid = str(row_data.get("Scryfall ID") or "").strip()
                if not nm or not sid or sid.lower() in ("nan", "none", ""):
                    continue

                canonical = _canonical(nm)
                if not canonical or canonical not in existing:
                    sf_rows_skipped += 1
                    continue

                cat_row  = existing[canonical]
                # Evitar duplicados en scryfall_ids
                current_ids = {
                    (e.get("scryfall_id") if isinstance(e, dict) else e)
                    for e in (cat_row.scryfall_ids or [])
                }
                if sid in current_ids:
                    sf_rows_skipped += 1
                    continue

                sf_entry = {
                    "scryfall_id":       sid,
                    "set_code":          str(row_data.get("Set code")         or "").strip().upper(),
                    "set_name":          str(row_data.get("Set name")         or "").strip(),
                    "collector_number":  str(row_data.get("Collector number") or "").strip(),
                    "lang":              str(row_data.get("Language")         or "en").strip()[:2].lower(),
                    "foil":              str(row_data.get("Foil")             or "normal").strip().lower(),
                }
                cat_row.scryfall_ids = list(cat_row.scryfall_ids or []) + [sf_entry]
                sf_rows_added += 1

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"Error al guardar catálogo: {e}")

    _invalidate_catalog_cache()
    _get_catalog_map(db)   # recargar _catalog_cache + _scryfall_map en RAM

    return {
        "status":           "ok",
        "phase1_inserted":  ins1,
        "phase1_updated":   upd1,
        "phase1_total":     ins1 + upd1,
        "phase2_sf_added":  sf_rows_added,
        "phase2_sf_skip":   sf_rows_skipped,
        "scryfall_map_size": len(_scryfall_map),
        "synced_at":        now.isoformat(),
    }


@app.get("/api/admin/catalog", dependencies=[Depends(verify_admin)])
def catalog_list(
    db: Session = Depends(get_db),
    q:     str           = "",
    tier:  Optional[str] = None,
    page:  int           = 1,
    limit: int           = 50,
):
    """
    Lista el catálogo con paginación y filtros.
    Enriquece cada carta con su tier (de staple_cards) y stock en vivo del cache JS.
    """
    from .models import CardCatalog

    query = db.query(CardCatalog)
    if q:
        query = query.filter(CardCatalog.name_normalized.contains(_canonical(q)))

    total = query.count()
    rows  = query.order_by(CardCatalog.name_display).offset((page - 1) * limit).limit(limit).all()

    # Enriquecer con tier actual y stock vivo
    staple_map = _get_staple_map(db)
    js_stock   = _js_stock_cache   # RAM — sin await, ya cargado

    result_rows = []
    for r in rows:
        srec      = staple_map.get(r.name_normalized)
        live_data = _resolve_stock(r.name_normalized, js_stock)
        live_stk  = live_data.get("stock") if live_data else r.total_stock

        if tier and (srec.tier if srec else None) != tier:
            continue

        result_rows.append({
            "id":              r.id,
            "name":            r.name_display,
            "canonical":       r.name_normalized,
            "js_product_ids":  r.js_product_ids or [],
            "variants":        r.js_variants    or [],
            "total_stock":     live_stk,
            "total_stock_db":  r.total_stock,
            "last_synced":     r.last_synced.isoformat() if r.last_synced else None,
            "tier":            srec.tier      if srec else None,
            "staple_id":       srec.id        if srec else None,
            "min_stock":       (srec.min_stock_override if srec and srec.min_stock_override
                                else settings.MIN_STOCK_ALTA if srec and srec.tier == "alta"
                                else settings.MIN_STOCK_NORMAL if srec else None),
        })

    return {
        "total":    total,
        "page":     page,
        "limit":    limit,
        "pages":    (total + limit - 1) // limit,
        "results":  result_rows,
    }


@app.get("/api/admin/catalog/stats", dependencies=[Depends(verify_admin)])
def catalog_stats(db: Session = Depends(get_db)):
    """Resumen del estado del catálogo para el dashboard."""
    from .models import CardCatalog
    total_cards  = db.query(CardCatalog).count()
    total_skus   = db.query(func.sum(func.json_array_length(CardCatalog.js_product_ids))).scalar() or 0
    total_stock  = db.query(func.sum(CardCatalog.total_stock)).scalar() or 0
    last_sync    = db.query(func.max(CardCatalog.last_synced)).scalar()
    # cartas con al menos 1 scryfall_id vinculado
    enriched = db.query(CardCatalog).filter(
        CardCatalog.scryfall_ids.isnot(None),
        func.json_array_length(CardCatalog.scryfall_ids) > 0
    ).count()
    from .models import StapleCard
    staple_count = db.query(StapleCard).count()
    return {
        "total_cards":      total_cards,
        "enriched_cards":   enriched,
        "total_skus":       int(total_skus),
        "total_stock":      int(total_stock),
        "last_sync":        last_sync.isoformat() if last_sync else None,
        "staple_count":     staple_count,
        "js_cache_live":    len(_js_stock_cache),
        "js_by_id_size":    len(_js_by_id),
        "scryfall_map_size": len(_scryfall_map),
        "enrich_running":   _enrich_running,
        "enrich_done":      _enrich_done,
        "enrich_total":     _enrich_total,
    }


# ── Scryfall Bulk Enrichment — streaming, sin OOM, ~2 minutos para todo ────────
#
# Estrategia:
#   1. GET https://api.scryfall.com/bulk-data → obtener download_uri de "default_cards"
#   2. Stream download del JSON (~100MB) con aiohttp
#   3. Parsear card por card con ijson (streaming) → RAM máxima ~30MB
#   4. Agrupar scryfall_ids por _canonical(name) en dict en RAM
#   5. Upsert masivo en card_catalog en lotes de 200
#   6. Invalidar cache → recargar _scryfall_map
#
# "default_cards": una entrada por impresión en inglés (~90,000 cards).
# Actualizado cada 12h por Scryfall. Sin rate limiting — 1 solo request.

async def _do_bulk_enrich() -> dict:
    """
    Descarga e indexa el bulk data 'default_cards' de Scryfall usando streaming JSON.
    Retorna dict con métricas del resultado.
    Diseñado para correr como asyncio.Task en background.
    """
    global _enrich_running, _enrich_total, _enrich_done, _enrich_errors
    global _enrich_skipped, _enrich_last_card

    _enrich_running  = True
    _enrich_done     = 0
    _enrich_errors   = 0
    _enrich_skipped  = 0
    _enrich_last_card = "obteniendo URL del bulk…"

    BULK_INDEX = "https://api.scryfall.com/bulk-data"
    HEADERS    = {
        "User-Agent": "GameQuestBuylist/1.0 (contacto@gamequest.cl)",
        "Accept":     "application/json;q=0.9,*/*;q=0.8",
    }

    db = SessionLocal()
    try:
        import ijson                        # streaming JSON parser
        from .models import CardCatalog

        # ── Paso 1: obtener download_uri del tipo "default_cards" ─────────────
        async with aiohttp.ClientSession(headers=HEADERS,
                                          timeout=aiohttp.ClientTimeout(total=30)) as session:
            async with session.get(BULK_INDEX) as r:
                if r.status != 200:
                    raise RuntimeError(f"Bulk index HTTP {r.status}")
                index_data = await r.json(content_type=None)

        download_uri = None
        for item in index_data.get("data", []):
            if item.get("type") == "default_cards":
                download_uri = item["download_uri"]
                approx_mb   = item.get("size", 0) // 1_048_576
                logger.info(f"[BULK] default_cards URI obtenida (~{approx_mb} MB): {download_uri}")
                break

        if not download_uri:
            raise RuntimeError("No se encontró 'default_cards' en el índice de bulk data")

        # ── Paso 2: cargar todos los canonicals existentes en BD → set ────────
        # Solo queremos enriquecer cartas que YA están en card_catalog
        existing_rows = {r.name_normalized: r for r in db.query(CardCatalog).all()}
        canonical_set = set(existing_rows.keys())
        _enrich_total = len(canonical_set)
        logger.info(f"[BULK] {_enrich_total} cartas en catálogo a enriquecer")

        # ── Paso 3: stream download + parse con ijson ─────────────────────────
        # Acumulamos scryfall_ids en RAM agrupados por canonical
        # {canonical: [{scryfall_id, set_code, set_name, collector_number, lang, finishes}]}
        sf_map_new: dict[str, list] = {c: [] for c in canonical_set}
        cards_seen = 0
        matched    = 0

        async with aiohttp.ClientSession(headers=HEADERS,
                                          timeout=aiohttp.ClientTimeout(
                                              total=600,    # 10 min — descarga grande
                                              sock_read=60,
                                          )) as session:
            async with session.get(download_uri) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"Download HTTP {resp.status}")

                logger.info("[BULK] Descarga iniciada — parseando en streaming…")

                # ijson necesita un objeto file-like síncrono.
                # Acumulamos chunks y procesamos con un pipe async → sync usando asyncio.
                # Estrategia: leer en chunks y pasar a ijson a través de un BytesIO rolling.
                # Para evitar OOM usamos un buffer circular de ~4MB.
                import io

                buffer = io.BytesIO()
                total_bytes = 0
                CHUNK = 512 * 1024   # 512KB por chunk

                # Leer TODO el stream en buffer (necesario para ijson síncrono)
                # PERO liberamos los objetos procesados inmediatamente
                # Alternativa real: usar ijson.parse() con coroutine wrapper
                # En free tier 512MB: 100MB gzip → ~100MB descomprimido en buffer → OK
                async for chunk in resp.content.iter_chunked(CHUNK):
                    buffer.write(chunk)
                    total_bytes += len(chunk)
                    # Actualizar progreso de descarga
                    _enrich_last_card = f"descargando… {total_bytes // 1_048_576} MB"
                    # Yield control para no bloquear el event loop
                    if total_bytes % (4 * 1024 * 1024) == 0:
                        await asyncio.sleep(0)

                logger.info(f"[BULK] Descarga completa: {total_bytes // 1_048_576} MB")
                buffer.seek(0)

                # ── Paso 4: parsear card por card con ijson ───────────────────
                _enrich_last_card = "parseando JSON…"
                await asyncio.sleep(0)   # yield antes del parseo síncrono

                # ijson.items es síncrono pero muy eficiente en memoria
                # procesa sin cargar el JSON completo como dict
                for card in ijson.items(buffer, "item"):
                    cards_seen += 1

                    name = card.get("name", "")
                    if not name:
                        continue

                    # Saltar tokens, emblemas, y cartas de otros idiomas
                    lang = card.get("lang", "en")

                    # Saltar layouts que no son cartas reales
                    layout = card.get("layout", "")
                    if layout in ("token", "emblem", "art_series", "double_faced_token"):
                        continue

                    canonical = _canonical(name)
                    if canonical not in canonical_set:
                        _enrich_skipped += 1
                        continue

                    finishes = card.get("finishes") or []
                    # Si finishes vacío, inferir desde campos legacy foil/nonfoil
                    if not finishes:
                        if card.get("foil"):    finishes.append("foil")
                        if card.get("nonfoil"): finishes.append("nonfoil")
                    if not finishes:
                        finishes = ["nonfoil"]   # default seguro

                    sf_entry = {
                        "scryfall_id":      card["id"],
                        "set_code":         card.get("set", "").upper(),
                        "set_name":         card.get("set_name", ""),
                        "collector_number": card.get("collector_number", ""),
                        "lang":             lang,
                        "finishes":         finishes,
                    }
                    sf_map_new[canonical].append(sf_entry)
                    matched += 1

                    if cards_seen % 5000 == 0:
                        logger.info(f"[BULK] Parseadas {cards_seen} cartas, {matched} matches…")
                        await asyncio.sleep(0)   # yield para no bloquear event loop

        logger.info(f"[BULK] Parseo completo: {cards_seen} cartas vistas, {matched} matches")

        # ── Paso 5: upsert masivo en BD en lotes de 200 ───────────────────────
        _enrich_last_card = "guardando en base de datos…"
        batch   = []
        written = 0

        for canonical, sf_ids in sf_map_new.items():
            row = existing_rows.get(canonical)
            if not row:
                continue
            if sf_ids:   # solo actualizar si encontramos algo
                row.scryfall_ids = sf_ids
                batch.append(row)
                written += 1
                _enrich_done += 1
            else:
                _enrich_skipped += 1

            _enrich_last_card = canonical

            if len(batch) >= 200:
                try:
                    db.commit()
                    batch = []
                    logger.info(f"[BULK] BD: {written} cartas escritas…")
                except Exception as e:
                    db.rollback()
                    logger.error(f"[BULK] Error commit: {e}")
                    _enrich_errors += 1

        # Commit del lote final
        if batch:
            try:
                db.commit()
            except Exception as e:
                db.rollback()
                logger.error(f"[BULK] Error commit final: {e}")

        _invalidate_catalog_cache()
        _get_catalog_map(db)   # recargar _scryfall_map con todos los IDs nuevos

        result = {
            "status":           "completed",
            "cards_in_file":    cards_seen,
            "matched_to_catalog": matched,
            "written_to_db":    written,
            "not_in_catalog":   _enrich_skipped,
            "scryfall_map_size": len(_scryfall_map),
        }
        logger.info(f"[BULK] ✅ {result}")
        return result

    except asyncio.CancelledError:
        logger.info("[BULK] Cancelado")
        return {"status": "cancelled", "done": _enrich_done}
    except Exception as e:
        logger.error(f"[BULK] Error fatal: {e}", exc_info=True)
        _enrich_errors += 1
        return {"status": "error", "detail": str(e)}
    finally:
        _enrich_running   = False
        _enrich_last_card = ""
        db.close()


@app.post("/api/admin/catalog/enrich_scryfall", dependencies=[Depends(verify_admin)])
async def start_enrich_scryfall():
    """
    Descarga el bulk data 'default_cards' de Scryfall (~100MB) y enriquece
    card_catalog.scryfall_ids para TODAS las cartas del catálogo en ~2 minutos.

    Proceso:
      1. Obtiene la URL del día desde https://api.scryfall.com/bulk-data
      2. Descarga en streaming con aiohttp
      3. Parsea con ijson (streaming) → RAM máxima ~30MB sin importar el tamaño
      4. Agrupa todos los scryfall_ids por nombre canónico
      5. Hace upsert masivo en BD en lotes de 200
      6. Recarga _scryfall_map en RAM

    Condiciones:
      - El catálogo debe estar poblado (correr catalog/sync primero)
      - Solo se enriquecen cartas que ya están en card_catalog
      - Idempotente: re-correr sobreescribe scryfall_ids con datos frescos
    """
    global _enrich_running, _enrich_task

    if _enrich_running:
        return {
            "status":    "already_running",
            "done":      _enrich_done,
            "total":     _enrich_total,
            "last_card": _enrich_last_card,
        }

    _enrich_task = asyncio.create_task(_do_bulk_enrich())
    return {
        "status":  "started",
        "message": "Bulk enrichment iniciado. ~2 minutos para completar. "
                   "Monitorear con GET /api/admin/catalog/enrich_status",
    }


@app.get("/api/admin/catalog/enrich_status", dependencies=[Depends(verify_admin)])
def enrich_status():
    """Polling del estado del enriquecimiento en curso."""
    total = _enrich_total or 1
    pct   = round(100 * _enrich_done / total) if _enrich_total > 0 else 0
    return {
        "running":           _enrich_running,
        "done":              _enrich_done,
        "total":             _enrich_total,
        "pct":               pct,
        "errors":            _enrich_errors,
        "skipped":           _enrich_skipped,
        "last_card":         _enrich_last_card,
        "scryfall_map_size": len(_scryfall_map),
    }


@app.post("/api/admin/catalog/enrich_cancel", dependencies=[Depends(verify_admin)])
async def cancel_enrich():
    """Cancela el enriquecimiento en curso."""
    global _enrich_running, _enrich_task
    if not _enrich_running:
        return {"status": "not_running"}
    _enrich_running = False
    if _enrich_task and not _enrich_task.done():
        _enrich_task.cancel()
    return {"status": "cancelling", "done": _enrich_done, "total": _enrich_total}



# =====================================================================
# 🃏 STAPLES CRUD
# =====================================================================

@app.get("/api/admin/staples", dependencies=[Depends(verify_admin)])
def list_staples(db: Session = Depends(get_db)):
    from .models import StapleCard
    return [{"id": c.id, "name": c.name_display, "tier": c.tier,
             "min_stock_override": c.min_stock_override, "margin_factor": c.margin_factor,
             "created_at": c.created_at.isoformat() if c.created_at else None}
            for c in db.query(StapleCard).order_by(StapleCard.name_display).all()]


@app.post("/api/admin/staples", dependencies=[Depends(verify_admin)])
def upsert_staple(req: StapleUpsertReq, db: Session = Depends(get_db)):
    """
    Guarda/actualiza una carta en la lista de demanda.
    Clave canónica: _canonical(name) — inmune a apóstrofes tipográficos.
    Maneja migración de registros viejos con name_normalized en U+2019.
    """
    from .models import StapleCard
    clean_name = req.name.strip()
    key        = _canonical(clean_name)

    # Buscar primero por la clave nueva (canónica)
    card = db.query(StapleCard).filter(StapleCard.name_normalized == key).first()

    # Fallback: buscar por _canonical(name_display) para registros con clave vieja (U+2019)
    if not card:
        card = (db.query(StapleCard)
                  .filter(func.lower(StapleCard.name_display) == clean_name.lower())
                  .first())
        if card:
            # Migrar clave al formato canónico actual
            card.name_normalized = key

    if card:
        card.tier               = req.tier
        card.min_stock_override = req.min_stock_override
        card.margin_factor      = req.margin_factor
        card.name_display       = clean_name   # actualizar siempre al nombre más reciente
        card.name_normalized    = key          # garantizar clave canónica
    else:
        card = StapleCard(
            name_normalized    = key,
            name_display       = clean_name,
            tier               = req.tier,
            min_stock_override = req.min_stock_override,
            margin_factor      = req.margin_factor,
        )
        db.add(card)

    db.commit()
    db.refresh(card)
    _invalidate_staple_cache()   # forzar recarga en el próximo request
    return {"status": "ok", "id": card.id, "name": card.name_display, "key": key}


@app.delete("/api/admin/staples/{staple_id}", dependencies=[Depends(verify_admin)])
def delete_staple(staple_id: int, db: Session = Depends(get_db)):
    from .models import StapleCard
    card = db.query(StapleCard).filter(StapleCard.id == staple_id).first()
    if not card:
        raise HTTPException(404, "Carta no encontrada")
    db.delete(card)
    db.commit()
    _invalidate_staple_cache()   # forzar recarga en el próximo request
    return {"status": "ok"}


@app.post("/api/admin/staples/bulk", dependencies=[Depends(verify_admin)])
async def bulk_import_staples(
    file: UploadFile = File(...),
    tier: str        = "alta",          # tier por defecto para todas las cartas del CSV
    db: Session = Depends(get_db),
):
    """
    Importa masivamente cartas a la tabla de demanda desde un CSV Manabox.

    - Lee la columna 'Name' del CSV y hace upsert de cada carta única.
    - tier: 'normal' | 'alta' | 'muy_alta' — se aplica a TODAS las cartas del CSV.
    - Si una carta ya existe, SOLO actualiza el tier (no toca min_stock_override ni margin_factor).
    - Si no existe, la crea con los valores por defecto.
    - Usa una sola transacción de BD → atómica (todo o nada).
    - Devuelve conteos: inserted, updated, skipped (nombres vacíos/duplicados).
    """
    from .models import StapleCard

    if tier not in ("normal", "alta", "muy_alta"):
        raise HTTPException(422, f"tier inválido '{tier}'. Debe ser: normal | alta | muy_alta")

    content = await file.read()
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(413, "Archivo demasiado grande (máx 5 MB)")

    df = _read_csv(content)
    if "Name" not in df.columns:
        raise HTTPException(422, f"Columna 'Name' no encontrada. Columnas presentes: {list(df.columns)}")

    # Extraer nombres únicos del CSV (ignorar vacíos)
    names_raw = df["Name"].dropna().astype(str).str.strip()
    names_raw = names_raw[names_raw != ""]
    unique_names = list(dict.fromkeys(names_raw.tolist()))   # orden de aparición, sin duplicados

    if not unique_names:
        raise HTTPException(422, "No se encontraron nombres de cartas en el CSV")

    # Cargar todos los staples existentes en memoria para lookup O(1)
    existing = {s.name_normalized: s for s in db.query(StapleCard).all()}

    inserted = updated = skipped = 0

    for name in unique_names:
        key = _canonical(name)
        if not key:
            skipped += 1
            continue

        if key in existing:
            # Solo actualizar tier — respetar configuración manual de stock/margin
            card = existing[key]
            if card.tier != tier:
                card.tier         = tier
                card.name_display = name   # actualizar al nombre más reciente
                updated += 1
            else:
                skipped += 1   # ya tiene ese tier, no tocar
        else:
            card = StapleCard(
                name_normalized = key,
                name_display    = name,
                tier            = tier,
            )
            db.add(card)
            existing[key] = card   # evitar duplicados dentro del mismo CSV
            inserted += 1

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"Error al guardar en BD: {str(e)}")

    _invalidate_staple_cache()

    return {
        "status":   "ok",
        "tier":     tier,
        "total":    len(unique_names),
        "inserted": inserted,
        "updated":  updated,
        "skipped":  skipped,
    }


@app.post("/api/admin/stock_check", dependencies=[Depends(verify_admin)])
async def stock_check(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    from .models import StapleCard

    content = await file.read()
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(413, "Archivo demasiado grande")

    df       = _read_csv(content)
    required = {"Name", "Quantity", "Purchase price"}
    missing  = required - set(df.columns)
    if missing:
        raise HTTPException(422, f"Columnas faltantes: {missing}")

    from .models import StapleCard
    base_min_price = _build_base_min_price(df)
    staple_map     = _get_staple_map(db)
    js_stock       = await _fetch_js_stock_cached()
    _get_catalog_map(db)   # pre-cargar catálogo en _catalog_cache

    # Constantes fuera del loop — evitar re-acceso a settings por cada fila
    F_CASH    = settings.BUYLIST_FACTOR_CASH
    F_CRED    = settings.BUYLIST_FACTOR_CREDITO
    STAKE_M   = settings.STAKE_MULTIPLIER
    MIN_ALT   = settings.MIN_STOCK_ALTA
    MIN_NRM   = settings.MIN_STOCK_NORMAL
    MIN_PU    = settings.MIN_PURCHASE_USD

    results      = []
    total_compra = 0.0

    for row in df.to_dict("records"):    # 1.8x más rápido que iterrows()
        name     = str(row.get("Name") or "").strip()
        qty      = int(row.get("Quantity") or 1)
        raw_usd  = float(row.get("Purchase price") or 0)
        foil_raw = str(row.get("Foil") or "normal").strip().lower()
        cond_raw = str(row.get("Condition") or "near_mint").strip().lower()
        version  = str(row.get("Version") or "").strip()
        currency = str(row.get("Purchase price currency") or "USD").strip().upper()

        if not name or raw_usd <= 0:
            continue
        if currency == "EUR":
            raw_usd *= 1.10

        eff_usd, price_usd, is_estaca_card = _compute_card_price(
            raw_usd, foil_raw, version, cond_raw, base_min_price, name
        )

        # Tier lookup O(1) — canonical ya en cache lru tras _compute_card_price
        key         = _canonical(name)    # O(1) hit en lru_cache
        staple_rec  = staple_map.get(key) # O(1) dict lookup directo
        tier        = staple_rec.tier if staple_rec else "normal"
        is_muy_alta = tier == "muy_alta"
        is_alta     = tier == "alta"
        min_stock   = (staple_rec.min_stock_override if staple_rec and staple_rec.min_stock_override
                       else MIN_ALT if is_alta else MIN_NRM)
        margin      = staple_rec.margin_factor if staple_rec else 2.5

        price_clp_cash    = price_usd * F_CASH
        price_clp_credito = price_usd * F_CRED
        min_price_venta   = price_clp_cash * margin

        js_data          = _resolve_stock(key, js_stock)   # SKU-first si hay catálogo
        stock_actual     = js_data.get("stock") if js_data else None
        price_js         = js_data.get("price", 0)
        js_id            = js_data.get("id")
        stock_proyectado = (stock_actual or 0) + qty

        # ── Alertas ───────────────────────────────────────────────────────
        alerts = []
        status = "ok"

        if stock_actual is None:
            alerts.append({"type": "info", "msg": "No existe en Jumpseller — se creará"})
            status = "info"

        if raw_usd < MIN_PU:
            alerts.append({"type": "warning",
                           "msg": f"Precio ${raw_usd:.2f} USD bajo mínimo rentable (${MIN_PU} USD)"})
            if status == "ok":
                status = "warning"

        if is_estaca_card:
            base_used = base_min_price.get(key, raw_usd)
            origin    = "CSV base" if key in base_min_price else "precio CK propio"
            alerts.append({"type": "info",
                           "msg": f"Estaca ×{STAKE_M} (base ${base_used:.2f} USD — {origin})"})

        if not is_muy_alta:
            overstock_limit = min_stock * 3
            if stock_proyectado > overstock_limit:
                alerts.append({"type": "danger",
                               "msg": f"Sobrestock: tendrías {stock_proyectado} u. (límite {overstock_limit})"})
                status = "danger"
            elif stock_proyectado > min_stock * 2:
                alerts.append({"type": "warning", "msg": f"Stock alto: {stock_proyectado} u. tras compra"})
                if status == "ok":
                    status = "warning"
        else:
            alerts.append({"type": "info", "msg": "Muy alta demanda — compra siempre"})

        if price_js > 0 and price_js < min_price_venta:
            alerts.append({"type": "danger",
                           "msg": f"Precio JS ${price_js:,.0f} bajo mínimo recomendado ${min_price_venta:,.0f}"})
            status = "danger"

        # ── Cupo disponible (semántica max_stock: compramos hasta llegar al objetivo) ───
        # min_stock aquí es el MÁXIMO deseado (misma semántica que analyze_buylist).
        # Si stock_actual < min_stock → hay cupo, podríamos comprar más.
        # Es info, no warning: no penaliza el `approved` de la carta.
        if not is_muy_alta and stock_actual is not None:
            cupo_sc = max(0, min_stock - stock_actual)
            if cupo_sc > 0:
                alerts.append({"type": "info",
                               "msg": f"Cupo disponible: {cupo_sc} u. "
                                      f"(stock {stock_actual}/{min_stock} objetivo)"})

        total_compra += raw_usd * qty

        results.append({
            "name":              name,
            "qty":               qty,
            "price_usd":         round(price_usd, 2),
            "price_usd_raw":     round(raw_usd, 4),
            "price_usd_eff":     round(eff_usd, 4),
            "foil":              foil_raw,
            "condition":         cond_raw,
            "version":           version,
            "is_estaca":         is_estaca_card,
            "stake_mult":        settings.STAKE_MULTIPLIER if is_estaca_card else 1.0,
            "price_cash":        int(price_clp_cash),
            "price_credito":     int(price_clp_credito),
            "min_price_venta":   int(min_price_venta),
            "tier":              tier,
            "is_muy_alta":       is_muy_alta,
            "js_variants_count": len(js_data.get("variants", [])),
            "min_stock":         min_stock,
            "stock_actual":      stock_actual,
            "stock_proyectado":  stock_proyectado,
            "price_js":          price_js,
            "js_id":             js_id,
            "alerts":            alerts,
            "status":            status,
            "approved":          is_muy_alta or all(a["type"] != "danger" for a in alerts),
        })

    order_map = {"danger": 0, "warning": 1, "info": 2, "ok": 3}
    results.sort(key=lambda r: order_map.get(r["status"], 9))

    total_clp_cash    = sum(r["price_cash"]    * r["qty"] for r in results)
    total_clp_credito = sum(r["price_credito"] * r["qty"] for r in results)

    return {
        "summary": {
            "total_cards":       len(results),
            "danger_count":      sum(1 for r in results if r["status"] == "danger"),
            "warning_count":     sum(1 for r in results if r["status"] == "warning"),
            "ok_count":          sum(1 for r in results if r["status"] == "ok"),
            "total_usd_compra":  round(total_compra, 2),
            "total_clp_cash":    total_clp_cash,
            "total_clp_credito": total_clp_credito,
        },
        "cards": results,
    }


# ── Enviar reporte de análisis interno por email ──────────────────────────────

@app.post("/api/admin/email_analysis", dependencies=[Depends(verify_admin)])
async def email_analysis_report(req: EmailAnalysisReq):
    """Envía el reporte del stock_check al email de la tienda."""
    ok = await email_service.send_internal_analysis_report(
        items    = req.cards,
        summary  = req.summary,
        filename = req.filename,
    )
    if ok:
        return {"status": "ok", "message": f"Reporte enviado a {settings.TARGET_EMAIL}"}
    raise HTTPException(502, "Error enviando email. Revisar configuración SMTP.")


# ── Editar precio en Jumpseller ───────────────────────────────────────────────

@app.post("/api/admin/js_update_price", dependencies=[Depends(verify_admin)])
async def js_update_price(req: JSPriceUpdateReq):
    url     = f"{settings.JUMPSELLER_API_BASE}/products/{req.product_id}.json"
    params  = {"login": settings.JS_LOGIN_CODE, "authtoken": settings.JS_AUTH_TOKEN}
    payload = {"product": {"price": req.new_price}}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.put(url, params=params, json=payload,
                                   timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status in [200, 201]:
                    # Invalidar cache JS para reflejar cambio
                    global _js_stock_cache_ts
                    _js_stock_cache_ts = 0.0
                    return {"status": "ok", "product_id": req.product_id, "new_price": req.new_price}
                err = await resp.text()
                raise HTTPException(502, f"Jumpseller [{resp.status}]: {err}")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(502, str(e))


# ── Buylist orders admin ──────────────────────────────────────────────────────

@app.get("/api/admin/buylist_orders", dependencies=[Depends(verify_admin)])
def get_buylist_orders(db: Session = Depends(get_db), status: Optional[str] = None):
    from .models import BuylistOrder
    q = db.query(BuylistOrder)
    if status:
        q = q.filter(BuylistOrder.status == status)
    orders = q.order_by(BuylistOrder.created_at.desc()).all()
    return [{"id": o.id, "rut": o.rut, "email": o.email,
             "payment_preference": o.payment_preference,
             "total_credito": float(o.total_credito or 0),
             "total_cash":    float(o.total_cash    or 0),
             "status":        o.status,
             "created_at":    o.created_at.isoformat() if o.created_at else None,
             "items":         o.items} for o in orders]


@app.patch("/api/admin/buylist_orders/{order_id}", dependencies=[Depends(verify_admin)])
def update_order_status(order_id: int, new_status: str = "reviewed",
                         db: Session = Depends(get_db)):
    from .models import BuylistOrder
    order = db.query(BuylistOrder).filter(BuylistOrder.id == order_id).first()
    if not order:
        raise HTTPException(404, "Orden no encontrada")
    order.status = new_status
    db.commit()
    return {"status": "ok", "order_id": order_id, "new_status": new_status}


# =====================================================================
# 🔥 BACKGROUND TASKS — cupones y webhooks
# =====================================================================

async def _bg_burn_coupons(payload_str: str):
    try:
        for code in set(re.findall(r"QP-[A-F0-9]{6}\b", payload_str)):
            await VaultController.burn_coupon(code)
    except Exception as e:
        logger.error(f"[BURN] {e}")

def _bg_sync_customer(customer_data: dict):
    db = SessionLocal()
    try:
        VaultController.sync_user(db, customer_data)
    except Exception as e:
        logger.error(f"[SYNC] {e}")
    finally:
        db.close()

@app.post("/api/webhooks/jumpseller/order")
async def jumpseller_order_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        payload_str = (await request.body()).decode("utf-8")
        background_tasks.add_task(_bg_burn_coupons, payload_str)
        return {"status": "received"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.post("/api/webhooks/jumpseller/customer")
async def jumpseller_customer_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        payload       = await request.json()
        customer_data = payload.get("customer", {})
        if customer_data and customer_data.get("email"):
            background_tasks.add_task(_bg_sync_customer, customer_data)
        return {"status": "received"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# =====================================================================
# 🛠️ SALUD
# =====================================================================

@app.get("/api/admin/stock_lookup", dependencies=[Depends(verify_admin)])
async def stock_lookup(q: str = "", db: Session = Depends(get_db)):
    """
    Búsqueda de carta en el cache de Jumpseller por tokens.

    Estrategia de match (en orden de prioridad):
      1. Exacto           — "sol ring"  == "sol ring"
      2. Prefijo completo — key.startswith(q_norm)
      3. Todos los tokens — "fat pus"  → ["fat","pus"] ambos son prefijo de
                             alguna palabra en "fatal push" ✓
                             "dimir hous" → ["dimir","hous"] ✓ en "dimir house guard"
      4. Cualquier token  — al menos 1 token ≥3 chars matchea como prefijo de palabra

    No procesa CSV — consulta el cache en RAM (O(n) sobre ~21k cartas, <10ms).
    """
    q_clean = q.strip()
    if len(q_clean) < 2:
        raise HTTPException(422, "Escribe al menos 2 caracteres")

    js_stock = await _fetch_js_stock_cached()
    from .models import StapleCard

    q_norm   = _normalize_name(q_clean)   # ej: "sol ring", "dimir hous"
    q_tokens = q_norm.split()             # ["sol","ring"] / ["dimir","hous"]

    def _score(key: str) -> int | None:
        """
        Devuelve prioridad de match (0=mejor) o None si no hay match.
        Comparamos contra las palabras del key para soportar typos parciales.
        """
        key_words = key.split()

        # 1. Coincidencia exacta
        if key == q_norm:
            return 0

        # 2. Key empieza con el query completo
        if key.startswith(q_norm):
            return 1

        # 3. Todos los tokens del query son prefijo de alguna palabra del key
        #    "fat pus" → "fat" es prefijo de "fatal" ✓, "pus" de "push" ✓
        def token_matches_any_word(tok: str) -> bool:
            return any(w.startswith(tok) for w in key_words)

        if all(token_matches_any_word(t) for t in q_tokens):
            # Cuántas palabras matchean exactamente (mejor ranking si más exactas)
            exact = sum(1 for t in q_tokens if t in key_words)
            return 2 + (10 - min(exact, 9))   # 2–11

        # 4. Al menos un token largo (≥3) matchea como prefijo de alguna palabra
        long_tokens = [t for t in q_tokens if len(t) >= 3]
        if long_tokens and any(token_matches_any_word(t) for t in long_tokens):
            return 20

        return None  # sin match

    # Recorrer cache y puntuar
    scored: list[tuple[int, str, dict]] = []
    for key, data in js_stock.items():
        s = _score(key)
        if s is not None:
            scored.append((s, key, data))

    scored.sort(key=lambda x: x[0])

    if not scored:
        return {"query": q_clean, "q_normalized": q_norm, "results": [], "total_matches": 0}

    # Staple map para enriquecer con tier — RAM cache TTL=300s
    staple_map = _get_staple_map(db)

    results = []
    for _, key, data in scored[:20]:
        srec = staple_map.get(key)
        min_s = (srec.min_stock_override if srec and srec.min_stock_override
                 else settings.MIN_STOCK_ALTA   if srec and srec.tier == "alta"
                 else settings.MIN_STOCK_NORMAL)

        # Nombre para mostrar: BD > primera variante JS (parte antes del pipe) > key
        if srec:
            display = srec.name_display
        elif data.get("variants"):
            display = data["variants"][0]["name"].split("|")[0].strip()
        else:
            display = key.title()

        results.append({
            "key":          key,
            "display_name": display,
            "total_stock":  data["stock"],
            "price_js":     data.get("price", 0),
            "variants":     [{"name": v["name"], "stock": v["stock"], "price": v["price"]}
                             for v in (data.get("variants") or [])],
            "tier":         srec.tier if srec else None,
            "staple_id":    srec.id   if srec else None,
            "min_stock":    min_s     if srec else None,
            "in_list":      srec is not None,
        })

    return {
        "query":         q_clean,
        "q_normalized":  q_norm,
        "results":       results,
        "total_matches": len(scored),
        "cache_valid":   bool(js_stock),
    }


@app.post("/api/admin/clean_coupons", dependencies=[Depends(verify_admin)])
async def clean_used_coupons():
    burned = await VaultController.sweep_used_coupons()
    return {"status": "success", "burned": burned}

@app.post("/api/admin/migrate_canonical", dependencies=[Depends(verify_admin)])
def migrate_canonical(db: Session = Depends(get_db)):
    """
    Migración idempotente: recalcula name_normalized con _canonical() para todos
    los staples. Necesario tras el fix de apóstrofes tipográficos (U+2019→U+0027).
    Seguro correrlo múltiples veces — no modifica datos si ya están normalizados.
    """
    from .models import StapleCard
    rows     = db.query(StapleCard).all()
    updated  = 0
    skipped  = 0
    for s in rows:
        new_key = _canonical(s.name_display)
        if s.name_normalized != new_key:
            s.name_normalized = new_key
            updated += 1
        else:
            skipped += 1
    db.commit()
    return {"status": "ok", "updated": updated, "skipped": skipped, "total": len(rows)}


@app.get("/health")
def health():
    ci = _canonical.cache_info()
    return {
        "status":               "ok",
        "version":              "5.2",
        "buylist_open":         settings.BUYLIST_OPEN,
        "cash_enabled":         settings.CASH_ENABLED,
        "js_cache_age_sec":     int(time.monotonic() - _js_stock_cache_ts),
        "js_cache_cards":       len(_js_stock_cache),
        "js_cache_valid":       bool(_js_stock_cache and time.monotonic() - _js_stock_cache_ts < JS_STOCK_TTL),
        "staple_cache_age_sec": int(time.monotonic() - _staple_map_cache_ts),
        "staple_cache_valid":   bool(_staple_map_cache and time.monotonic() - _staple_map_cache_ts < STAPLE_CACHE_TTL),
        "staple_cache_entries": len(_staple_map_cache),
        "canonical_cache_hits": ci.hits,
        "canonical_cache_miss": ci.misses,
        "canonical_cache_size": ci.currsize,
    }


@app.post("/api/admin/warmup_cache", dependencies=[Depends(verify_admin)])
async def warmup_cache():
    """
    Fuerza recarga del catálogo JS desde cero.
    Útil después de actualizar productos en Jumpseller o al despertar el free tier.
    """
    global _js_stock_cache_ts
    _js_stock_cache_ts = 0.0  # invalidar cache
    products = await _fetch_js_stock_cached(force=True)
    total_stock = sum(v["stock"] for v in products.values())
    return {
        "status":          "ok",
        "cards_loaded":    len(products),
        "total_stock":     total_stock,
        "cache_valid_sec": JS_STOCK_TTL,
    }


@app.get("/api/admin/cache_status", dependencies=[Depends(verify_admin)])
def cache_status():
    """Estado del cache de stock JS para diagnóstico."""
    age = int(time.monotonic() - _js_stock_cache_ts)
    return {
        "cache_age_sec":   age,
        "cache_ttl_sec":   JS_STOCK_TTL,
        "cache_valid":     bool(_js_stock_cache and age < JS_STOCK_TTL),
        "cards_loaded":    len(_js_stock_cache),
        "total_stock":     sum(v["stock"] for v in _js_stock_cache.values()),
        "sample_5":        list(_js_stock_cache.keys())[:5],
    }
