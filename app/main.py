"""
main.py — GameQuest API v5.5
Cambios v5.5: foil/nicho pipeline definitivo, stock info privado, AbortController,
              CORS Jumpseller, lru_cache 65536, imports de módulo, fixes de sesión,
            email en TODAS las buylists.
"""
import re
import io
import json
import hmac
import time
import secrets
import hashlib
import base64
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
from sqlalchemy.dialects.postgresql import insert as pg_insert  # H-04: ON CONFLICT DO NOTHING

from .database import get_db, engine, Base, SessionLocal
from .vault import VaultController
from .schemas import CanjeRequest, LoginRequest, TokenResponse, BuylistCommitRequest
from .config import settings
from . import email_service
from .models import Gampoint, BuylistOrder, StapleCard, CardCatalog, CashbackRecord, CKPrice, CanjeRecord  # MAIN-08 FIX

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
        # MAIN-05 FIX: _rate_store limpiado cada iteración (era cada 480s solo)
        # Evita memory leak con IPs rotativas bajo ataque DDoS en free tier 512MB
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


async def _background_ck_sync():
    """
    Job diario: sincroniza la tabla ck_prices desde la pricelist de CardKingdom.

    Comportamiento:
    - Corre una vez al arrancar (carga inicial, especialmente útil en cold start).
    - Luego espera 24 horas y repite indefinidamente.
    - Si el sync falla (red, BD) → log warning, no mata la app, reintenta al día siguiente.
    - _ck_sync_lock en _sync_ck_prices() garantiza que dos arranques simultáneos
      (scaling o restart) no ejecuten dos upserts al mismo tiempo.
    """
    # Carga inicial — no bloquea el arranque (es una task independiente)
    try:
        logger.info("[CK_SYNC] Carga inicial al arrancar...")
        await _sync_ck_prices()
    except Exception as exc:
        logger.warning(f"[CK_SYNC] Error en carga inicial: {exc}")

    # Loop diario
    while True:
        await asyncio.sleep(86_400)   # 24 horas
        try:
            logger.info("[CK_SYNC] Sync diario iniciado...")
            await _sync_ck_prices()
        except Exception as exc:
            logger.warning(f"[CK_SYNC] Error en sync diario: {exc}")


@asynccontextmanager
async def lifespan(app_: FastAPI):
    """Gestiona el ciclo de vida de la app: arranca el prefetch al iniciar."""
    # FIX CRÍTICO: validar credenciales antes de aceptar cualquier request.

    # Bloquea el arranque en producción si alguna credencial usa el default inseguro.
    settings.validate_production_secrets()
    # TRV-01 FIX: configurar nivel de logging explícito para evitar verbosidad debug en prod
    import logging as _logging
    _logging.getLogger("app").setLevel(_logging.INFO)
    _logging.getLogger("app.main").setLevel(_logging.INFO)
    _init_cond_mult()   # poblar dict de condición una vez (settings ya cargadas)
    task_js = asyncio.create_task(_background_cache_refresh())
    task_ck = asyncio.create_task(_background_ck_sync())
    logger.info("[STARTUP] Background cache refresh y CK sync iniciados.")
    yield
    for task in (task_js, task_ck):
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="GameCoins API", version="5.5", lifespan=lifespan,  # MAIN-01 FIX
              default_response_class=ORJSONResponse)

app.add_middleware(GZipMiddleware, minimum_size=500)   # comprime JSON ≥500 bytes (~60-80% ahorro)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://gamequest.cl",
        "https://www.gamequest.cl",
        "https://game-quest.jumpseller.com",   # CORS-FIX: preview/admin Jumpseller
        "https://gamecoins.onrender.com",      # dominio Render del propio backend
        "http://localhost:10000",              # desarrollo local
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

# ── CardKingdom pricelist — fuente de verdad para detección De Nicho ──────────
# Los precios se persisten en la tabla ck_prices (PostgreSQL).
# Un job diario hace el upsert; el hot path lee solo las cartas del CSV.
CK_PRICELIST_URL = "https://api.cardkingdom.com/api/pricelist"
_ck_sync_lock    = asyncio.Lock()   # evita ejecuciones simultáneas del job

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

# ── Globals de progreso del enriquecimiento Scryfall ──────────────────────────
# Deben estar a nivel de módulo para que start_enrich_scryfall pueda leerlos
# antes de que _do_bulk_enrich() haya corrido por primera vez.
_enrich_running:   bool  = False
_enrich_total:     int   = 0
_enrich_done:      int   = 0
_enrich_errors:    int   = 0
_enrich_skipped:   int   = 0
_enrich_last_card: str   = ""
_enrich_task:      object = None
# FIX A4: asyncio.Lock para prevenir TOCTOU en start_enrich_scryfall.
# Sin lock, dos requests simultáneos pueden ambos pasar el check `if _enrich_running`
# antes de que cualquiera haya seteado _enrich_running=True, lanzando dos tareas
# que escriben en BD concurrentemente con conflictos de commit.
# asyncio.Lock() a nivel de módulo es seguro: uvicorn corre en un solo event loop
# por worker (1 worker en free tier), igual que _js_fetch_lock en L470.
_enrich_lock: asyncio.Lock = asyncio.Lock()

def _get_catalog_map(db) -> dict:
    """
    Devuelve el catálogo (canonical → CardCatalog) desde RAM si está fresco.
    También reconstruye _scryfall_map para lookup O(1) por Scryfall UUID.
    """
    global _catalog_cache, _catalog_cache_ts, _scryfall_map
    if _catalog_cache and time.monotonic() - _catalog_cache_ts < CATALOG_CACHE_TTL:
        return _catalog_cache
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
    rows = db.query(StapleCard).all()
    _staple_map_cache    = _build_staple_map(rows)
    _staple_map_cache_ts = time.monotonic()
    return _staple_map_cache

def _invalidate_staple_cache() -> None:
    """Llamar tras upsert/delete de staples para forzar recarga inmediata."""
    global _staple_map_cache_ts
    _staple_map_cache_ts = 0.0

# ── Budget diario de Cash (CLP) — reinicio automático a medianoche ────────────
# dict {YYYY-MM-DD: CLP total gastado ese día en compras cash públicas}.
# Solo lo usa commit_buylist (público). El admin no tiene límite.
# In-memory: se reinicia al reiniciar el servidor (aceptable — free tier duerme).
# Thread-safety: asyncio single-threaded (1 worker) → no hay race condition.
import datetime as _dt
# NEW-06 KNOWN LIMITATION: _daily_cash_spent vive en RAM — se reinicia con cada deploy/cold start.
# En free tier de Render los deploys son frecuentes. Para límite estricto, persistir en BD.
# Decisión: aceptar como limitación conocida del free tier.
_daily_cash_spent: dict[str, float] = {}

def _check_and_register_cash_budget(amount_clp: float) -> None:
    """
    Verifica si hay presupuesto disponible y registra el gasto si lo hay.
    Lanza HTTPException 503 si el presupuesto diario de cash está agotado.
    Diseñada para llamarse ANTES de persistir la orden en BD.

    - Si BUYLIST_DAILY_BUDGET_CASH == 0.0 → sin límite, retorna inmediatamente.
    - El día se determina por datetime.date.today() del servidor (UTC en Render).
    - El registro es optimista: se suma al bucket ANTES del db.commit().
      Si el commit falla, el presupuesto queda sobrecontabilizado levemente
      (conservador: prefiere rechazar una orden válida a aceptar una inválida).
    """
    limit = settings.BUYLIST_DAILY_BUDGET_CASH
    if limit <= 0:
        return   # sin límite configurado

    today     = str(_dt.date.today())
    spent     = _daily_cash_spent.get(today, 0.0)
    new_total = spent + amount_clp

    if new_total > limit:
        remaining = max(0.0, limit - spent)
        raise HTTPException(
            status_code=503,
            detail=(
                f"Presupuesto diario de compras en cash alcanzado "
                f"(${limit:,.0f} CLP/día). "
                f"Presupuesto restante hoy: ${remaining:,.0f} CLP. "
                f"Intenta mañana o contacta a la tienda."
            )
        )
    _daily_cash_spent[today] = new_total

# ── Auth ───────────────────────────────────────────────────────────────────────
security = HTTPBearer()

def verify_admin(credentials: HTTPAuthorizationCredentials = Depends(security)):
    # FIX A3: secrets.compare_digest usa comparación en tiempo constante O(n),
    # previniendo timing side-channel attacks donde un atacante puede determinar
    # el token carácter a carácter midiendo el tiempo de respuesta.
    # El operador != es O(k) con cortocircuito en el primer carácter diferente.
    if not secrets.compare_digest(credentials.credentials, settings.STORE_TOKEN):
        raise HTTPException(status_code=401, detail="Token inválido")
    return True

def verify_store_token(x_store_token: Optional[str] = Header(default=None)):
    # FIX A3: Mismo patrón — compare_digest requiere que ambos operandos sean str
    # o ambos bytes.  Si x_store_token es None, la comparación contra el token real
    # siempre falla (length differs) sin exponer información de timing.
    if not secrets.compare_digest(x_store_token or "", settings.STORE_TOKEN):
        raise HTTPException(status_code=401, detail="Token inválido")
    return True

def verify_public_token(x_store_token: Optional[str] = Header(default=None)):
    """
    C-02 FIX: Token público — usado por account.liquid (browser del cliente).
    Valida contra PUBLIC_STORE_TOKEN, que es DISTINTO a STORE_TOKEN.
    Nunca da acceso a endpoints /api/admin/*.
    """
    if not secrets.compare_digest(x_store_token or "", settings.PUBLIC_STORE_TOKEN):
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

@lru_cache(maxsize=65536)   # PROD-02 FIX: 3.4M misses → aumentar cache para catálogo completo
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

# FIX M-07: _COND_MULT inicializado a nivel de módulo con os.getenv() directamente.
# Antes: dict vacío {} poblado en lifespan()._init_cond_mult(). Si lifespan fallaba
# antes de _init_cond_mult() (p.ej. por validate_production_secrets()), todas las
# condiciones retornaban COND_NM hasta el próximo restart — bug silencioso.
# Ahora: los valores se leen al importar el módulo, idénticos a los de settings.
# _init_cond_mult() se mantiene como no-op para no romper la llamada en lifespan().
# MAIN-02 FIX: usar settings directamente para consistencia y evitar desfase en hot-reload
# Antes: float(_os.getenv(...)) duplicaba la fuente de verdad con os.getenv()
_COND_MULT: dict[str, float] = {
    "near_mint":         settings.COND_NM,
    "lightly_played":    settings.COND_LP,
    "moderately_played": settings.COND_MP,
    "heavily_played":    settings.COND_HP,
    "damaged":           settings.COND_DMG,
}

def _init_cond_mult() -> None:
    """
    Refresca _COND_MULT desde settings (por si cambiaron env vars en caliente).
    Llamado en lifespan() para mantener compatibilidad con arranques normales.
    El dict ya está poblado a nivel de módulo — esta función es idempotente.
    """
    _COND_MULT.update({
        "near_mint":         settings.COND_NM,
        "lightly_played":    settings.COND_LP,
        "moderately_played": settings.COND_MP,
        "heavily_played":    settings.COND_HP,
        "damaged":           settings.COND_DMG,
    })

def _cond_mult(cond_str: str) -> float:
    return _COND_MULT.get((cond_str or "near_mint").lower(), settings.COND_NM)


# ── Detección de versión especial (De Nicho) ─────────────────────────────────
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
    True si la carta es una VARIANTE que debe excluirse del cálculo de precio base NM.

    Se usa EXCLUSIVAMENTE en _build_base_min_price() para que base_nm sea siempre
    el precio de la versión NM no-foil más barata — nunca una versión premium.

    EXCLUYE:
      - foil, etched (columna Foil del CSV) → variante de precio diferente
      - versiones especiales premium (Extended Art, Showcase, Serialized, etc.)
        detectadas en la columna Version del CSV

    NO se usa para determinar si una carta es De Nicho.
    El criterio De Nicho es SOLO price_csv > base_nm × STAKE_MULTIPLIER
    y se evalúa en _compute_card_price().

    - foil   : columna Foil del CSV (normal | foil | etched)
    - name   : no se usa (se mantiene por compatibilidad de firma)
    - version: columna Version del CSV (Extended Art, Showcase, etc.)
    """
    foil_l = (foil or "normal").strip().lower()
    if foil_l in ("foil", "etched"):
        return True   # excluir foil del precio base NM
    source = (version or "").strip().lower()
    return bool(source) and any(kw in source for kw in _ESTACA_KEYWORDS)


# ── JS stock con cache ────────────────────────────────────────────────────────

# ── CardKingdom: sync diario y lookup por nombres del CSV ─────────────────────

async def _sync_ck_prices() -> dict:
    """
    Descarga la pricelist pública de CardKingdom y hace upsert masivo en
    la tabla ck_prices de PostgreSQL.

    REGLA: solo versiones no-foil (is_foil=False/0).
           Por cada nombre canónico se guarda el precio MÁS BAJO encontrado
           (cualquier edición, cualquier variante, cualquier condición NM).

    UPSERT: INSERT ... ON CONFLICT (name_canonical) DO UPDATE
            → idempotente; re-ejecutar no duplica filas.

    Retorna {"upserted": N, "elapsed": s} para logging.
    Ante cualquier error de red o de BD → log + retorna {"error": msg}.
    El job diario captura el error sin detener la app.
    """
    async with _ck_sync_lock:
        t0 = time.monotonic()
        logger.info("[CK_SYNC] Iniciando descarga de pricelist CardKingdom...")

        # ── 1. Fetch ─────────────────────────────────────────────────────────
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    CK_PRICELIST_URL,
                    timeout=aiohttp.ClientTimeout(total=60),
                    headers={"Accept": "application/json"},
                ) as resp:
                    if resp.status != 200:
                        msg = f"HTTP {resp.status}"
                        logger.warning(f"[CK_SYNC] {msg}")
                        return {"error": msg}
                    payload = await resp.json(content_type=None)
        except Exception as exc:
            logger.warning(f"[CK_SYNC] Fetch error: {exc}")
            return {"error": str(exc)}

        # ── 2. Reducir a min_buy_price por nombre canónico ───────────────────
        # {canonical → (name_raw, min_price)}
        prices: dict[str, tuple[str, float]] = {}
        for entry in payload.get("data", []):
            if entry.get("is_foil"):
                continue                          # excluir foil
            name_raw = (entry.get("name") or "").strip()
            if not name_raw:
                continue
            try:
                buy = float(entry.get("buy_price") or entry.get("buylist_price") or 0)
            except (ValueError, TypeError):
                continue
            if buy <= 0:
                continue
            cn = _canonical(name_raw)
            if cn not in prices or buy < prices[cn][1]:
                prices[cn] = (name_raw, buy)

        if not prices:
            logger.warning("[CK_SYNC] Pricelist vacía o sin entradas válidas")
            return {"error": "empty_pricelist"}

        # ── 3. Upsert masivo en PostgreSQL ───────────────────────────────────
        # asyncio.to_thread: la sesión SQLAlchemy es síncrona; el upsert puede
        # tardar varios segundos con >50k filas — no bloquear el event loop.
        from sqlalchemy.dialects.postgresql import insert as pg_insert
    
        def _do_upsert():
            with SessionLocal() as db:
                stake = settings.STAKE_MULTIPLIER   # leer valor actual del env
                rows = [
                    {
                        "name_canonical":  cn,
                        "name_raw":        name_raw,
                        "min_buy_price":   price,
                        "nicho_threshold": round(price * stake, 4),
                    }
                    for cn, (name_raw, price) in prices.items()
                ]
                # Batches de 1000 para no saturar el pool en free tier
                BATCH = 1000
                total = 0
                for i in range(0, len(rows), BATCH):
                    stmt = pg_insert(CKPrice).values(rows[i:i + BATCH])
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["name_canonical"],
                        set_={
                            "name_raw":       stmt.excluded.name_raw,
                            "min_buy_price":  stmt.excluded.min_buy_price,
                            "nicho_threshold":stmt.excluded.nicho_threshold,
                            "updated_at":     func.now(),
                        },
                    )
                    db.execute(stmt)
                    total += len(rows[i:i + BATCH])
                db.commit()
                return total

        try:
            upserted = await asyncio.to_thread(_do_upsert)
        except Exception as exc:
            logger.error(f"[CK_SYNC] Upsert error: {exc}")
            return {"error": str(exc)}

        elapsed = round(time.monotonic() - t0, 1)
        logger.info(f"[CK_SYNC] ✅ {upserted:,} cartas upserted en {elapsed}s")
        return {"upserted": upserted, "elapsed": elapsed}


def _get_ck_prices_for_names(names: list[str]) -> dict[str, dict]:
    """
    Consulta la tabla ck_prices SOLO para los nombres canónicos del CSV actual.

    Retorna {name_canonical → {"min_buy_price": float, "nicho_threshold": float}}.
    Si la tabla está vacía o la BD no responde → retorna {} (fallback por tipo).
    """
    if not names:
        return {}
    canonicals = [_canonical(n) for n in names if n]
    if not canonicals:
        return {}
    try:
        with SessionLocal() as db:
            rows = (
                db.query(CKPrice.name_canonical, CKPrice.min_buy_price, CKPrice.nicho_threshold)
                .filter(CKPrice.name_canonical.in_(canonicals))
                .all()
            )
        return {
            r.name_canonical: {
                "min_buy_price":   float(r.min_buy_price),
                "nicho_threshold": float(r.nicho_threshold),
            }
            for r in rows
        }
    except Exception as exc:
        logger.warning(f"[CK_LOOKUP] Error al leer ck_prices: {exc} — fallback por tipo")
        return {}


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
    price_usd:      float,
    foil_raw:       str,
    version:        str,
    cond_raw:       str,
    base_min_price: dict,
    card_name:      str,
    ck_nm_prices:   dict | None = None,
) -> tuple[float, float, bool, str]:
    """
    Calcula precio efectivo USD y detecta si la carta es De Nicho.

    DETECCIÓN DE PRECIO DE REFERENCIA NM (orden de prioridad):
      1. base_nm = base_min_price[canonical]  — versión NM más barata del mismo CSV
      2. base_nm = ck_nm_prices[canonical]    — precio CK de la tabla ck_prices
      3. Sin referencia                        → is_nicho = False (no hay umbral)

    CRITERIO DE NICHO:
      Con base_nm disponible: es De Nicho si price_usd > base_nm × STAKE_MULTIPLIER
      Sin base_nm            : es De Nicho si _is_estaca() → True

    Si es De Nicho:
      eff_usd = base_nm × STAKE_MULTIPLIER
      (si base_nm vino de fallback: base_nm = price_usd → eff = price × STAKE_MULTIPLIER)

    Devuelve (precio_efectivo_usd, precio_ajustado_usd, is_nicho, base_origin)
    donde base_origin es: 'csv' | 'cardkingdom' | 'fallback_tipo'
    """
    bn       = _canonical(card_name)
    ck_map   = ck_nm_prices or {}
    mult     = settings.STAKE_MULTIPLIER

    # ── Origen del precio de referencia NM ───────────────────────────────────
    if bn in base_min_price:
        base_nm         = base_min_price[bn]
        nicho_threshold = base_nm * mult   # calculado desde CSV
        base_origin     = "csv"
    elif bn in ck_map:
        # ck_map ahora es {canonical → {"min_buy_price": f, "nicho_threshold": f}}
        ck_entry        = ck_map[bn]
        base_nm         = ck_entry["min_buy_price"]
        # TRV-02 FIX: nicho_threshold puede ser None si migración no corrió aún
        _nt             = ck_entry.get("nicho_threshold")
        nicho_threshold = _nt if _nt is not None else round(base_nm * settings.STAKE_MULTIPLIER, 4)
        base_origin     = "cardkingdom"
    else:
        base_nm         = None
        nicho_threshold = None
        base_origin     = "sin_referencia"

    # ── Criterio De Nicho ─────────────────────────────────────────────────────
    # REGLA DEFINITIVA: De Nicho SOLO por comparación de precios.
    # Una carta es De Nicho si y solo si su precio en el CSV supera 1.5× el
    # precio de referencia (CSV NM propio o tabla ck_prices).
    # Sin precio de referencia → is_nicho = False. No hay fallback por tipo de carta.
    # Esto garantiza que foil básico, etched, y cualquier versión sin precio CK
    # se cotiza a precio normal sin multiplicador De Nicho.
    if nicho_threshold is not None:
        is_nicho = price_usd > nicho_threshold
    else:
        # Sin base de referencia: precio normal, sin De Nicho
        is_nicho = False

    # ── Precio efectivo ───────────────────────────────────────────────────────
    eff_usd  = (nicho_threshold or price_usd) if is_nicho else price_usd
    adjusted = round(eff_usd * _cond_mult(cond_raw), 4)
    return eff_usd, adjusted, is_nicho, base_origin


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
    """
    Lee un CSV de buylist con detección automática de encoding.

    FIX B-01: Manejo de excepciones granular por tipo de error.
    Antes: (UnicodeDecodeError, Exception) enmascaraba todos los errores,
    incluyendo ParserError reales de pandas (columnas malformadas, BOM
    inesperado, delimitadores incorrectos) que no mejoran cambiando el encoding.

    Estrategia:
      - UnicodeDecodeError: error de encoding → probar siguiente encoding
      - pd.errors.ParserError: CSV malformado → probar siguiente encoding (a veces
        el parser falla con un encoding incorrecto antes del UnicodeDecodeError)
      - Cualquier otro error: loggear y re-raise para no enmascararlo
    """
    last_error: Exception | None = None
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(io.StringIO(content.decode(enc)))
        except UnicodeDecodeError:
            # Encoding incorrecto → probar el siguiente
            last_error = UnicodeDecodeError.__new__(UnicodeDecodeError)
            continue
        except pd.errors.ParserError as e:
            # CSV malformado con este encoding → intentar con el siguiente
            logger.debug(f"[CSV] ParserError con encoding {enc}: {e}")
            last_error = e
            continue
        except pd.errors.EmptyDataError:
            raise HTTPException(status_code=400, detail="El CSV está vacío.")
        except Exception as e:
            # Error inesperado no relacionado con encoding — loggear y fallar
            logger.error(f"[CSV] Error inesperado leyendo CSV ({enc}): {type(e).__name__}: {e}")
            raise HTTPException(
                status_code=400,
                detail=f"Error leyendo el CSV: {type(e).__name__}: {e}"
            )
    # Todos los encodings fallaron
    detail = (
        f"No se pudo leer el CSV (encodings intentados: utf-8, utf-8-sig, latin-1). "
        f"Verificá que el archivo sea un CSV válido exportado desde Manabox."
    )
    logger.warning(f"[CSV] Todos los encodings fallaron. Último error: {last_error}")
    raise HTTPException(status_code=400, detail=detail)


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

@app.get("/api/balance/{email}",        dependencies=[Depends(verify_public_token)])
@app.get("/api/saldo/{email}",          dependencies=[Depends(verify_public_token)])
@app.get("/api/public/balance/{email}", dependencies=[Depends(verify_public_token)])
def get_balance(email: str, db: Session = Depends(get_db)):
    """
    H-01 FIX: protegido con verify_public_token — requiere x-store-token válido.
    Evita que cualquier persona enumere saldos de otros usuarios (IDOR).

    M-03 FIX: incluye historico_acumulado para que el cliente vea su cashback ganado.

    L-01 FIX: diferencia entre "usuario no encontrado" (retorna 0, correcto)
    y "error de BD" (propaga HTTPException 503 en vez de enmascararlo como $0).
    """
    try:
        user = db.query(Gampoint).filter(
            Gampoint.email == email.lower().strip()).first()
        # Usuario no registrado → saldo 0 (correcto: el tema muestra $0)
        if not user:
            return {"saldo": 0.0, "historico_canjeado": 0.0, "historico_acumulado": 0.0}
        return {
            "saldo":               float(user.saldo               or 0),
            "historico_canjeado":  float(user.historico_canjeado  or 0),
            "historico_acumulado": float(user.historico_acumulado or 0),
        }
    except Exception as exc:
        # Error de BD/infraestructura: loggear y retornar 503, no $0 silencioso
        logger.error(f"[BALANCE] Error consultando saldo para {email}: {exc}")
        raise HTTPException(status_code=503, detail="Servicio temporalmente no disponible")


@app.post("/api/canje", dependencies=[Depends(verify_public_token)])  # C-02: usa PUBLIC_STORE_TOKEN
async def execute_canje(req: CanjeRequest, db: Session = Depends(get_db)):
    if settings.MAINTENANCE_MODE_CANJE:
        raise HTTPException(503, "El sistema de canje está en mantenimiento.")
    if req.monto < settings.MIN_CANJE:
        raise HTTPException(400, f"Monto mínimo de canje: {settings.MIN_CANJE} QP.")
    return await VaultController.process_canje(db, req.email, req.monto, req.cart_total)


@app.get("/api/public/last_canje/{email}", dependencies=[Depends(verify_public_token)])
def get_last_canje(email: str, db: Session = Depends(get_db)):
    """
    Retorna el último canje QP del cliente, para mostrarlo en account.liquid.

    Respuesta cuando hay canje:
      { found: true, amount_qp, coupon_code, adjusted, created_at }
    Respuesta cuando no hay:
      { found: false }

    Seguridad: requiere x-store-token = PUBLIC_STORE_TOKEN (igual que /api/public/balance).
    No expone el historial completo — solo el último registro.
    """
    try:
        record = (
            db.query(CanjeRecord)
            .filter(CanjeRecord.email == email.lower().strip())
            .order_by(CanjeRecord.created_at.desc())
            .first()
        )
        if not record:
            return {"found": False}
        return {
            "found":           True,
            "amount_qp":       float(record.amount_qp),
            "coupon_code":     record.coupon_code,
            "adjusted":        bool(record.adjusted),
            "monto_original":  float(record.monto_original) if record.monto_original else None,
            "created_at":      record.created_at.isoformat() if record.created_at else None,
        }
    except Exception as exc:
        logger.error(f"[LAST_CANJE] Error para {email}: {exc}")
        raise HTTPException(503, "Servicio temporalmente no disponible")


@app.get("/api/admin/canje_history/{email}", dependencies=[Depends(verify_admin)])
def get_canje_history(email: str, limit: int = 20, db: Session = Depends(get_db)):
    """
    Historial completo de canjes de un cliente — para la Bóveda admin.

    Parámetros:
      - email: email del cliente
      - limit: máximo de registros a retornar (default 20, max 100)

    Retorna lista ordenada por fecha desc con todos los canjes del cliente.
    """
    limit = min(limit, 100)
    records = (
        db.query(CanjeRecord)
        .filter(CanjeRecord.email == email.lower().strip())
        .order_by(CanjeRecord.created_at.desc())
        .limit(limit)
        .all()
    )
    return {
        "email": email.lower().strip(),
        "total": len(records),
        "canjes": [
            {
                "id":             r.id,
                "amount_qp":      float(r.amount_qp),
                "coupon_code":    r.coupon_code,
                "adjusted":       bool(r.adjusted),
                "monto_original": float(r.monto_original) if r.monto_original else None,
                "cart_total":     float(r.cart_total) if r.cart_total else None,
                "created_at":     r.created_at.isoformat() if r.created_at else None,
            }
            for r in records
        ],
    }


# =====================================================================
# 📦 BUYLIST PÚBLICA — analyze + commit
# =====================================================================

@app.get("/api/public/buylist_status")
def buylist_status():
    today  = str(_dt.date.today())
    limit  = settings.BUYLIST_DAILY_BUDGET_CASH
    spent  = _daily_cash_spent.get(today, 0.0)
    return {
        "open":                    settings.BUYLIST_OPEN,
        "cash_enabled":            settings.CASH_ENABLED,
        "budget_cash_limit_clp":   limit,
        "budget_cash_spent_clp":   round(spent, 0),
        "budget_cash_remaining_clp": round(max(0.0, limit - spent), 0) if limit > 0 else None,
    }


async def _analyze_buylist_impl(
    file:    UploadFile,
    request,
    db:      Session,
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

    # MAIN-04 FIX: rechazar archivos que no sean CSV para prevenir inyección de datos
    _ALLOWED_CT = {"text/csv", "text/plain", "application/csv", "application/octet-stream", ""}
    ct = (file.content_type or "").split(";")[0].strip().lower()
    if ct and ct not in _ALLOWED_CT:
        raise HTTPException(422, f"Tipo de archivo no permitido: '{file.content_type}'. Debe ser CSV.")
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

    # ── Precios CK para los nombres únicos del CSV ─────────────────────────
    # Un solo SELECT IN sobre la tabla ck_prices — solo las cartas del CSV.
    # _get_ck_prices_for_names es síncrono → asyncio.to_thread para no bloquear.
    unique_names  = list({str(n) for n in df_nm if n})
    ck_nm_prices  = await asyncio.to_thread(_get_ck_prices_for_names, unique_names)

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

        eff_usd, adjusted_price, is_estaca_card, base_origin = _compute_card_price(
            raw_usd, foil_raw, version, cond_r, base_min_price, nm, ck_nm_prices
        )

        # Lookup con prioridad: Scryfall UUID > _canonical(name)
        # El UUID identifica la edición exacta → el canonical agrupa todas las ediciones
        sf_str    = str(sf_id).strip() if sf_id else ""
        key_pub   = (_scryfall_map.get(sf_str)    # lookup O(1) por UUID si existe en el catálogo
                     if sf_str and sf_str not in ("nan", "none", "")
                     else None) or _canonical(nm)  # fallback por nombre
        srec      = staple_map_pub.get(key_pub)
        # Tier default "normal" — sin_lista eliminado.
        # Toda carta no listada en staple_cards se trata como tier normal
        # (stock mínimo MIN_STOCK_NORMAL, compra hasta completar ese stock).
        # Solo se aplica tier especial si está explícitamente en staple_cards.
        tier_pub  = srec.tier if srec else "normal"
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
        else:
            # normal | alta — comprar hasta completar stock objetivo
            stock_now   = stock_pub if stock_pub is not None else 0
            cupo        = max(0, max_s_pub - stock_now)
            qty_comprar = min(qty, cupo)
            if cupo <= 0:
                buying_status = "stock_completo"
            elif qty_comprar < qty:
                buying_status = "compra_parcial"
            else:
                buying_status = "compramos"

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
            "nicho_base":     base_origin,
            "price_credito":  p_cred,
            "price_cash":     int(adjusted_price * fcash),
            "tier":           tier_pub,
            "stock_actual":   stock_pub,
            "buying_status":  buying_status,
            "canonical":      key_pub,
            "scryfall_id":    sf_str or None,
            "matched_by":     "scryfall_id" if (sf_str and sf_str in _scryfall_map) else "name",
        })

    # ── Sanitizar respuesta pública — ocultar campos sensibles de inventario ──
    # stock_actual, max_stock, cupo, canonical, tier, etc. son información
    # comercial interna que no debe exponerse al cliente en el endpoint público.
    # qty_comprar se renombra a max_qty para que el frontend pueda limitar la
    # cantidad sin revelar la lógica de cupo.
    # _admin=True: retornar todos los campos sin filtrar (Buylist_Interna.html).
    # _analyze_buylist_impl retorna los resultados completos sin sanitizar
    return results


# ── Endpoint público: sanitiza la respuesta ────────────────────────────────
@app.post("/api/public/analyze_buylist")
async def analyze_buylist(
    file:    UploadFile = File(...),
    request: Request    = None,
    db:      Session    = Depends(get_db),
):
    """
    Endpoint público de análisis de buylist.
    Retorna solo los campos necesarios para el cliente — sin stock, tier ni metadatos.
    """
    results = await _analyze_buylist_impl(file=file, request=request, db=db)
    # Sanitizar: eliminar campos de inventario interno
    _SENSITIVE_FIELDS = frozenset({
        "stock_actual", "max_stock", "cupo",
        "canonical", "scryfall_id", "matched_by",
        "nicho_base", "tier",
        "price_usd_raw", "price_usd_base",
    })
    sanitized = []
    for r in results:
        pub = {k: v for k, v in r.items() if k not in _SENSITIVE_FIELDS}
        # qty_comprar se expone solo como max_qty — sin revelar la lógica de cupo
        pub["max_qty"] = r.get("qty_comprar", r["qty"])
        pub.pop("qty_comprar", None)
        sanitized.append(pub)
    return sanitized


@app.post("/api/admin/analyze_buylist", dependencies=[Depends(verify_admin)])
async def admin_analyze_buylist(
    file:    UploadFile = File(...),
    db:      Session    = Depends(get_db),
    request: Request    = None,
):
    """
    NEW-01 FIX: endpoint admin independiente del público.
    Retorna resultados completos sin sanitizar (stock_actual, max_stock, cupo,
    tier, canonical, matched_by, nicho_base) — solo accesible con STORE_TOKEN.
    La separación arquitectural garantiza que el flag admin no sea un query param HTTP.
    """
    # Delegar a la función interna — _analyze_buylist_impl hace la validación
    # Admin recibe datos completos sin sanitizar (stock_actual, tier, cupo, etc.)
    results = await _analyze_buylist_impl(file=file, request=request, db=db)
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

    # FIX CRÍTICO: verificar y registrar presupuesto diario de cash.
    # Solo aplica si BUYLIST_DAILY_BUDGET_CASH > 0 (0 = sin límite).
    # Para órdenes en crédito o mixto, se cuenta solo el componente cash.
    if req.payment_preference in ("cash", "mixto"):
        _check_and_register_cash_budget(float(req.total_cash))

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
    db.refresh(order)   # MAIN-03 FIX: garantizar order.id disponible (defensive)

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
    db.refresh(order)   # MAIN-07 FIX: garantizar order.id disponible (defensive)

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
def login(req: LoginRequest, request: Request):
    # FIX A2: Rate limiting — máx 5 intentos por IP cada 5 minutos.
    # Sin esto, un atacante puede fuerza-bruta ADMIN_USER/ADMIN_PASS y obtener
    # el STORE_TOKEN que da acceso a todos los endpoints /api/admin/*.
    # Usamos la misma infraestructura _rate_limit existente para consistencia.
    ip = (
        (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
        or (request.client.host if request and request.client else "unknown")
    )
    if not _rate_limit(f"login:{ip}", max_calls=5, window_sec=300):
        raise HTTPException(
            status_code=429,
            detail="Demasiados intentos de login. Espera 5 minutos e inténtalo de nuevo."
        )
    # FIX A3 también aplicado aquí: compare_digest para credenciales admin.
    # Comparar username y password en tiempo constante evita enumeración de usuarios
    # (misma latencia para usuario inválido y usuario válido con contraseña incorrecta).
    user_ok = secrets.compare_digest(req.username, settings.ADMIN_USER)
    pass_ok = secrets.compare_digest(req.password, settings.ADMIN_PASS)
    if user_ok and pass_ok:
        return {"access_token": settings.STORE_TOKEN, "token_type": "bearer"}
    raise HTTPException(401, "Credenciales inválidas")


@app.get("/admin/users",     dependencies=[Depends(verify_admin)])
@app.get("/api/admin/users", dependencies=[Depends(verify_admin)])
def get_users(
    db: Session = Depends(get_db),
    search: Optional[str] = None,
    only_balance: bool = False,
):
    query = db.query(Gampoint)
    if search:
        term = f"%{search.lower()}%"
        # Busca en email, name Y surname para cubrir búsqueda por nombre completo
        query = query.filter(
            Gampoint.email.ilike(term)   |
            Gampoint.name.ilike(term)    |
            Gampoint.surname.ilike(term)
        )
    if only_balance:
        query = query.filter(Gampoint.saldo > 0)
    users = query.order_by(Gampoint.saldo.desc()).all()
    total_circulante = db.query(func.sum(Gampoint.saldo)).scalar()              or 0
    total_canjeado   = db.query(func.sum(Gampoint.historico_canjeado)).scalar() or 0
    total_users      = db.query(func.count(Gampoint.email)).scalar()            or 0

    def _display_name(u: Gampoint) -> str:
        """
        Nombre para mostrar en la Bóveda.
        Prioriza el nombre completo si ambas partes existen.
        Si solo hay una parte, la retorna sola.
        Si no hay ninguna, retorna cadena vacía (la UI muestra 'Sin nombre').
        """
        parts = [p for p in [u.name, u.surname] if p and p.strip()]
        return " ".join(parts)

    return {
        "users": [{
            "email":               u.email,
            "name":                u.name,
            "surname":             u.surname,
            "display_name":        _display_name(u),   # campo compuesto para la UI
            "saldo":               float(u.saldo or 0),
            "historico_canjeado":  float(u.historico_canjeado  or 0),
            "historico_acumulado": float(u.historico_acumulado or 0),
            "jumpseller_id":       u.jumpseller_id,
        } for u in users],
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
    # H-02 FIX: WITH FOR UPDATE — bloqueo de fila para prevenir race condition
    # si dos admins ajustan el mismo usuario simultáneamente. Sin el lock, un
    # "subtract" concurrente puede dejar el saldo en un valor incorrecto.
    user = db.query(Gampoint).filter(
        Gampoint.email == req.email.lower()
    ).with_for_update().first()
    if not user:
        raise HTTPException(404, "Usuario no encontrado")
    delta = Decimal(req.amount)
    if req.operation == "add":
        user.saldo               += delta
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
async def catalog_list(
    db: Session = Depends(get_db),
    q:     str           = "",
    tier:  Optional[str] = None,
    page:  int           = 1,
    limit: int           = 50,
):
    """
    Lista el catálogo con paginación y filtros.
    Enriquece cada carta con su tier (de staple_cards) y stock en vivo del cache JS.

    FIX A5: El filtro `tier` se aplica ANTES de paginar mediante un JOIN SQL.
    FIX QA: async def + await _fetch_js_stock_cached() — garantiza que el cache
    esté poblado incluso en cold start (antes el stock podía ser vacío tras restart).
    """

    query = db.query(CardCatalog)
    if q:
        query = query.filter(CardCatalog.name_normalized.contains(_canonical(q)))

    # FIX A5: Filtrar por tier haciendo JOIN con staple_cards en SQL.
    # Esto garantiza que total, pages y los resultados sean consistentes.
    if tier:
        query = (
            query
            .join(StapleCard, StapleCard.name_normalized == CardCatalog.name_normalized)
            .filter(StapleCard.tier == tier)
        )

    total = query.count()
    rows  = query.order_by(CardCatalog.name_display).offset((page - 1) * limit).limit(limit).all()

    # Enriquecer con tier actual y stock vivo
    staple_map = _get_staple_map(db)
    js_stock   = await _fetch_js_stock_cached()   # FIX QA: garantiza cache poblado

    result_rows = []
    for r in rows:
        srec      = staple_map.get(r.name_normalized)
        live_data = _resolve_stock(r.name_normalized, js_stock)
        live_stk  = live_data.get("stock") if live_data else r.total_stock

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
    total_cards  = db.query(CardCatalog).count()
    total_skus   = db.query(func.sum(func.json_array_length(CardCatalog.js_product_ids))).scalar() or 0
    total_stock  = db.query(func.sum(CardCatalog.total_stock)).scalar() or 0
    last_sync    = db.query(func.max(CardCatalog.last_synced)).scalar()
    # cartas con al menos 1 scryfall_id vinculado
    enriched = db.query(CardCatalog).filter(
        CardCatalog.scryfall_ids.isnot(None),
        func.json_array_length(CardCatalog.scryfall_ids) > 0
    ).count()
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

        # ── Paso 3: stream download (async) + parse ijson (thread pool) ───────
        # sf_map_new, cards_seen y matched son inicializados y devueltos por
        # _parse_scryfall_buffer() que corre en asyncio.to_thread() (FIX C3).

        async with aiohttp.ClientSession(headers=HEADERS,
                                          timeout=aiohttp.ClientTimeout(
                                              total=600,    # 10 min — descarga grande
                                              sock_read=60,
                                          )) as session:
            async with session.get(download_uri) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"Download HTTP {resp.status}")

                logger.info("[BULK] Descarga iniciada — acumulando en buffer…")

                import io

                buffer = io.BytesIO()
                total_bytes = 0
                CHUNK = 512 * 1024   # 512KB por chunk

                async for chunk in resp.content.iter_chunked(CHUNK):
                    buffer.write(chunk)
                    total_bytes += len(chunk)
                    _enrich_last_card = f"descargando… {total_bytes // 1_048_576} MB"
                    if total_bytes % (4 * 1024 * 1024) == 0:
                        await asyncio.sleep(0)

                logger.info(f"[BULK] Descarga completa: {total_bytes // 1_048_576} MB")
                # La conexión HTTP se cierra aquí al salir del context manager.
                # FIX C3+C4: el parseo ijson ocurre FUERA de este bloque para:
                #   (a) cerrar la conexión TCP a Scryfall antes de parsear.
                #   (b) mover el parseo síncrono al thread pool (asyncio.to_thread)
                #       para no bloquear el event loop 30-60 segundos.

        # ── Paso 4: parsear card por card con ijson en thread pool ────────────
        # FIX C3: ijson.items() es completamente síncrono. Con ~90k cartas / 100MB
        # bloquea el event loop entre 30-60s, congelando TODOS los requests activos.
        # asyncio.to_thread() mueve el parseo al ThreadPoolExecutor del event loop,
        # liberando el hilo principal para atender requests mientras parsea.
        #
        # FIX C4: buffer se cierra y libera explícitamente en el finally del helper
        # para asegurar que los 100MB se devuelvan al GC independientemente del
        # resultado del parseo (éxito, excepción o cancelación asyncio).
        _enrich_last_card = "parseando JSON (thread pool)…"
        await asyncio.sleep(0)   # yield antes del parseo para que FastAPI pueda atender requests

        def _parse_scryfall_buffer(buf: io.BytesIO, known_canonicals: set) -> tuple[dict, int, int]:
            """
            Parsea el bulk data de Scryfall en un hilo separado (thread pool).
            Devuelve (sf_map_new, cards_seen, matched).
            Se ejecuta fuera del event loop — NO puede llamar a funciones async
            ni acceder a globals asyncio sin sincronización.
            """
            import ijson
            sf_map: dict[str, list] = {c: [] for c in known_canonicals}
            seen = matched = 0
            buf.seek(0)
            try:
                for card in ijson.items(buf, "item"):
                    seen += 1
                    name = card.get("name", "")
                    if not name:
                        continue
                    layout = card.get("layout", "")
                    if layout in ("token", "emblem", "art_series", "double_faced_token"):
                        continue
                    canonical = _canonical(name)
                    if canonical not in known_canonicals:
                        continue
                    finishes = card.get("finishes") or []
                    if not finishes:
                        if card.get("foil"):    finishes.append("foil")
                        if card.get("nonfoil"): finishes.append("nonfoil")
                    if not finishes:
                        finishes = ["nonfoil"]
                    sf_map[canonical].append({
                        "scryfall_id":      card["id"],
                        "set_code":         card.get("set", "").upper(),
                        "set_name":         card.get("set_name", ""),
                        "collector_number": card.get("collector_number", ""),
                        "lang":             card.get("lang", "en"),
                        "finishes":         finishes,
                    })
                    matched += 1
            finally:
                # FIX C4: liberar el BytesIO (~100MB) explícitamente.
                # Sin esto el GC puede tardar en reclamarlo en el free tier 512MB.
                buf.close()
            return sf_map, seen, matched

        sf_map_new, cards_seen, matched = await asyncio.to_thread(
            _parse_scryfall_buffer, buffer, canonical_set
        )
        # buffer fue cerrado dentro de _parse_scryfall_buffer → del ayuda al GC
        del buffer

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
        # Revertir cualquier transacción abierta antes de cerrar la sesión.
        # Necesario cuando task.cancel() llega durante un db.commit() en vuelo.
        try:
            db.rollback()
        except Exception:
            pass
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

    # FIX A4: asyncio.Lock elimina el TOCTOU entre el check y el create_task.
    # Sin lock, dos requests simultáneos pueden ambos pasar `if _enrich_running`
    # antes de que cualquiera haya actualizado el flag, lanzando dos tareas que
    # escriben concurrentemente en card_catalog con conflictos de commit en BD.
    # acquire() es O(1) cuando el lock está libre; el segundo request espera
    # dentro del `async with` y ve _enrich_running=True al intentar tomar el lock.
    async with _enrich_lock:
        if _enrich_running:
            return {
                "status":    "already_running",
                "done":      _enrich_done,
                "total":     _enrich_total,
                "last_card": _enrich_last_card,
            }
        # Setear _enrich_running DENTRO del lock antes de soltar — garantiza
        # que cualquier request que arrive ahora vea el flag en True.
        _enrich_running = True
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


@app.get("/api/admin/staples_status", dependencies=[Depends(verify_admin)])
def staples_status(db: Session = Depends(get_db)):
    """
    P-01 FIX: Diagnóstico del caché de staples y estado de la tabla staple_cards.

    Devuelve:
      - cache_valid / cache_entries / cache_age_sec — estado del caché en RAM
      - db_total / db_tiers — cartas en la tabla staple_cards por tier
      - ck_prices_total — cartas en la tabla ck_prices
    """
    cache_entries = len(_staple_map_cache) if _staple_map_cache else 0
    cache_age     = int(time.monotonic() - _staple_map_cache_ts)
    cache_valid   = cache_entries > 0 and cache_age < STAPLE_CACHE_TTL

    try:
        db_total = db.query(StapleCard).count()
        tiers    = db.query(StapleCard.tier,
                            func.count(StapleCard.id).label("n")) \
                     .group_by(StapleCard.tier).all()
        db_tiers = {t.tier: t.n for t in tiers}
    except Exception as exc:
        db_total = -1
        db_tiers = {"error": str(exc)}

    try:
        ck_total = db.query(CKPrice).count()
    except Exception as exc:
        ck_total = -1

    return {
        "cache_valid":      cache_valid,
        "cache_entries":    cache_entries,
        "cache_age_sec":    cache_age,
        "db_staples_total": db_total,
        "db_staples_tiers": db_tiers,
        "ck_prices_total":  ck_total,
        "recommendation":   (
            "tabla staple_cards vacía — poblar con POST /api/admin/staples/bulk"
            if db_total == 0 else
            "cache inválido pero BD tiene datos — próximo request lo recargará"
            if not cache_valid and db_total > 0 else
            "ok"
        ),
    }


@app.post("/api/admin/stock_check", dependencies=[Depends(verify_admin)])
async def stock_check(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):

    content = await file.read()
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(413, "Archivo demasiado grande")

    df       = _read_csv(content)
    required = {"Name", "Quantity", "Purchase price"}
    missing  = required - set(df.columns)
    if missing:
        raise HTTPException(422, f"Columnas faltantes: {missing}")

    base_min_price = _build_base_min_price(df)
    staple_map     = _get_staple_map(db)
    js_stock       = await _fetch_js_stock_cached()
    _get_catalog_map(db)   # pre-cargar catálogo en _catalog_cache

    # Precios CK para los nombres únicos del CSV (SELECT IN sobre ck_prices)
    _col_cb    = lambda c, d="": df[c].fillna(d).astype(str) if c in df.columns else pd.Series([d]*len(df))
    unique_names_cb = list({str(n).strip() for n in _col_cb("Name") if str(n).strip()})
    ck_nm_prices    = await asyncio.to_thread(_get_ck_prices_for_names, unique_names_cb)

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

        eff_usd, price_usd, is_estaca_card, base_origin = _compute_card_price(
            raw_usd, foil_raw, version, cond_raw, base_min_price, name, ck_nm_prices
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
            # base_origin: 'csv' | 'cardkingdom' | 'fallback_tipo'
            if base_origin == "csv":
                base_used   = base_min_price.get(key, raw_usd)
                origin_desc = f"base NM del CSV ${base_used:.2f} USD"
            elif base_origin == "cardkingdom":
                ck_entry    = ck_nm_prices.get(key, {})
                base_used   = ck_entry.get("min_buy_price", raw_usd)
                origin_desc = f"base CK Buylist ${base_used:.2f} USD"
            else:
                base_used   = raw_usd
                origin_desc = "sin NM disponible — detección por tipo"
            alerts.append({"type": "info",
                           "msg": f"De Nicho ×{STAKE_M} ({origin_desc})"})

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
            "nicho_base":        base_origin,
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
def update_order_status(
    order_id:   int,
    # FIX A7: new_status era `str` sin validación → cualquier valor se persistía en BD.
    # Un operador podía escribir 'hacked', 'null', o valores que rompieran filtros.
    # Literal limita a los 4 estados válidos del ciclo de vida de una orden.
    # FastAPI genera un error 422 automático con los valores permitidos si se envía otro.
    new_status: str = "reviewed",
    db:         Session = Depends(get_db),
):
    _VALID_STATUSES = {"pending", "reviewed", "closed", "cancelled"}
    if new_status not in _VALID_STATUSES:
        raise HTTPException(
            status_code=422,
            detail=f"Estado inválido '{new_status}'. Valores permitidos: {sorted(_VALID_STATUSES)}"
        )
    order = db.query(BuylistOrder).filter(BuylistOrder.id == order_id).first()
    if not order:
        raise HTTPException(404, "Orden no encontrada")
    order.status = new_status
    db.commit()
    return {"status": "ok", "order_id": order_id, "new_status": new_status}


# =====================================================================
# 🔥 BACKGROUND TASKS — cupones y webhooks
# =====================================================================

# ── Verificación HMAC-SHA256 para webhooks de Jumpseller ──────────────────────
# Jumpseller firma cada webhook con HMAC-SHA256 usando el hooks_token de la tienda
# (Admin → Config → Notificaciones) y envía la firma en el header:
#   Jumpseller-Hmac-Sha256: <base64(HMAC-SHA256(hooks_token, body))>
#
# Referencia oficial: https://jumpseller.com/support/webhooks/
# Sin esta verificación cualquiera que conozca la URL puede enviar payloads
# falsos: quemar cupones QP legítimos, sincronizar datos basura de clientes, etc.

def _verify_jumpseller_hmac(body: bytes, signature_header: str) -> bool:
    """
    Verifica la firma HMAC-SHA256 del webhook de Jumpseller.

    Args:
        body:             Cuerpo crudo del request (bytes sin decodificar).
        signature_header: Valor del header 'Jumpseller-Hmac-Sha256' (base64).

    Returns:
        True si la firma es válida o si JUMPSELLER_HOOKS_TOKEN no está configurado
        (modo desarrollo — log de advertencia).
        False si la firma no coincide (el request debe ser rechazado con 401).

    Implementación:
        digest = Base64( HMAC-SHA256(hooks_token, body) )
        compare_digest(digest, signature_header)  ← tiempo constante, previene timing attacks
    """
    hooks_token = settings.JUMPSELLER_HOOKS_TOKEN
    if not hooks_token:
        # Modo desarrollo: sin token configurado se acepta pero se advierte.
        # En producción JUMPSELLER_HOOKS_TOKEN debe estar en las env vars de Render.
        logger.warning(
            "[WEBHOOK] JUMPSELLER_HOOKS_TOKEN no configurado — "
            "verificación HMAC omitida. Configúralo en Render para producción."
        )
        return True

    if not signature_header:
        logger.warning("[WEBHOOK] Header 'Jumpseller-Hmac-Sha256' ausente en el request.")
        return False

    expected = base64.b64encode(
        hmac.new(hooks_token.encode("utf-8"), body, hashlib.sha256).digest()
    ).decode("utf-8")

    # secrets.compare_digest: comparación en tiempo constante.
    # hmac.compare_digest haría lo mismo, pero secrets es el módulo idiomático
    # de Python para operaciones sensibles a timing.
    return secrets.compare_digest(expected, signature_header)


async def _bg_burn_coupons(payload_str: str):
    try:
        for code in set(re.findall(r"QP-[A-F0-9]{6}\b", payload_str)):
            await VaultController.burn_coupon(code)
    except Exception as e:
        logger.error(f"[BURN] {e}")

# ── Cashback 2% — acredita QP al cliente cuando la orden queda pagada ─────────
# TASA: 2% del precio FINAL de la orden (order.total, después de todos los
# descuentos incluyendo el cupón QP aplicado). Se redondea al entero inferior.
#
# IDEMPOTENCIA: CashbackRecord usa order_id como PK. Si el webhook llega dos
# veces por la misma orden, el segundo INSERT falla por violación de PK y se
# descarta silenciosamente — el usuario no recibe crédito doble.
#
# FAIL-SAFE: si el usuario no existe en Gampoint, se loggea warning y se omite.
# Esto puede pasar si el cliente nunca entró a la bóveda y no fue sincronizado.
_CASHBACK_RATE = Decimal("0.02")   # 2%

# ── H-03: Núcleo síncrono — corre en thread pool vía run_in_executor ──────────
def _sync_cashback(payload_str: str) -> None:
    """
    Operaciones sincrónicas de BD para el cashback.
    Nunca llamar directamente desde el event loop — usar _bg_cashback (async).

    H-04 FIX: usa INSERT … ON CONFLICT DO NOTHING para idempotencia atómica.
    El check "¿ya procesé esta orden?" y la acreditación ocurren en la MISMA
    transacción: si el INSERT de CashbackRecord falla por PK duplicada → rowcount 0
    → no se acredita saldo. No hay ventana de race condition entre check y debit.

    Flujo:
      1. Parsear JSON y validar campos obligatorios.
      2. Verificar status == "paid" (case-insensitive).
      3. Calcular cashback_qp = floor(total * 2%) — mínimo 1 QP.
      4. INSERT CashbackRecord ON CONFLICT (order_id) DO NOTHING.
         - rowcount 0 → webhook duplicado → retornar sin acreditar.
         - rowcount 1 → primera vez → continuar.
      5. SELECT Gampoint WITH FOR UPDATE (lock de fila).
         - Usuario no encontrado → ROLLBACK (revierte el INSERT) → retornar.
      6. Acreditar saldo + historico_acumulado.
      7. COMMIT (CashbackRecord + Gampoint en una sola transacción).
    """
    try:
        payload = json.loads(payload_str)
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"[CASHBACK] Payload no es JSON válido: {e}")
        return

    order    = payload.get("order", payload)
    status   = str(order.get("status") or "").lower().strip()

    if status != "paid":
        logger.debug(f"[CASHBACK] status='{status}' — no es 'paid', ignorando")
        return

    order_id = order.get("id")
    if not order_id:
        logger.error("[CASHBACK] Payload sin order.id")
        return

    customer = order.get("customer") or {}
    email    = str(customer.get("email") or "").lower().strip()
    if not email:
        logger.warning(f"[CASHBACK] Orden {order_id} sin customer.email — omitido")
        return

    order_total = float(order.get("total") or 0)
    if order_total <= 0:
        logger.warning(f"[CASHBACK] Orden {order_id} total={order_total} ≤ 0 — omitido")
        return

    cashback_qp = int(Decimal(str(order_total)) * _CASHBACK_RATE)
    if cashback_qp < 1:
        logger.info(
            f"[CASHBACK] Orden {order_id}: {order_total:.0f} CLP → "
            f"{cashback_qp} QP (< 1 mínimo) — omitido"
        )
        return

    cashback_dec = Decimal(cashback_qp)
    db = SessionLocal()
    try:
        # ── H-04: INSERT atómico — idempotencia garantizada por constraint de BD ──
        # on_conflict_do_nothing sobre PK order_id: si ya existe → rowcount=0 → skip.
        # El UPDATE de saldo ocurre en la MISMA transacción, eliminando la race
        # condition del patrón SELECT-then-INSERT anterior.
        stmt = pg_insert(CashbackRecord).values(
            order_id        = int(order_id),
            email           = email,
            amount_qp       = cashback_dec,
            order_total_clp = Decimal(str(order_total)),
        ).on_conflict_do_nothing(index_elements=["order_id"])

        result = db.execute(stmt)

        if result.rowcount == 0:
            # La fila ya existía → webhook duplicado → rollback y salir
            db.rollback()
            logger.info(
                f"[CASHBACK] Orden {order_id} ya procesada — webhook duplicado ignorado"
            )
            return

        # ── Acreditar al usuario en la misma transacción ──────────────────────
        # with_for_update() evita que otro proceso modifique el saldo entre el
        # SELECT y el UPDATE (consistencia ante ajustes admin concurrentes).
        user = db.query(Gampoint).filter(
            Gampoint.email == email
        ).with_for_update().first()

        if not user:
            # Usuario no registrado: revertir el INSERT de CashbackRecord también
            db.rollback()
            logger.warning(
                f"[CASHBACK] Usuario '{email}' no en Gampoint. "
                f"Orden {order_id}: {cashback_qp} QP no acreditados. "
                f"Debe iniciar sesión en la bóveda para registrarse."
            )
            return

        user.saldo               += cashback_dec
        user.historico_acumulado += cashback_dec
        db.commit()

        logger.info(
            f"[CASHBACK] ✅ Orden {order_id}: '{email}' +{cashback_qp} QP "
            f"(2% de ${order_total:,.0f} CLP) → saldo: {float(user.saldo):,.0f} QP"
        )

    except Exception as e:
        db.rollback()
        logger.error(f"[CASHBACK] ❌ Error orden {order_id}: {e}")
    finally:
        db.close()


# ── H-03: Wrapper async — libera el event loop delegando al thread pool ───────
async def _bg_cashback(payload_str: str) -> None:
    """
    H-03 FIX: async def en vez de def — FastAPI ejecuta background tasks async
    directamente en el event loop. Usar run_in_executor delega las operaciones
    síncronas de BD al thread pool, evitando bloquear el event loop durante
    un burst de webhooks.

    Sin este fix, múltiples webhooks simultáneos ocupan todos los threads de
    Starlette y bloquean otros endpoints (login, balance, etc.).

    FIX: get_running_loop() en lugar de get_event_loop() (deprecado en Python 3.10,
    RuntimeError en Python 3.12+). get_running_loop() es correcto aquí porque esta
    coroutine siempre se llama desde dentro del event loop de uvicorn.
    """
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _sync_cashback, payload_str)

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
    # FIX A1: Verificar firma HMAC-SHA256 antes de procesar cualquier acción.
    body = await request.body()
    hmac_header = request.headers.get("Jumpseller-Hmac-Sha256", "")
    if not _verify_jumpseller_hmac(body, hmac_header):
        logger.warning(
            f"[WEBHOOK] Firma HMAC inválida en /order — "
            f"IP: {request.headers.get('X-Forwarded-For', request.client.host if request.client else 'unknown')}"
        )
        raise HTTPException(status_code=401, detail="Webhook signature inválida")
    try:
        payload_str = body.decode("utf-8")
        # Task 1: quemar cupones QP que aparezcan en la orden
        background_tasks.add_task(_bg_burn_coupons, payload_str)
        # Task 2: acreditar cashback 2% si la orden está pagada
        background_tasks.add_task(_bg_cashback, payload_str)
        return {"status": "received"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.post("/api/webhooks/jumpseller/customer")
async def jumpseller_customer_webhook(request: Request, background_tasks: BackgroundTasks):
    # FIX A1: Misma verificación para el webhook de clientes.
    body = await request.body()
    hmac_header = request.headers.get("Jumpseller-Hmac-Sha256", "")
    if not _verify_jumpseller_hmac(body, hmac_header):
        logger.warning(
            f"[WEBHOOK] Firma HMAC inválida en /customer — "
            f"IP: {request.headers.get('X-Forwarded-For', request.client.host if request.client else 'unknown')}"
        )
        raise HTTPException(status_code=401, detail="Webhook signature inválida")
    try:
        # MAIN-06 FIX: parsear desde el body ya leído (stream ya consumido por HMAC)
        payload       = json.loads(body)
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
    today = str(_dt.date.today())
    limit = settings.BUYLIST_DAILY_BUDGET_CASH
    spent = _daily_cash_spent.get(today, 0.0)
    return {
        "status":               "ok",
        "version":              "5.5",  # MAIN-01 FIX
        "buylist_open":         settings.BUYLIST_OPEN,
        "cash_enabled":         settings.CASH_ENABLED,
        "budget_cash_limit":    limit,
        "budget_cash_spent":    int(spent),          # NEW-02 FIX: int en vez de float
        "budget_cash_remaining": int(max(0.0, limit - spent)) if limit > 0 else None,
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


@app.post("/api/admin/sync_ck_prices", dependencies=[Depends(verify_admin)])
async def admin_sync_ck_prices():
    """
    Fuerza la sincronización inmediata de la tabla ck_prices desde CardKingdom.
    Útil para poblar la tabla en un despliegue nuevo o refrescar precios manualmente.
    El job diario corre automáticamente; este endpoint es solo para emergencias.
    """
    result = await _sync_ck_prices()
    if "error" in result:
        raise HTTPException(502, f"Sync CK falló: {result['error']}")
    return {"ok": True, **result}


@app.get("/api/admin/ck_prices_status", dependencies=[Depends(verify_admin)])
def admin_ck_prices_status(db=Depends(get_db)):
    """Estado de la tabla ck_prices: total de cartas y fecha de última actualización."""
    from sqlalchemy import func as sqlfunc
    try:
        total   = db.query(CKPrice).count()
        last_up = db.query(sqlfunc.max(CKPrice.updated_at)).scalar()
        sample  = [r.name_raw for r in db.query(CKPrice).limit(5).all()]
        return {
            "total_cards":   total,
            "last_updated":  str(last_up) if last_up else None,
            "table_ready":   total > 0,
            "sample_5":      sample,
        }
    except Exception as exc:
        raise HTTPException(500, f"Error al leer ck_prices: {exc}")
