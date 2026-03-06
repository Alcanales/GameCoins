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
import unicodedata
import pandas as pd

from collections import defaultdict
from datetime import datetime

from pathlib import Path
from decimal import Decimal
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Header, Request, BackgroundTasks, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
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
app = FastAPI(title="GameCoins API", version="4.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

STATIC_DIR = Path(__file__).parent.parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ── Rate limiting (in-memory, resets on redeploy — suficiente para free tier) ─
_rate_store: dict[str, list] = defaultdict(list)

def _rate_limit(key: str, max_calls: int, window_sec: int) -> bool:
    """True = permitido. False = bloqueado."""
    now  = time.time()
    calls = [t for t in _rate_store[key] if now - t < window_sec]
    if len(calls) >= max_calls:
        return False
    calls.append(now)
    _rate_store[key] = calls
    return True

# ── Cache de stock JS (TTL 5 minutos) ─────────────────────────────────────────
_js_stock_cache: dict = {}
_js_stock_cache_ts: float = 0.0
JS_STOCK_TTL = 600  # 10 min — free tier se duerme 15 min, así siempre hay cache al despertar

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

def _canonical(name: str) -> str:
    """
    Produce la clave normalizada canónica de una carta MTG.

    Maneja todos los casos reales de inconsistencia entre fuentes:
      - Apóstrofes tipográficos (U+2019 ' → U+0027 ')
        "Thassa's Oracle" en JS == "Thassa's Oracle" en CSV  ✓
      - Em-dash / en-dash como separadores de versión
        "Cabal Coffers – Extended | NM" → "cabal coffers"   ✓
      - Paréntesis con versiones
        "Sol Ring (Borderless) | NM" → "sol ring"            ✓
      - Acentos diacríticos
        "Teferi, Héroe de Dominaria" → "teferi, heroe de dominaria" ✓
      - Separador pipe de JS
        "Force of Will | Etched | NM | 2XM" → "force of will" ✓
    """
    if not name:
        return ""
    n = name.strip()
    # 1. Cortar en primer pipe  (formato JS: "Nombre | Idioma | Cond | Set")
    n = n.split("|")[0].strip()
    # 2. Cortar en em-dash / en-dash (separador ad-hoc del admin en JS)
    #    Ningún nombre canónico de Scryfall contiene estos caracteres
    n = re.split(r'[\u2013\u2014\u2015\u2212]', n)[0].strip()
    # 3. Quitar contenido entre paréntesis y corchetes (versiones, sets, idioma)
    n = re.sub(r"[\(\[][^\)\]]*[\)\]]", "", n).strip()
    # 4. Normalizar variantes unicode de apóstrofe → ASCII U+0027
    for ch in _APOSTROPHES:
        n = n.replace(ch, "'")
    # 5. NFKD para acentos diacríticos (â→a, é→e, ñ→n, ü→u, etc.)
    n = unicodedata.normalize("NFKD", n)
    n = "".join(c for c in n if not unicodedata.combining(c))
    # 6. Lowercase + colapsar espacios
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

def _cond_mult(cond_str: str) -> float:
    return {
        "near_mint":         settings.COND_NM,
        "lightly_played":    settings.COND_LP,
        "moderately_played": settings.COND_MP,
        "heavily_played":    settings.COND_HP,
        "damaged":           settings.COND_DMG,
    }.get((cond_str or "near_mint").lower(), settings.COND_NM)


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
    Pre-pase: precio base mínimo de cada carta en el CSV
    (ignora versiones especiales — se usa como referencia para el precio estaca).
    """
    base_min: dict[str, float] = {}
    for _, row in df.iterrows():
        nm  = str(row.get("Name", "")).strip()
        pr  = float(row.get("Purchase price", 0) or 0)
        fo  = str(row.get("Foil",    "normal")).strip().lower()
        ve  = str(row.get("Version", "")).strip()
        cur = str(row.get("Purchase price currency", "USD")).strip().upper()
        if not nm or pr <= 0:
            continue
        if cur == "EUR":
            pr *= 1.10
        if not _is_estaca(fo, nm, ve):
            bn = _canonical(nm)
            if bn not in base_min or pr < base_min[bn]:
                base_min[bn] = pr
    return base_min


async def _fetch_js_stock_cached(force: bool = False) -> dict:
    """
    Descarga catálogo de Jumpseller y agrega stock + precio de venta por nombre base.
    
    CORAZÓN DEL SISTEMA:
    - Clave del dict = nombre base de carta (sin versión/foil/idioma/set/condición)
    - stock = suma de TODAS las versiones en JS (NM, LP, foil, retro frame, etc.)
    - price = precio de venta JS de la variante regular más barata
    - Usa limit=100 para reducir páginas (~213 para 21250 productos)
    - Retry hasta 3 intentos por página para resiliencia en free tier
    """
    global _js_stock_cache, _js_stock_cache_ts
    if not force and time.time() - _js_stock_cache_ts < JS_STOCK_TTL and _js_stock_cache:
        return _js_stock_cache

    logger.info("[JS_STOCK] Cargando catálogo Jumpseller (limit=100, agregando por carta)...")
    url    = f"{settings.JUMPSELLER_API_BASE}/products.json"
    params = {"login": settings.JS_LOGIN_CODE, "authtoken": settings.JS_AUTH_TOKEN,
              "limit": 100, "page": 1}

    raw_cards: dict = {}
    total_prods = 0

    async with aiohttp.ClientSession() as session:
        while True:
            # Retry hasta 3 veces por página (free tier puede tener latencia alta)
            last_err = None
            for attempt in range(3):
                try:
                    async with session.get(url, params=params,
                                           timeout=aiohttp.ClientTimeout(total=20)) as resp:
                        if resp.status == 429:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        if resp.status != 200:
                            last_err = f"HTTP {resp.status}"
                            break
                        data = await resp.json()
                        last_err = None
                        break
                except Exception as e:
                    last_err = str(e)
                    await asyncio.sleep(1)
            else:
                data = []

            if last_err or not data:
                if last_err:
                    logger.warning(f"[JS_STOCK] Página {params['page']}: {last_err} — deteniendo")
                break

            for p in data:
                prod  = p.get("product", p)
                name  = (prod.get("name") or "").strip()
                if not name:
                    continue

                # Stock: sumar todas las variantes del producto
                variants = prod.get("variants", [])
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

                # ── CLAVE: nombre base — independiente de versión, idioma, set, condición ──
                key = _canonical(name)
                if not key:
                    continue

                if key not in raw_cards:
                    raw_cards[key] = {
                        "total_stock": 0,
                        "best_price":  0,
                        "variants":    [],
                        "first_id":    prod.get("id"),
                    }
                raw_cards[key]["total_stock"] += prod_stock
                raw_cards[key]["variants"].append({
                    "name": name, "stock": prod_stock, "price": prod_price,
                    "id": prod.get("id")
                })
                if prod_price > 0 and (raw_cards[key]["best_price"] == 0
                                        or prod_price < raw_cards[key]["best_price"]):
                    raw_cards[key]["best_price"] = prod_price
                total_prods += 1

            if len(data) < 100:
                break
            params["page"] += 1
            await asyncio.sleep(0.05)  # Gentle rate limiting

    products = {
        key: {
            "stock":    v["total_stock"],
            "price":    v["best_price"],   # precio de venta JS (más bajo)
            "id":       v["first_id"],
            "variants": v["variants"],
        }
        for key, v in raw_cards.items()
    }

    _js_stock_cache    = products
    _js_stock_cache_ts = time.time()
    logger.info(f"[JS_STOCK] ✅ {len(products)} cartas únicas / {total_prods} productos / "
                f"{sum(v['stock'] for v in products.values())} u. stock total")
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
    Rate limit: 20 análisis/hora por IP (protege el free tier).
    """
    if not settings.BUYLIST_OPEN:
        raise HTTPException(503, "La Buylist está temporalmente cerrada. Vuelve pronto.")

    ip = (request.client.host if request and request.client else "unknown")
    if not _rate_limit(f"analyze:{ip}", max_calls=20, window_sec=3600):
        raise HTTPException(429, "Demasiados análisis. Espera un momento e inténtalo de nuevo.")

    content = await file.read()
    if len(content) > 5 * 1024 * 1024:  # 5 MB max
        raise HTTPException(413, "Archivo demasiado grande (máx 5 MB)")

    df = _read_csv(content)

    required = {"Name", "Quantity", "Purchase price"}
    missing  = required - set(df.columns)
    if missing:
        raise HTTPException(422, f"Columnas faltantes: {missing}. Encontradas: {list(df.columns)}")

    # ── Pre-pase y tablas de lookup ────────────────────────────────────────
    from .models import StapleCard
    base_min_price  = _build_base_min_price(df)
    staple_map_pub  = _build_staple_map(db.query(StapleCard).all())
    js_stock_pub    = await _fetch_js_stock_cached()

    results = []
    for _, row in df.iterrows():
        name      = str(row.get("Name",           "")).strip()
        qty       = int(row.get("Quantity",         1) or 1)
        price_usd = float(row.get("Purchase price", 0) or 0)
        foil_raw  = str(row.get("Foil",      "normal")).strip().lower()
        cond_raw  = str(row.get("Condition", "near_mint")).strip().lower()
        version   = str(row.get("Version",   "")).strip()
        currency  = str(row.get("Purchase price currency", "USD")).strip().upper()

        if not name or price_usd <= 0:
            continue
        if currency == "EUR":
            price_usd *= 1.10

        eff_usd, adjusted_price, is_estaca_card = _compute_card_price(
            price_usd, foil_raw, version, cond_raw, base_min_price, name
        )

        price_credito = int(adjusted_price * settings.BUYLIST_FACTOR_CREDITO)
        price_cash    = int(adjusted_price * settings.BUYLIST_FACTOR_CASH)

        # ── Tier y estado de compra ─────────────────────────────────────────
        srec         = _staple_lookup(staple_map_pub, name)
        tier_pub     = srec.tier if srec else "sin_lista"
        key_pub      = _canonical(name)
        js_pub       = js_stock_pub.get(key_pub, {})
        stock_pub    = js_pub.get("stock", None)
        min_s_pub    = (srec.min_stock_override if srec and srec.min_stock_override
                        else settings.MIN_STOCK_ALTA   if tier_pub == "alta"
                        else settings.MIN_STOCK_NORMAL)

        if tier_pub == "muy_alta":
            buying_status = "muy_alta"
        elif tier_pub in ("alta", "normal"):
            buying_status = "buscamos" if (stock_pub is None or stock_pub < min_s_pub * 3) else "stock_ok"
        else:
            buying_status = "sin_lista"

        results.append({
            "name":           name,
            "qty":            qty,
            "price_usd":      round(adjusted_price, 2),
            "price_usd_raw":  round(price_usd, 2),
            "price_usd_base": round(eff_usd, 2),
            "foil":           foil_raw,
            "condition":      cond_raw,
            "version":        version,
            "is_estaca":      is_estaca_card,
            "stake_mult":     settings.STAKE_MULTIPLIER if is_estaca_card else 1.0,
            "price_credito":  price_credito,
            "price_cash":     price_cash,
            "price_normal":   price_credito,   # alias legacy para frontend
            "tier":           tier_pub,
            "stock_actual":   stock_pub,
            "min_stock":      min_s_pub,
            "buying_status":  buying_status,
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

    # Rate limiting por IP
    ip = request.client.host if request.client else "unknown"
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
    db.refresh(order)



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
    return {"status": "ok", "id": card.id, "name": card.name_display, "key": key}


@app.delete("/api/admin/staples/{staple_id}", dependencies=[Depends(verify_admin)])
def delete_staple(staple_id: int, db: Session = Depends(get_db)):
    from .models import StapleCard
    card = db.query(StapleCard).filter(StapleCard.id == staple_id).first()
    if not card:
        raise HTTPException(404, "Carta no encontrada")
    db.delete(card)
    db.commit()
    return {"status": "ok"}


# =====================================================================
# 📊 STOCK CHECK — análisis CSV vs Jumpseller
# =====================================================================

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
    staple_map     = _build_staple_map(db.query(StapleCard).all())
    js_stock       = await _fetch_js_stock_cached()

    results      = []
    total_compra = 0.0

    for _, row in df.iterrows():
        name     = str(row.get("Name",           "")).strip()
        qty      = int(row.get("Quantity",         1) or 1)
        raw_usd  = float(row.get("Purchase price", 0) or 0)
        foil_raw = str(row.get("Foil",      "normal")).strip().lower()
        cond_raw = str(row.get("Condition", "near_mint")).strip().lower()
        version  = str(row.get("Version",   "")).strip()
        currency = str(row.get("Purchase price currency", "USD")).strip().upper()

        if not name or raw_usd <= 0:
            continue
        if currency == "EUR":
            raw_usd *= 1.10

        eff_usd, price_usd, is_estaca_card = _compute_card_price(
            raw_usd, foil_raw, version, cond_raw, base_min_price, name
        )

        # Tier lookup O(1)
        staple_rec  = _staple_lookup(staple_map, name)
        tier        = staple_rec.tier if staple_rec else "normal"
        is_muy_alta = tier == "muy_alta"
        is_alta     = tier == "alta"
        min_stock   = (staple_rec.min_stock_override if staple_rec and staple_rec.min_stock_override
                       else settings.MIN_STOCK_ALTA if is_alta else settings.MIN_STOCK_NORMAL)
        margin      = staple_rec.margin_factor if staple_rec else 2.5

        price_clp_cash    = price_usd * settings.BUYLIST_FACTOR_CASH
        price_clp_credito = price_usd * settings.BUYLIST_FACTOR_CREDITO
        min_price_venta   = price_clp_cash * margin

        key              = _canonical(name)
        js_data          = js_stock.get(key, {})
        stock_actual     = js_data.get("stock", None)
        price_js         = js_data.get("price", 0)
        js_id            = js_data.get("id")
        stock_proyectado = (stock_actual or 0) + qty

        # ── Alertas ───────────────────────────────────────────────────────
        alerts = []
        status = "ok"

        if stock_actual is None:
            alerts.append({"type": "info", "msg": "No existe en Jumpseller — se creará"})
            status = "info"

        if raw_usd < settings.MIN_PURCHASE_USD:
            alerts.append({"type": "warning",
                           "msg": f"Precio ${raw_usd:.2f} USD bajo mínimo rentable (${settings.MIN_PURCHASE_USD} USD)"})
            if status == "ok":
                status = "warning"

        if is_estaca_card:
            bn        = _canonical(name)
            base_used = base_min_price.get(bn, raw_usd)
            origin    = "CSV base" if bn in base_min_price else "precio CK propio"
            alerts.append({"type": "info",
                           "msg": f"Estaca ×{settings.STAKE_MULTIPLIER} (base ${base_used:.2f} USD — {origin})"})

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

        if not is_muy_alta and stock_actual is not None and stock_actual < min_stock:
            alerts.append({"type": "warning",
                           "msg": f"Stock actual ({stock_actual}) bajo mínimo ({min_stock})"})
            if status == "ok":
                status = "warning"

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

    # Staple map para enriquecer con tier — usa el mismo helper que los otros endpoints
    from .models import StapleCard
    staple_map = _build_staple_map(db.query(StapleCard).all())

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
    return {
        "status":           "ok",
        "version":          "4.6",
        "buylist_open":     settings.BUYLIST_OPEN,
        "cash_enabled":     settings.CASH_ENABLED,
        "js_cache_age_sec": int(time.time() - _js_stock_cache_ts),
        "js_cache_cards":   len(_js_stock_cache),
        "js_cache_valid":   bool(_js_stock_cache and time.time() - _js_stock_cache_ts < JS_STOCK_TTL),
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
    age = int(time.time() - _js_stock_cache_ts)
    return {
        "cache_age_sec":   age,
        "cache_ttl_sec":   JS_STOCK_TTL,
        "cache_valid":     bool(_js_stock_cache and age < JS_STOCK_TTL),
        "cards_loaded":    len(_js_stock_cache),
        "total_stock":     sum(v["stock"] for v in _js_stock_cache.values()),
        "sample_5":        list(_js_stock_cache.keys())[:5],
    }
""