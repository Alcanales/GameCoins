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
from datetime import datetime, date

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
JS_STOCK_TTL = 300  # segundos

# ── Budget diario de buylist ──────────────────────────────────────────────────
_daily_budget: dict[str, float] = {}  # {"2025-01-15": 123.45}

def _today() -> str:
    return date.today().isoformat()

def _budget_spent() -> float:
    return _daily_budget.get(_today(), 0.0)

def _budget_add(usd: float):
    key = _today()
    _daily_budget[key] = _daily_budget.get(key, 0.0) + usd

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
    is_staple:          bool  = True
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

def _normalize_name(name: str) -> str:
    """
    Normaliza nombre de carta para matching:
    - Remove accents (û→u, é→e)
    - Lowercase
    - Strip set/edition info in parentheses/brackets
    - Collapse spaces
    """
    n = name.strip()
    # Quitar info de set entre paréntesis o corchetes
    n = re.sub(r"[\(\[][^\)\]]+[\)\]]", "", n)
    # Normalizar unicode (ñ, û, é, etc.)
    n = unicodedata.normalize("NFKD", n)
    n = "".join(c for c in n if not unicodedata.combining(c))
    # Lowercase y colapsar espacios
    n = " ".join(n.lower().split())
    return n


# ── Condición (CK cotiza en NM, aplicamos descuento por estado físico) ──────────

def _cond_mult(cond_str: str) -> float:
    return {
        "near_mint":         settings.COND_NM,
        "lightly_played":    settings.COND_LP,
        "moderately_played": settings.COND_MP,
        "heavily_played":    settings.COND_HP,
        "damaged":           settings.COND_DMG,
    }.get((cond_str or "near_mint").lower(), settings.COND_NM)


# ── Detección de versión especial (Estaca) ────────────────────────────────────
# Una carta es "estaca" si es foil/etched O si su nombre o versión indica arte
# premium (Borderless, Extended Art, Showcase, Alternate Art, Full Art, etc.).
# En Manabox el campo "Foil" cubre foil/etched; el resto se detecta por keywords
# en el nombre o en la columna "Version" si el CSV la trae.

_ESTACA_KEYWORDS = {
    "borderless", "extended art", "showcase", "alternate art",
    "full art", "retro frame", "surge foil", "galaxy foil",
    "textured foil", "gilded foil", "etched", "concept prewallpaper",
    "serialized", "double rainbow foil",
}

def _is_estaca(foil: str, name: str, version: str = "") -> bool:
    """
    Retorna True si la carta es una versión especial premium.
    - foil   : columna 'Foil' del CSV  (normal | foil | etched)
    - name   : nombre completo de la carta
    - version: columna 'Version' del CSV si existe (Manabox puede traerla)
    """
    foil_l = (foil or "normal").strip().lower()
    if foil_l in ("foil", "etched"):
        return True
    combined = f"{name} {version}".lower()
    return any(kw in combined for kw in _ESTACA_KEYWORDS)


def _base_name(name: str) -> str:
    """
    Extrae el nombre base de la carta quitando sufijos de versión
    para agrupar todas las versiones de la misma carta.
    Ej: 'Lightning Bolt (Borderless)' → 'lightning bolt'
         'Thassa's Oracle Extended Art' → 'thassa's oracle'
    """
    n = _normalize_name(name)  # ya quita paréntesis/corchetes y normaliza
    # Quitar keywords de versión que puedan estar en el nombre normalizado
    for kw in sorted(_ESTACA_KEYWORDS, key=len, reverse=True):
        n = n.replace(kw, "").strip()
    return " ".join(n.split())  # colapsar espacios dobles


# ── JS stock con cache ────────────────────────────────────────────────────────

async def _fetch_js_stock_cached() -> dict:
    global _js_stock_cache, _js_stock_cache_ts
    if time.time() - _js_stock_cache_ts < JS_STOCK_TTL and _js_stock_cache:
        logger.info("[JS_STOCK] Usando caché")
        return _js_stock_cache

    logger.info("[JS_STOCK] Descargando catálogo de Jumpseller...")
    url    = f"{settings.JUMPSELLER_API_BASE}/products.json"
    params = {"login": settings.JS_LOGIN_CODE, "authtoken": settings.JS_AUTH_TOKEN,
              "limit": 50, "page": 1}
    products = {}

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(url, params=params,
                                       timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json()
                    if not data:
                        break
                    for p in data:
                        prod  = p.get("product", p)
                        name  = (prod.get("name") or "").strip()
                        if not name:
                            continue
                        variants = prod.get("variants", [])
                        if variants:
                            v = variants[0].get("variant", variants[0])
                            stock = int(v.get("stock", 0) or 0)
                            price = float(v.get("price", 0) or 0)
                        else:
                            stock = int(prod.get("stock", 0) or 0)
                            price = float(prod.get("price", 0) or 0)
                        key = _normalize_name(name)
                        products[key] = {"id": prod.get("id"), "name": name,
                                         "stock": stock, "price": price}
                    if len(data) < 50:
                        break
                    params["page"] += 1
                    await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"[JS_STOCK] Error página {params['page']}: {e}")
                break

    _js_stock_cache    = products
    _js_stock_cache_ts = time.time()
    logger.info(f"[JS_STOCK] {len(products)} productos cargados")
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
    spent = _budget_spent()
    return {
        "open":          settings.BUYLIST_OPEN,
        "budget_daily":  settings.BUYLIST_DAILY_BUDGET_USD,
        "budget_spent":  round(spent, 2),
        "budget_left":   max(0.0, round(settings.BUYLIST_DAILY_BUDGET_USD - spent, 2)),
    }


@app.post("/api/public/analyze_buylist")
async def analyze_buylist(file: UploadFile = File(...)):
    """
    Recibe CSV Manabox. Aplica foil/condición/moneda. Retorna cotización.
    Versión pública — sin datos de stock, solo precios de compra.
    """
    if not settings.BUYLIST_OPEN:
        raise HTTPException(503, "La Buylist está temporalmente cerrada. Vuelve pronto.")

    content = await file.read()
    if len(content) > 5 * 1024 * 1024:  # 5 MB max
        raise HTTPException(413, "Archivo demasiado grande (máx 5 MB)")

    df = _read_csv(content)

    required = {"Name", "Quantity", "Purchase price"}
    missing  = required - set(df.columns)
    if missing:
        raise HTTPException(422, f"Columnas faltantes: {missing}. Encontradas: {list(df.columns)}")

    # ── Pre-pase: precio base mínimo por nombre de carta (para cálculo estaca) ───
    # Se agrupa por nombre normalizado e ignora versiones especiales para encontrar
    # el precio de la versión regular más barata de cada carta en el CSV.
    base_min_price: dict[str, float] = {}
    for _, row in df.iterrows():
        nm    = str(row.get("Name", "")).strip()
        pr    = float(row.get("Purchase price", 0) or 0)
        fo    = str(row.get("Foil", "normal")).strip().lower()
        ve    = str(row.get("Version", "")).strip()
        cur   = str(row.get("Purchase price currency", "USD")).strip().upper()
        if not nm or pr <= 0:
            continue
        if cur == "EUR":
            pr = pr * 1.10
        if not _is_estaca(fo, nm, ve):
            bn = _base_name(nm)
            if bn not in base_min_price or pr < base_min_price[bn]:
                base_min_price[bn] = pr

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
            price_usd = price_usd * 1.10

        # ── Lógica de Estaca ──────────────────────────────────────────────────
        is_estaca_card = _is_estaca(foil_raw, name, version)
        if is_estaca_card:
            bn = _base_name(name)
            # Usar precio base mínimo si existe en el CSV; si no, usar el propio precio CK
            ref_price = base_min_price.get(bn, price_usd)
            effective_price = ref_price * settings.STAKE_MULTIPLIER
        else:
            effective_price = price_usd

        # Condición aplica siempre (CK cotiza en NM)
        cm             = _cond_mult(cond_raw)
        adjusted_price = effective_price * cm

        price_credito = int(adjusted_price * settings.BUYLIST_FACTOR_CREDITO)
        price_cash    = int(adjusted_price * settings.BUYLIST_FACTOR_CASH)

        results.append({
            "name":           name,
            "qty":            qty,
            "price_usd":      round(adjusted_price, 2),
            "price_usd_raw":  round(price_usd, 2),
            "price_usd_base": round(effective_price, 2),  # precio base antes de condición
            "foil":           foil_raw,
            "condition":      cond_raw,
            "version":        version,
            "is_estaca":      is_estaca_card,
            "stake_mult":     settings.STAKE_MULTIPLIER if is_estaca_card else 1.0,
            "price_credito":  price_credito,
            "price_cash":     price_cash,
            "price_normal":   price_credito,  # compat. HTML anterior
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

    # Verificar presupuesto diario
    total_usd = sum(
        it.price_usd * getattr(it, "qty", getattr(it, "qty_csv", 1))
        for it in req.items
    )
    if _budget_spent() + total_usd > settings.BUYLIST_DAILY_BUDGET_USD:
        raise HTTPException(503,
            "El presupuesto diario de compra está agotado. Vuelve mañana.")

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

    _budget_add(total_usd)

    # ── Emails en background — wrapper sync que corre la coroutine ──────────────
    def _send_both_sync():
        import asyncio
        asyncio.run(email_service.send_public_buylist_both(
            vendor_email   = req.email,
            rut            = req.rut,
            payment_pref   = req.payment_preference,
            items          = items_dict,
            total_credito  = req.total_credito,
            total_cash     = req.total_cash,
            order_id       = order.id,
        ))
    background_tasks.add_task(_send_both_sync)

    return {
        "status":   "ok",
        "order_id": order.id,
        "message":  f"Orden #{order.id} guardada. Email enviado a {req.email} y respaldo a tienda.",
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
    return [{"id": c.id, "name": c.name_display, "is_staple": c.is_staple,
             "min_stock_override": c.min_stock_override, "margin_factor": c.margin_factor,
             "created_at": c.created_at.isoformat() if c.created_at else None}
            for c in db.query(StapleCard).order_by(StapleCard.name_display).all()]


@app.post("/api/admin/staples", dependencies=[Depends(verify_admin)])
def upsert_staple(req: StapleUpsertReq, db: Session = Depends(get_db)):
    from .models import StapleCard
    key  = _normalize_name(req.name)
    card = db.query(StapleCard).filter(StapleCard.name_normalized == key).first()
    if card:
        card.is_staple          = req.is_staple
        card.min_stock_override = req.min_stock_override
        card.margin_factor      = req.margin_factor
        card.name_display       = req.name.strip()
    else:
        card = StapleCard(name_normalized=key, name_display=req.name.strip(),
                          is_staple=req.is_staple, min_stock_override=req.min_stock_override,
                          margin_factor=req.margin_factor)
        db.add(card)
    db.commit()
    db.refresh(card)
    return {"status": "ok", "id": card.id, "name": card.name_display}


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

    # Cargar staples y stock JS (en paralelo)
    staple_rows = db.query(StapleCard).all()
    staple_map  = {_normalize_name(s.name_display): s for s in staple_rows}
    js_stock    = await _fetch_js_stock_cached()

    # ── Pre-pase: precio base mínimo por carta (para estaca) ────────────────────
    base_min_price: dict[str, float] = {}
    for _, row in df.iterrows():
        nm  = str(row.get("Name", "")).strip()
        pr  = float(row.get("Purchase price", 0) or 0)
        fo  = str(row.get("Foil", "normal")).strip().lower()
        ve  = str(row.get("Version", "")).strip()
        cur = str(row.get("Purchase price currency", "USD")).strip().upper()
        if not nm or pr <= 0:
            continue
        if cur == "EUR":
            pr = pr * 1.10
        if not _is_estaca(fo, nm, ve):
            bn = _base_name(nm)
            if bn not in base_min_price or pr < base_min_price[bn]:
                base_min_price[bn] = pr

    results      = []
    total_compra = 0.0

    for _, row in df.iterrows():
        name     = str(row.get("Name",           "")).strip()
        qty_csv  = int(row.get("Quantity",         1) or 1)
        raw_usd  = float(row.get("Purchase price", 0) or 0)
        foil_raw = str(row.get("Foil",      "normal")).strip().lower()
        cond_raw = str(row.get("Condition", "near_mint")).strip().lower()
        version  = str(row.get("Version",   "")).strip()
        currency = str(row.get("Purchase price currency", "USD")).strip().upper()

        if not name or raw_usd <= 0:
            continue

        if currency == "EUR":
            raw_usd = raw_usd * 1.10

        # ── Lógica de Estaca ──────────────────────────────────────────────────
        is_estaca_card = _is_estaca(foil_raw, name, version)
        if is_estaca_card:
            bn        = _base_name(name)
            ref_price = base_min_price.get(bn, raw_usd)  # fallback al propio precio CK
            eff_usd   = ref_price * settings.STAKE_MULTIPLIER
        else:
            eff_usd   = raw_usd

        # Condición aplica siempre
        cm        = _cond_mult(cond_raw)
        price_usd = round(eff_usd * cm, 4)

        # Staple lookup
        key        = _normalize_name(name)
        staple_rec = staple_map.get(key)
        is_staple  = staple_rec.is_staple if staple_rec else False
        min_stock  = (staple_rec.min_stock_override if staple_rec and staple_rec.min_stock_override
                      else (8 if is_staple else 4))
        margin     = staple_rec.margin_factor if staple_rec else 2.5

        # Precios CLP
        price_clp_cash    = price_usd * settings.BUYLIST_FACTOR_CASH
        price_clp_credito = price_usd * settings.BUYLIST_FACTOR_CREDITO
        min_price_venta   = price_clp_cash * margin

        # Stock JS
        js_data          = js_stock.get(key, {})
        stock_actual     = js_data.get("stock", None)
        price_js         = js_data.get("price", 0)
        js_id            = js_data.get("id")
        stock_proyectado = (stock_actual or 0) + qty_csv

        # ── Alertas ───────────────────────────────────────────────────
        alerts = []
        status = "ok"

        # Carta no existe en JS
        if stock_actual is None:
            alerts.append({"type": "info", "msg": "No existe en Jumpseller — se creará"})
            status = "info"

        # Precio mínimo de compra
        if raw_usd < settings.MIN_PURCHASE_USD:
            alerts.append({"type": "warning",
                           "msg": f"Precio base ${raw_usd:.2f} USD bajo mínimo rentable (${settings.MIN_PURCHASE_USD} USD)"})
            if status == "ok":
                status = "warning"

        # Estaca: informar qué multiplicador y precio base se usó
        if is_estaca_card:
            bn        = _base_name(name)
            base_used = base_min_price.get(bn, raw_usd)
            origin    = "CSV base" if bn in base_min_price else "precio CK propio"
            alerts.append({"type": "info",
                           "msg": (f"Estaca ×{settings.STAKE_MULTIPLIER} "
                                   f"(base ${base_used:.2f} USD — {origin})")})

        # Sobrestock
        overstock_limit = min_stock * 3
        if stock_proyectado > overstock_limit:
            alerts.append({"type": "danger",
                           "msg": f"Sobrestock: tendrías {stock_proyectado} u. (límite {overstock_limit})"})
            status = "danger"
        elif stock_proyectado > min_stock * 2:
            alerts.append({"type": "warning",
                           "msg": f"Stock alto: {stock_proyectado} u. tras compra"})
            if status == "ok":
                status = "warning"

        # Precio JS bajo mínimo de venta
        if price_js > 0 and price_js < min_price_venta:
            alerts.append({"type": "danger",
                           "msg": f"Precio JS ${price_js:,.0f} bajo mínimo recomendado ${min_price_venta:,.0f}"})
            status = "danger"

        # Stock actual bajo mínimo
        if stock_actual is not None and stock_actual < min_stock:
            alerts.append({"type": "warning",
                           "msg": f"Stock actual ({stock_actual}) bajo mínimo ({min_stock}) — compra ayuda"})
            if status == "ok":
                status = "warning"

        total_compra += raw_usd * qty_csv

        results.append({
            "name":              name,
            "qty_csv":           qty_csv,
            "price_usd":         round(price_usd, 2),
            "price_usd_raw":     round(raw_usd, 4),
            "price_usd_eff":     round(eff_usd, 4),   # precio efectivo tras estaca
            "foil":              foil_raw,
            "condition":         cond_raw,
            "version":           version,
            "is_estaca":         is_estaca_card,
            "stake_mult":        settings.STAKE_MULTIPLIER if is_estaca_card else 1.0,
            "price_cash":        int(price_clp_cash),
            "price_credito":     int(price_clp_credito),
            "min_price_venta":   int(min_price_venta),
            "is_staple":         is_staple,
            "min_stock":         min_stock,
            "stock_actual":      stock_actual,
            "stock_proyectado":  stock_proyectado,
            "price_js":          price_js,
            "js_id":             js_id,
            "alerts":            alerts,
            "status":            status,
            "approved":          all(a["type"] != "danger" for a in alerts),
        })

    order_map = {"danger": 0, "warning": 1, "info": 2, "ok": 3}
    results.sort(key=lambda r: order_map.get(r["status"], 9))

    total_clp_cash    = sum(r["price_cash"]    * r["qty_csv"] for r in results)
    total_clp_credito = sum(r["price_credito"] * r["qty_csv"] for r in results)

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

@app.post("/api/admin/clean_coupons", dependencies=[Depends(verify_admin)])
async def clean_used_coupons():
    burned = await VaultController.sweep_used_coupons()
    return {"status": "success", "burned": burned}

@app.get("/health")
def health():
    return {
        "status":        "ok",
        "version":       "4.0",
        "buylist_open":  settings.BUYLIST_OPEN,
        "budget_spent":  round(_budget_spent(), 2),
        "budget_limit":  settings.BUYLIST_DAILY_BUDGET_USD,
        "js_cache_age":  int(time.time() - _js_stock_cache_ts),
    }
