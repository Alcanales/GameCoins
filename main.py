import json
import secrets
import random
import string
import hmac
import hashlib
import aiohttp
from fastapi import FastAPI, HTTPException, Form, BackgroundTasks, Depends, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text, inspect

from config import settings
from database import engine, Base, get_db
import services as logic
from schemas import BuylistSubmitRequest, UpdateRequest, CanjeRequest, ConfigRequest
from models import GameCoinUser, SystemConfig

Base.metadata.create_all(bind=engine)

app = FastAPI(title="GameQuest API", version="Secure-Edition")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["["https://gamequest.cl", "https://gamequest-cl.jumpseller.com"]"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- SEGURIDAD ---
def verify_admin(x_admin_user: str = Header(None), x_admin_pass: str = Header(None)):
    """Verifica credenciales maestras (Para Bóveda)"""
    if not (x_admin_user and x_admin_pass): raise HTTPException(401)
    auth_ok = secrets.compare_digest(x_admin_user, settings.ADMIN_USER) and secrets.compare_digest(x_admin_pass, settings.ADMIN_PASS)
    if not auth_ok: raise HTTPException(401, "Credenciales Admin Incorrectas")

def verify_store_token(x_store_token: str = Header(None)):
    """Verifica token de tienda (Para Widget Público)"""
    if x_store_token != settings.STORE_TOKEN:
        raise HTTPException(401, "Token de Tienda Inválido")

# --- ENDPOINTS ---
@app.get("/api/public/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    u = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
    return {"saldo": int(u.saldo if u else 0)}

# --- ADMIN API (Protegida con User/Pass) ---
@app.get("/admin/users", dependencies=[Depends(verify_admin)])
def list_users(db: Session = Depends(get_db)):
    return db.query(GameCoinUser).all()

@app.post("/admin/update_saldo", dependencies=[Depends(verify_admin)])
def update_saldo(req: UpdateRequest, db: Session = Depends(get_db)):
    u = db.query(GameCoinUser).filter(GameCoinUser.email == req.email).with_for_update().first()
    if not u:
        u = GameCoinUser(email=req.email, rut="N/A", saldo=0); db.add(u)
    if req.accion == "add": u.saldo += req.monto
    elif req.accion == "set": u.saldo = req.monto
    elif req.accion == "subtract": u.saldo = max(0, u.saldo - req.monto)
    db.commit()
    return {"status": "ok", "nuevo_saldo": u.saldo}

# --- CONFIGURACIÓN DB ---
def set_db_config(db: Session, key: str, value: str):
    item = db.query(SystemConfig).filter(SystemConfig.key == key).first()
    if not item: db.add(SystemConfig(key=key, value=value))
    else: item.value = value
    db.commit()

def get_db_config(db: Session, key: str):
    item = db.query(SystemConfig).filter(SystemConfig.key == key).first()
    return item.value if item else ""

@app.post("/admin/config", dependencies=[Depends(verify_admin)])
def update_config(req: ConfigRequest, db: Session = Depends(get_db)):
    clean_store = req.store_login.lower().replace("https://", "").replace(".jumpseller.com", "").strip()
    set_db_config(db, "JUMPSELLER_API_TOKEN", req.api_token.strip())
    set_db_config(db, "JUMPSELLER_STORE", clean_store)
    set_db_config(db, "JUMPSELLER_HOOKS_TOKEN", req.hooks_token.strip())
    return {"status": "ok"}

# --- CANJE SEGURO (Protegido con Token Público) ---
# CAMBIO CRÍTICO: Ahora usa verify_store_token en lugar de verify_admin
@app.post("/api/canje", dependencies=[Depends(verify_store_token)])
async def canje(req: CanjeRequest, db: Session = Depends(get_db)):
    # Leer credenciales Jumpseller de DB
    token = get_db_config(db, "JUMPSELLER_API_TOKEN")
    store = get_db_config(db, "JUMPSELLER_STORE")

    if not token or not store:
        raise HTTPException(503, "Tienda no configurada. Contacta al soporte.")

    # Verificar Saldo
    u = db.query(GameCoinUser).filter(GameCoinUser.email == req.email).with_for_update().first()
    if not u or u.saldo < req.monto: raise HTTPException(400, "Saldo insuficiente")
    
    # Descontar
    u.saldo -= req.monto
    u.historico_canjeado += req.monto
    db.commit()
    
    # Crear Cupón
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    code = f"GQ-{suffix}"
    
    async with aiohttp.ClientSession() as s:
        res = await logic.crear_cupon_jumpseller(s, code, req.monto, req.email)
        if not res or "promotion" not in res:
             u.saldo += req.monto
             u.historico_canjeado -= req.monto
             db.commit()
             return {"status": "error", "mensaje": "Error creando cupón. Saldo devuelto."}
        
        return {"status": "ok", "cupon_codigo": code}

# --- WEBHOOK ---
@app.post("/api/jumpseller/webhook")
async def jumpseller_webhook(request: Request, x_jumpseller_hmac_sha256: str = Header(None), db: Session = Depends(get_db)):
    hook_token = get_db_config(db, "JUMPSELLER_HOOKS_TOKEN")
    if not hook_token: return {"status": "ignored"}
    
    body_bytes = await request.body()
    signature = hmac.new(hook_token.encode(), body_bytes, hashlib.sha256).digest()
    import base64
    if base64.b64encode(signature).decode() != x_jumpseller_hmac_sha256:
        raise HTTPException(401, "Firma inválida")

    try:
        data = json.loads(body_bytes)
        order = data.get("order", {})
        if order.get("status") == "paid" and order.get("customer", {}).get("email"):
            points = int(float(order.get("total", 0)) / 1000)
            if points > 0:
                email = order.get("customer", {}).get("email")
                user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
                if not user:
                    nm = f"{order.get('customer', {}).get('name','')} {order.get('customer', {}).get('surname','')}"
                    user = GameCoinUser(email=email, name=nm.strip(), saldo=0)
                    db.add(user)
                user.saldo += points
                db.commit()
    except: pass
    return {"status": "ok"}
