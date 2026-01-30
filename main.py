import json
import secrets
import random
import string
import hmac
import hashlib
import aiohttp
from fastapi import FastAPI, HTTPException, Depends, Header, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text, inspect

from config import settings
from database import engine, Base, get_db
import services as logic
from schemas import UpdateRequest, CanjeRequest, ConfigRequest
from models import GameCoinUser, SystemConfig

# Crear tablas automáticamente
Base.metadata.create_all(bind=engine)

app = FastAPI(title="GameQuest API", version="Platinum-Audit")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- SEGURIDAD ---
def verify_admin(x_admin_user: str = Header(None), x_admin_pass: str = Header(None)):
    """Seguridad Bóveda (Alta)"""
    if not (x_admin_user and x_admin_pass): raise HTTPException(401)
    if not (secrets.compare_digest(x_admin_user, settings.ADMIN_USER) and 
            secrets.compare_digest(x_admin_pass, settings.ADMIN_PASS)):
        raise HTTPException(401, "Credenciales inválidas")

def verify_store_token(x_store_token: str = Header(None)):
    """Seguridad Widget (Pública)"""
    if x_store_token != settings.STORE_TOKEN:
        raise HTTPException(401, "Token de tienda inválido")

# --- ENDPOINTS PÚBLICOS (Widget) ---
@app.get("/api/public/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    u = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
    return {"saldo": int(u.saldo if u else 0)}

@app.post("/api/canje", dependencies=[Depends(verify_store_token)])
async def canje_publico(req: CanjeRequest, db: Session = Depends(get_db)):
    """Endpoint unificado para canje desde el Widget"""
    # 1. Verificar Configuración
    token = logic.get_db_config(db, "JUMPSELLER_API_TOKEN")
    store = logic.get_db_config(db, "JUMPSELLER_STORE")
    if not token or not store:
        raise HTTPException(503, "Sistema de canje no configurado. Contactar soporte.")

    # 2. Verificar Saldo y Bloquear Fila
    u = db.query(GameCoinUser).filter(GameCoinUser.email == req.email).with_for_update().first()
    if not u or u.saldo < req.monto:
        raise HTTPException(400, "Saldo insuficiente")

    # 3. Descontar Saldo (Optimista)
    u.saldo -= req.monto
    u.historico_canjeado += req.monto
    db.commit()

    # 4. Generar Cupón
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    code = f"GQ-{suffix}"

    async with aiohttp.ClientSession() as s:
        res = await logic.crear_cupon_jumpseller(s, code, req.monto, req.email, db)
        
        # 5. Fail-Safe: Si Jumpseller falla, devolver saldo
        if not res or "promotion" not in res:
            u.saldo += req.monto
            u.historico_canjeado -= req.monto
            db.commit()
            return {"status": "error", "mensaje": "Error de comunicación con Jumpseller. Saldo devuelto."}
            
        return {"status": "ok", "cupon_codigo": code, "nuevo_saldo": u.saldo}

# --- ENDPOINTS ADMIN (Bóveda) ---
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

# --- CONFIGURACIÓN DINÁMICA ---
@app.post("/admin/config", dependencies=[Depends(verify_admin)])
def update_config(req: ConfigRequest, db: Session = Depends(get_db)):
    def set_cfg(k, v):
        item = db.query(SystemConfig).filter(SystemConfig.key == k).first()
        if not item: db.add(SystemConfig(key=k, value=v))
        else: item.value = v
    
    # Sanitizar URL de tienda
    clean_store = req.store_login.lower().replace("https://", "").replace(".jumpseller.com", "").replace("/", "").strip()
    
    set_cfg("JUMPSELLER_API_TOKEN", req.api_token.strip())
    set_cfg("JUMPSELLER_STORE", clean_store)
    set_cfg("JUMPSELLER_HOOKS_TOKEN", req.hooks_token.strip())
    db.commit()
    return {"status": "ok", "mensaje": "Configuración guardada en DB"}

# --- WEBHOOK (Lealtad) ---
@app.post("/api/jumpseller/webhook")
async def jumpseller_webhook(request: Request, x_jumpseller_hmac_sha256: str = Header(None), db: Session = Depends(get_db)):
    hook_token = logic.get_db_config(db, "JUMPSELLER_HOOKS_TOKEN")
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
                u = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
                if not u:
                    nm = f"{order.get('customer', {}).get('name','')} {order.get('customer', {}).get('surname','')}"
                    u = GameCoinUser(email=email, name=nm.strip(), saldo=0); db.add(u)
                u.saldo += points
                db.commit()
    except: pass
    return {"status": "ok"}
