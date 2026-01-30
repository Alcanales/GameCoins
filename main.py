import json
import secrets
import random
import string
import hmac
import hashlib
import aiohttp
from fastapi import FastAPI, UploadFile, File, HTTPException, Form, BackgroundTasks, Depends, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text, inspect

from config import settings
from database import engine, Base, get_db
import services as logic
from schemas import BuylistSubmitRequest, UpdateRequest, CanjeRequest, ConfigRequest
from models import GameCoinUser

Base.metadata.create_all(bind=engine)

app = FastAPI(title="GameQuest API", version="Modal-Edition")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- AUTO-HEALING DB ---
@app.on_event("startup")
def startup_db_check():
    try:
        inspector = inspect(engine)
        if inspector.has_table("gamecoins"):
            cols = [c['name'] for c in inspector.get_columns('gamecoins')]
            with engine.connect() as conn:
                if 'name' not in cols: conn.execute(text("ALTER TABLE gamecoins ADD COLUMN name VARCHAR;"))
                conn.commit()
    except Exception as e: print(f"DB Check: {e}")

# --- SECURITY ---
def verify_admin(x_admin_user: str = Header(None), x_admin_pass: str = Header(None)):
    if not (x_admin_user and x_admin_pass): raise HTTPException(401)
    auth_ok = (secrets.compare_digest(x_admin_user, settings.ADMIN_USER) and secrets.compare_digest(x_admin_pass, settings.ADMIN_PASS))
    if not auth_ok: raise HTTPException(401)

# --- ENDPOINTS ---
@app.get("/api/public/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    u = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
    return {"saldo": int(u.saldo if u else 0)}

@app.post("/api/enviar_buylist")
async def send_buylist(background_tasks: BackgroundTasks, payload: str = Form(...), csv_file: UploadFile = File(...)):
    return {"status": "received"} # Lógica simplificada

# --- ADMIN API ---
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

# --- CONFIGURACIÓN DINÁMICA (MODAL) ---
@app.post("/admin/config", dependencies=[Depends(verify_admin)])
def update_config(req: ConfigRequest):
    """Actualiza las credenciales en memoria"""
    settings.JUMPSELLER_API_TOKEN = req.api_token
    settings.JUMPSELLER_STORE = req.store_login
    settings.JUMPSELLER_HOOKS_TOKEN = req.hooks_token
    return {"status": "ok", "mensaje": "Credenciales actualizadas."}

@app.get("/admin/get_config", dependencies=[Depends(verify_admin)])
def get_config_endpoint():
    """Devuelve la configuración actual (opcional para ver si guardó)"""
    return {
        "store": settings.JUMPSELLER_STORE,
        "has_token": bool(settings.JUMPSELLER_API_TOKEN),
        "has_hook": bool(settings.JUMPSELLER_HOOKS_TOKEN)
    }

# --- CANJE SEGURO ---
@app.post("/admin/canje", dependencies=[Depends(verify_admin)])
async def canje(req: CanjeRequest, db: Session = Depends(get_db)):
    # 1. Verificar credenciales antes de intentar nada
    if not settings.JUMPSELLER_API_TOKEN or not settings.JUMPSELLER_STORE:
        raise HTTPException(500, "Error: Tienda no configurada. Ve al panel de admin y configura las llaves.")

    # 2. Verificar Saldo
    u = db.query(GameCoinUser).filter(GameCoinUser.email == req.email).with_for_update().first()
    if not u or u.saldo < req.monto: raise HTTPException(400, "Saldo insuficiente")
    
    # 3. Descontar Saldo (Optimistic Locking)
    u.saldo -= req.monto
    u.historico_canjeado += req.monto
    db.commit()
    
    # 4. Generar Cupón
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    code = f"GQ-{suffix}"
    
    async with aiohttp.ClientSession() as s:
        res = await logic.crear_cupon_jumpseller(s, code, req.monto, req.email)
        
        # 5. Fail-Safe: Si falla Jumpseller, devolvemos el dinero
        if not res or "promotion" not in res:
             u.saldo += req.monto
             u.historico_canjeado -= req.monto
             db.commit()
             print(f"❌ Fallo al crear cupón en Jumpseller: {res}")
             return {"status": "error", "mensaje": "Error de comunicación con Jumpseller. Saldo devuelto."}
        
        return {"status": "ok", "cupon_codigo": code}

# --- WEBHOOK (Lealtad + Validación) ---
@app.post("/api/jumpseller/webhook")
async def jumpseller_webhook(request: Request, x_jumpseller_hmac_sha256: str = Header(None), db: Session = Depends(get_db)):
    if not settings.JUMPSELLER_HOOKS_TOKEN: return {"status": "ignored_no_token"}
    
    body_bytes = await request.body()
    signature = hmac.new(settings.JUMPSELLER_HOOKS_TOKEN.encode(), body_bytes, hashlib.sha256).digest()
    import base64
    calculated = base64.b64encode(signature).decode()
    
    if calculated != x_jumpseller_hmac_sha256:
        raise HTTPException(401, "Firma inválida")

    try:
        data = json.loads(body_bytes)
        order = data.get("order", {})
        if order.get("status") == "paid" and order.get("customer", {}).get("email"):
            # Lógica: 1 QP por cada $1000
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
                print(f"💰 Lealtad: {points} QP sumados a {email}")
    except Exception as e:
        print(f"Error Webhook: {e}")
    
    return {"status": "ok"}
