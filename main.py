import json
import secrets
import random
import string
import hmac
import hashlib
import aiohttp
from fastapi import FastAPI, HTTPException, Depends, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text, inspect, func
from config import settings
from database import engine, Base, get_db
import services as logic
from schemas import UpdateRequest, CanjeRequest, ConfigRequest
from models import GameCoinUser, SystemConfig

Base.metadata.create_all(bind=engine)

app = FastAPI(title="GameQuest API", version="Gold-Standard")
origins = [
    "https://gamequest.cl",
    "https://www.gamequest.cl",
    "https://gamequest-cl.jumpseller.com",
    "http://localhost:8000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["x-admin-user", "x-admin-pass", "x-store-token", "Content-Type", "Authorization"],
    expose_headers=["*"],
)



def verify_admin(x_admin_user: str = Header(None), x_admin_pass: str = Header(None)):
    if not (x_admin_user and x_admin_pass): raise HTTPException(401)
    if not (secrets.compare_digest(x_admin_user, settings.ADMIN_USER) and secrets.compare_digest(x_admin_pass, settings.ADMIN_PASS)): raise HTTPException(401)

def verify_store_token(x_store_token: str = Header(None)):
    if x_store_token != settings.STORE_TOKEN: raise HTTPException(401)

@app.get("/api/public/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    u = db.query(GameCoinUser).filter(func.lower(GameCoinUser.email) == email.lower().strip()).first()
    return {"saldo": int(u.saldo if u else 0)}

@app.post("/api/canje", dependencies=[Depends(verify_store_token)])
async def canje_publico(req: CanjeRequest, db: Session = Depends(get_db)):
    email_clean = req.email.lower().strip()
    token = logic.get_db_config(db, "JUMPSELLER_API_TOKEN")
    store = logic.get_db_config(db, "JUMPSELLER_STORE")
    if not token or not store: raise HTTPException(503, "Tienda no configurada")
    u = db.query(GameCoinUser).filter(func.lower(GameCoinUser.email) == email_clean).with_for_update().first()
    if not u or u.saldo < req.monto: raise HTTPException(400, "Saldo insuficiente")
    u.saldo -= req.monto
    u.historico_canjeado += req.monto
    db.commit()
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    code = f"GQ-{suffix}"
    async with aiohttp.ClientSession() as s:
        res = await logic.crear_cupon_jumpseller(s, code, req.monto, email_clean, db)
        if not res or "promotion" not in res:
            u.saldo += req.monto; u.historico_canjeado -= req.monto; db.commit()
            return {"status": "error", "mensaje": "Error Jumpseller. Saldo devuelto."}
        return {"status": "ok", "cupon_codigo": code, "nuevo_saldo": u.saldo}

@app.get("/admin/users", dependencies=[Depends(verify_admin)])
def list_users(db: Session = Depends(get_db)): return db.query(GameCoinUser).all()

@app.post("/admin/update_saldo", dependencies=[Depends(verify_admin)])
def update_saldo(req: UpdateRequest, db: Session = Depends(get_db)):
    email_clean = req.email.lower().strip()
    u = db.query(GameCoinUser).filter(func.lower(GameCoinUser.email) == email_clean).with_for_update().first()
    if not u: u = GameCoinUser(email=email_clean, rut="N/A", name="Usuario Manual", saldo=0); db.add(u)
    if req.accion == "add": u.saldo += req.monto
    elif req.accion == "set": u.saldo = req.monto
    elif req.accion == "subtract": u.saldo = max(0, u.saldo - req.monto)
    db.commit()
    return {"status": "ok", "nuevo_saldo": u.saldo}

@app.post("/admin/sync_clients", dependencies=[Depends(verify_admin)])
async def sync_clients(db: Session = Depends(get_db)):
    res = await logic.sync_jumpseller_customers_logic(db)
    return {"status": "ok", "data": res}

@app.post("/admin/config", dependencies=[Depends(verify_admin)])
def update_config(req: ConfigRequest, db: Session = Depends(get_db)):
    def set_cfg(k, v):
        item = db.query(SystemConfig).filter(SystemConfig.key == k).first()
        if not item: db.add(SystemConfig(key=k, value=v))
        else: item.value = v
    set_cfg("JUMPSELLER_API_TOKEN", req.api_token.strip())
    set_cfg("JUMPSELLER_STORE", req.store_login.lower().replace("https://", "").replace(".jumpseller.com", "").strip())
    set_cfg("JUMPSELLER_HOOKS_TOKEN", req.hooks_token.strip())
    db.commit()
    return {"status": "ok"}

@app.post("/api/jumpseller/webhook")
async def jumpseller_webhook(request: Request, x_jumpseller_hmac_sha256: str = Header(None), db: Session = Depends(get_db)):
    hook_token = logic.get_db_config(db, "JUMPSELLER_HOOKS_TOKEN")
    if not hook_token: return {"status": "ignored"}
    body_bytes = await request.body()
    signature = hmac.new(hook_token.encode(), body_bytes, hashlib.sha256).digest()
    import base64
    if base64.b64encode(signature).decode() != x_jumpseller_hmac_sha256: raise HTTPException(401)
    try:
        data = json.loads(body_bytes)
        order = data.get("order", {})
        if order.get("status") == "paid" and order.get("customer", {}).get("email"):
            points = int(float(order.get("total", 0)) / 1000)
            if points > 0:
                email = order.get("customer", {}).get("email", "").lower().strip()
                u = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
                if not u: u = GameCoinUser(email=email, name=f"{order.get('customer',{}).get('name','')} {order.get('customer',{}).get('surname','')}".strip(), saldo=0); db.add(u)
                u.saldo += points; db.commit()
    except: pass
    return {"status": "ok"}
