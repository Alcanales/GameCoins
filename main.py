import magic
import json
import secrets
import random
import string
import hmac
import hashlib
from fastapi import Request
import time
import aiohttp
from fastapi import FastAPI, UploadFile, File, HTTPException, Form, BackgroundTasks, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text, inspect

from config import settings
from database import engine, Base, get_db
import services as logic
from schemas import BuylistSubmitRequest, UpdateRequest, CanjeRequest
from models import GameCoinUser

Base.metadata.create_all(bind=engine)

app = FastAPI(title=settings.APP_NAME, version="11.1-GAMECOINS-DB")

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- AUTO-HEALING DB (Actualizado para tabla 'gamecoins') ---
@app.on_event("startup")
def startup_db_check():
    """Verifica y repara columnas faltantes en la tabla 'gamecoins'."""
    print("🏥 Verificando salud de la Base de Datos...")
    try:
        inspector = inspect(engine)
        # CAMBIO: Verificamos la tabla "gamecoins"
        if inspector.has_table("gamecoins"):
            cols = [c['name'] for c in inspector.get_columns('gamecoins')]
            with engine.connect() as conn:
                if 'name' not in cols:
                    print("➕ Agregando columna 'name'")
                    conn.execute(text("ALTER TABLE gamecoins ADD COLUMN name VARCHAR;"))
                if 'rut' not in cols:
                    print("➕ Agregando columna 'rut'")
                    conn.execute(text("ALTER TABLE gamecoins ADD COLUMN rut VARCHAR;"))
                conn.commit()
            print("✅ Tabla 'gamecoins' saludable.")
        else:
            print("ℹ️ La tabla 'gamecoins' se creará automáticamente por SQLAlchemy.")
    except Exception as e:
        print(f"⚠️ Error no crítico en chequeo DB: {e}")

# --- SECURITY ---
def verify_admin(x_admin_user: str = Header(None), x_admin_pass: str = Header(None)):
    if not (x_admin_user and x_admin_pass): raise HTTPException(401)
    auth_ok = (secrets.compare_digest(x_admin_user, settings.ADMIN_USER) and secrets.compare_digest(x_admin_pass, settings.ADMIN_PASS)) or \
              (secrets.compare_digest(x_admin_user, settings.MASTER_USER) and secrets.compare_digest(x_admin_pass, settings.MASTER_PASS))
    if not auth_ok: raise HTTPException(401)

def check_maintenance():
    if settings.MAINTENANCE_MODE_CANJE: raise HTTPException(503, "Sistema en mantenimiento")

# --- ENDPOINTS ---
@app.get("/")
def health(): return {"status": "ok", "version": "11.1"}

@app.get("/api/public/status")
def status(): return {"status": "maintenance" if settings.MAINTENANCE_MODE_CANJE else "operational"}

@app.get("/api/public/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    if not email: return {"saldo": 0, "historico_canjeado": 0}
    u = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
    return {"email": email, "saldo": int(u.saldo if u else 0), "historico_canjeado": int(u.historico_canjeado if u else 0)}

@app.post("/api/analizar")
async def analyze(file: UploadFile = File(...), mode: str = Form("client")):
    content = await file.read()
    res = await logic.procesar_csv_logic(content, internal_mode=(mode=="internal"))
    if isinstance(res, dict) and "error" in res: raise HTTPException(400, res["error"])
    return {"data": res}

@app.post("/api/enviar_buylist", dependencies=[Depends(check_maintenance)])
async def send_buylist(background_tasks: BackgroundTasks, payload: str = Form(...), csv_file: UploadFile = File(...)):
    try:
        data = json.loads(payload)
        req = BuylistSubmitRequest(**data)
    except: raise HTTPException(422)
    content = await csv_file.read()
    background_tasks.add_task(logic.enviar_correo_dual, req.cliente.model_dump(), [c.model_dump() for c in req.cartas], req.total_clp, req.total_gc, content, csv_file.filename)
    return {"status": "received"}

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

@app.post("/admin/sync_clients", dependencies=[Depends(verify_admin)])
async def sync_clients(db: Session = Depends(get_db)):
    try:
        customers = await logic.sync_jumpseller_customers_logic()
        new, upd = 0, 0
        for c in customers:
            u = db.query(GameCoinUser).filter(GameCoinUser.email == c['email']).first()
            if not u:
                db.add(GameCoinUser(email=c['email'], name=c['name'])); new += 1
            elif u.name != c['name']:
                u.name = c['name']; upd += 1
        db.commit()
        return {"status": "ok", "nuevos": new, "actualizados": upd}
    except Exception as e:
        db.rollback(); raise HTTPException(500, str(e))

@app.post("/admin/canje", dependencies=[Depends(verify_admin), Depends(check_maintenance)])
async def canje(req: CanjeRequest, db: Session = Depends(get_db)):
    u = db.query(GameCoinUser).filter(GameCoinUser.email == req.email).with_for_update().first()
    if not u or u.saldo < req.monto: raise HTTPException(400, "Saldo insuficiente")
    
    u.saldo -= req.monto
    u.historico_canjeado += req.monto
    db.commit()
    
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    code = f"GQ-{suffix}"
    
    async with aiohttp.ClientSession() as s:
        res = await logic.crear_cupon_jumpseller(s, code, req.monto, req.email)
        if not res or "promotion" not in res:
             u.saldo += req.monto; u.historico_canjeado -= req.monto; db.commit()
             return {"status": "error", "mensaje": "Fallo Jumpseller"}
        return {"status": "ok", "cupon_codigo": code}
    # --- WEBHOOKS JUMPSELLER (Acumulación Automática) ---
@app.post("/api/jumpseller/webhook")
async def jumpseller_webhook(request: Request, x_jumpseller_hmac_sha256: str = Header(None), db: Session = Depends(get_db)):
    """
    Recibe notificaciones de Jumpseller.
    Si la orden está 'paid', calcula puntos y los suma al usuario.
    """
    if not settings.JUMPSELLER_HOOKS_TOKEN:
        print("⚠️ Webhook recibido pero sin Token configurado. Ignorando.")
        return {"status": "ignored", "reason": "no_token_configured"}

    # 1. Obtener el cuerpo raw para validar firma
    body_bytes = await request.body()
    
    # 2. Verificar Firma de Seguridad (HMAC)
    # Jumpseller firma el mensaje usando tu token secreto.
    signature = hmac.new(
        settings.JUMPSELLER_HOOKS_TOKEN.encode(),
        body_bytes,
        hashlib.sha256
    ).digest()
    
    import base64
    calculated_hmac = base64.b64encode(signature).decode()
    
    if calculated_hmac != x_jumpseller_hmac_sha256:
        print(f"⛔ Firma inválida. Recibido: {x_jumpseller_hmac_sha256} | Calculado: {calculated_hmac}")
        raise HTTPException(401, "Firma inválida")

    # 3. Procesar Datos
    try:
        data = json.loads(body_bytes)
        order = data.get("order", {})
        status = order.get("status")
        email = order.get("customer", {}).get("email")
        
        print(f"🔔 Webhook Orden #{order.get('id')} - Status: {status} - Email: {email}")

        if status == "paid" and email:
            total_clp = float(order.get("total", 0))
            
           
            puntos_ganados = int(total_clp * 1) # 1% del total en puntos
            
            # Buscar usuario
            user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
            if not user:
                # Si no existe, lo creamos
                name = f"{order.get('customer', {}).get('name', '')} {order.get('customer', {}).get('surname', '')}"
                user = GameCoinUser(email=email, name=name.strip(), saldo=0)
                db.add(user)
            
            # Sumar puntos (evitar duplicados requeriría lógica extra de guardar IDs de orden, esto es básico)
            user.saldo += puntos_ganados
            db.commit()
            print(f"✅ {puntos_ganados} Puntos sumados a {email}. Nuevo saldo: {user.saldo}")
            
    except Exception as e:
        print(f"❌ Error procesando webhook: {e}")
        return {"status": "error"}

    return {"status": "ok"}