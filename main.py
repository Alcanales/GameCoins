import secrets
import random
import string
import hmac
import hashlib
import base64
from fastapi import FastAPI, UploadFile, File, HTTPException, Header, Depends, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel

from config import settings
import services as logic
from database import engine, Base, get_db
from models import GameCoinUser 

# Crear tablas al iniciar
Base.metadata.create_all(bind=engine)

# --- MIDDLEWARE DE SEGURIDAD ---
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        return response

app = FastAPI(title="GameQuest GameCoins API", version="3.1")

# --- MIDDLEWARES ---
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=["gamecoins.onrender.com", "gamequest.cl", "*.gamequest.cl", "localhost", "127.0.0.1"])

origins = [
    "http://localhost", "http://localhost:8000",
    "https://game-quest.jumpseller.com", "https://game-quest.cl",       
    "https://www.game-quest.cl", "https://gamequest.cl", "https://www.gamequest.cl"    
]
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# --- DEPENDENCIAS ---
def verificar_admin(x_admin_user: str = Header(None), x_admin_pass: str = Header(None)):
    if not (x_admin_user and x_admin_pass): raise HTTPException(401)
    if not (secrets.compare_digest(x_admin_user, settings.ADMIN_USER) and secrets.compare_digest(x_admin_pass, settings.ADMIN_PASS)): raise HTTPException(401)
    return True

# --- MODELOS PYDANTIC ---
class UpdateRequest(BaseModel):
    email: str; monto: int; accion: str

class CanjeRequest(BaseModel):
    email: str; monto: int

class BuylistSubmitRequest(BaseModel):
    cliente: dict; cartas: list; total_clp: str; total_gc: str

@app.get("/")
def home(): return {"status": "Online"}

# --- ENDPOINTS BUYLIST ---
@app.post("/api/analizar")
def buylist_analisis(file: UploadFile = File(...), mode: str = Form("client")):
    content = file.file.read()
    if len(content) > 5*1024*1024: raise HTTPException(413, "El archivo es muy grande (Max 5MB)")
    
    # Llama a la lógica avanzada (Estacas/Staples)
    res = logic.procesar_csv_manabox(content, internal_mode=(mode == "internal"))
    
    if isinstance(res, dict) and "error" in res: raise HTTPException(400, res["error"])
    return {"data": res}

@app.post("/api/enviar_buylist")
def submit_buylist(payload: BuylistSubmitRequest):
    return logic.enviar_correo_buylist(payload.cliente, payload.cartas, payload.total_clp, payload.total_gc)

# --- ENDPOINTS CLIENTE ---
@app.get("/api/saldo/{email}")
def consultar_saldo(email: str, db: Session = Depends(get_db)):
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email.strip().lower()).first()
    return {"email": email, "saldo": user.saldo if user else 0}

@app.post("/api/canjear")
def canjear_puntos(payload: CanjeRequest, db: Session = Depends(get_db)):
    email = payload.email.strip().lower(); monto = int(payload.monto)
    if monto <= 0: raise HTTPException(400, "Monto inválido")
    
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email).with_for_update().first()
    if not user or user.saldo < monto: raise HTTPException(400, "Saldo insuficiente")
    
    user.saldo -= monto
    try:
        db.commit() 
        codigo = f"GC-{''.join(random.choices(string.ascii_uppercase + string.digits, k=4))}"
        if logic.crear_cupon_jumpseller(codigo, monto):
            return {"status": "ok", "codigo": codigo, "nuevo_saldo": user.saldo}
        else:
            user.saldo += monto; db.commit() # Rollback manual
            raise HTTPException(502, "Error al crear cupón en Jumpseller")
    except Exception:
        db.rollback(); raise HTTPException(500, "Error interno")

# --- ENDPOINTS ADMIN ---
@app.get("/admin/users", dependencies=[Depends(verificar_admin)])
def listar_usuarios(db: Session = Depends(get_db)):
    return db.query(GameCoinUser).order_by(GameCoinUser.updated_at.desc()).limit(200).all()

@app.post("/admin/update", dependencies=[Depends(verificar_admin)])
def actualizar_saldo_manual(payload: UpdateRequest, db: Session = Depends(get_db)):
    email = payload.email.strip().lower()
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
    if not user:
        if payload.accion == "restar": raise HTTPException(404, "Usuario no existe")
        user = GameCoinUser(email=email, saldo=0, rut=f"MAN-{email}")
        db.add(user)
    
    if payload.accion == "sumar": user.saldo += payload.monto
    elif payload.accion == "restar": user.saldo = max(0, user.saldo - payload.monto)
    db.commit()
    return {"msg": "OK", "nuevo_saldo": user.saldo}

@app.post("/admin/sync_clients", dependencies=[Depends(verificar_admin)])
def trigger_sync(db: Session = Depends(get_db)):
    return logic.sincronizar_clientes_jumpseller(db, GameCoinUser)

# --- WEBHOOK DE PAGO AUTOMÁTICO ---
@app.post("/webhook/order_created")
async def procesar_pago_gamecoins(request: Request, db: Session = Depends(get_db)):
    # 1. Leer Body
    body = await request.body()
    try: payload = await request.json()
    except: return {"status": "error_json"}

    # 2. Seguridad HMAC
    if settings.JUMPSELLER_HOOKS_TOKEN:
        sig = request.headers.get("Jumpseller-Hmac-Sha256", "")
        calc = base64.b64encode(hmac.new(settings.JUMPSELLER_HOOKS_TOKEN.encode(), body, hashlib.sha256).digest()).decode()
        if not secrets.compare_digest(sig, calc): return {"status": "ignored_signature"}

    # 3. Lógica
    order = payload.get("order", {})
    payment = order.get("payment_method_name", "")
    status = order.get("status", "")
    
    # 4. Procesamiento
    if "GameCoins" in payment and status == "Pending":
        email = order.get("customer", {}).get("email", "").strip().lower()
        total = float(order.get("total", 0))
        
        user = db.query(GameCoinUser).filter(GameCoinUser.email == email).with_for_update().first()
        
        if user and user.saldo >= total:
            user.saldo -= int(total)
            db.commit()
            logic.actualizar_orden_jumpseller(order.get("id"), "Paid", f"Pago Total GC: -${int(total)}")
            return {"status": "pagado_exitoso"}
        else:
            logic.actualizar_orden_jumpseller(order.get("id"), "Canceled", "Saldo insuficiente GameCoins")
            
    return {"status": "ok"}