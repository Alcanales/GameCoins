import random
import string
import hmac
import hashlib
import base64
from fastapi import FastAPI, UploadFile, File, HTTPException, Header, Depends, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel

from config import settings
from database import engine, Base, get_db
from models import GameCoinUser
import services as logic

# Inicialización DB
Base.metadata.create_all(bind=engine)

app = FastAPI(title="GameQuest GameCoins API", version="2.0")

# CORS Setup
origins = [
    "http://localhost",
    "http://localhost:8000",
    "https://game-quest.jumpseller.com", 
    "https://game-quest.cl",       
    "https://www.game-quest.cl"    
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

# --- DEPENDENCIAS ---
def verificar_admin(x_admin_user: str = Header(None), x_admin_pass: str = Header(None)):
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Credenciales de administrador inválidas")
    return True

class UpdateRequest(BaseModel):
    email: str
    monto: int
    accion: str

class CanjeRequest(BaseModel):
    email: str
    monto: int

class BuylistSubmitRequest(BaseModel):
    cliente: dict
    cartas: list
    total_clp: str
    total_gc: str

# --- ENDPOINTS PÚBLICOS ---

@app.get("/")
def home():
    return {"status": "Online", "version": "2.0"}

@app.post("/api/analizar")
async def buylist_analisis(file: UploadFile = File(...), mode: str = Form("client")):
    """Procesa archivo CSV de ManaBox y retorna evaluación."""
    content = await file.read()
    is_internal = (mode == "internal")
    res = logic.procesar_csv_manabox(content, internal_mode=is_internal)
    if isinstance(res, dict) and "error" in res:
        raise HTTPException(400, res["error"])
    return {"data": res}

@app.post("/api/enviar_buylist")
def submit_buylist(payload: BuylistSubmitRequest):
    """Envía correo con la solicitud de venta."""
    return logic.enviar_correo_buylist(payload.cliente, payload.cartas, payload.total_clp, payload.total_gc)

@app.get("/api/saldo/{email}")
def consultar_saldo(email: str, db: Session = Depends(get_db)):
    """Consulta saldo de usuario por email."""
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email.strip().lower()).first()
    return {"email": email, "saldo": user.saldo if user else 0}

@app.post("/api/canjear")
def canjear_puntos(payload: CanjeRequest, db: Session = Depends(get_db)):
    """
    Canje transaccional de puntos por cupón.
    Estrategia: Descontar primero (Lock) -> Crear Cupón -> Si falla, Rollback.
    """
    email = payload.email.strip().lower()
    monto = int(payload.monto)
    
    if monto <= 0: raise HTTPException(400, "Monto inválido")
    
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email).with_for_update().first()
    
    if not user: raise HTTPException(404, "Usuario no encontrado")
    if user.saldo < monto: raise HTTPException(400, "Saldo insuficiente")
    
    user.saldo -= monto
    
    try:
        db.commit() 
        
        suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
        codigo = f"GC-{suffix}"
        
        exito_cupon = logic.crear_cupon_jumpseller(codigo, monto)
        
        if exito_cupon:
            return {"status": "ok", "codigo": codigo, "nuevo_saldo": user.saldo}
        else:
            user_refund = db.query(GameCoinUser).filter(GameCoinUser.email == email).with_for_update().first()
            if user_refund:
                user_refund.saldo += monto
                db.commit()
            raise HTTPException(502, "Error al generar cupón en Jumpseller. Tus puntos han sido devueltos.")

    except HTTPException as he:
        raise he
    except Exception as e:
        db.rollback() 
        print(f"CRITICAL ERROR CANJE: {e}")
        raise HTTPException(500, "Error interno del servidor")

# --- ENDPOINTS ADMIN ---

@app.get("/admin/users", dependencies=[Depends(verificar_admin)])
def listar_usuarios(db: Session = Depends(get_db)):
    return db.query(GameCoinUser).order_by(GameCoinUser.updated_at.desc()).limit(200).all()

@app.post("/admin/update", dependencies=[Depends(verificar_admin)])
def actualizar_saldo_manual(payload: UpdateRequest, db: Session = Depends(get_db)):
    email = payload.email.strip().lower()
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
    
    if not user:
        if payload.accion == "restar":
            raise HTTPException(404, "Usuario no existe y no se puede restar saldo.")
        user = GameCoinUser(email=email, saldo=0, name="Manual", surname="User", rut=f"MAN-{email}")
        db.add(user)
    
    if payload.accion == "sumar":
        user.saldo += payload.monto
    elif payload.accion == "restar":
        user.saldo = max(0, user.saldo - payload.monto)
    
    db.commit()
    return {"msg": "Saldo actualizado", "nuevo_saldo": user.saldo}

@app.post("/admin/sync_clients", dependencies=[Depends(verificar_admin)])
def trigger_sync(db: Session = Depends(get_db)):
    """Sincronización manual de clientes desde Jumpseller."""
    return logic.sincronizar_clientes_jumpseller(db, GameCoinUser)

# --- WEBHOOKS ---

@app.post("/webhook/order_created")
async def procesar_pago_gamecoins(request: Request, db: Session = Depends(get_db)):
    """Webhook: Descuenta saldo si el cliente paga con 'GameCoins'."""
    body_bytes = await request.body()
    signature = request.headers.get("Jumpseller-Hmac-Sha256")
    
    if settings.JUMPSELLER_HOOKS_TOKEN and signature:
        calculated = base64.b64encode(
            hmac.new(settings.JUMPSELLER_HOOKS_TOKEN.encode(), body_bytes, hashlib.sha256).digest()
        ).decode()
        if signature != calculated:
            return {"status": "ignored", "reason": "invalid_signature"}

    try:
        payload = await request.json()
    except:
        return {"status": "error", "msg": "Invalid JSON"}

    order = payload.get("order", {})
    order_id = order.get("id")
    payment_method = order.get("payment_method_name", "")
    status = order.get("status", "")
    
    if "GameCoins" in payment_method and status == "Pending":
        customer_email = order.get("customer", {}).get("email", "").strip().lower()
        total_order = float(order.get("total", 0))
        
        user = db.query(GameCoinUser).filter(GameCoinUser.email == customer_email).with_for_update().first()
        
        if user and user.saldo >= total_order:
            user.saldo -= int(total_order)
            db.commit()
            
            nota = f"PAGO EXITOSO GAMECOINS. Descontado: ${int(total_order)}. Saldo restante: ${user.saldo}"
            logic.actualizar_orden_jumpseller(order_id, "Paid", nota)
            print(f"💰 Pago exitoso Orden #{order_id} - {customer_email}")
        else:
            saldo_actual = user.saldo if user else 0
            msg = f"Rechazado. Saldo insuficiente (Tiene: ${saldo_actual}, Requiere: ${int(total_order)})"
            logic.actualizar_orden_jumpseller(order_id, "Canceled", msg)
            print(f"⛔ Pago rechazado Orden #{order_id} - {customer_email}")

    return {"status": "ok"}
