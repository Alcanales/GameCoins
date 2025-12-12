import os
import random
import string
import hmac
import hashlib
import base64
import json
from fastapi import FastAPI, UploadFile, File, HTTPException, Header, Depends, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel
from database import engine, Base, get_db
from models import GameCoinUser
from logic import procesar_csv_manabox, sincronizar_clientes_jumpseller, crear_cupom_jumpseller, enviar_correo_buylist, actualizar_orden_jumpseller

Base.metadata.create_all(bind=engine)

app = FastAPI()

ADMIN_USER = os.environ.get("ADMIN_USER", "Tomas_1_2_3")
ADMIN_PASS = os.environ.get("ADMIN_PASSWORD", "GameQuest2025_1")
JUMPSELLER_HOOKS_TOKEN = os.environ.get("JUMPSELLER_HOOKS_TOKEN", "")

origins = [
    "http://localhost",
    "http://localhost:8000",
    "https://gamequest.cl",        
    "https://www.gamequest.cl",    
    "https://game-quest.jumpseller.com" 
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,     
    allow_credentials=True,    
    allow_methods=["*"],      
    allow_headers=["*"],       
)

def verificar_admin(x_admin_user: str = Header(None), x_admin_pass: str = Header(None)):
    if x_admin_user != ADMIN_USER or x_admin_pass != ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Credenciales incorrectas")
    return True

# --- CLASES Y MODELOS ---

class UpdateRequest(BaseModel):
    email: str
    monto: int
    accion: str

class DeleteRequest(BaseModel):
    user_id: int

class CanjeRequest(BaseModel):
    email: str
    monto: int

class BuylistSubmitRequest(BaseModel):
    cliente: dict
    cartas: list
    total_clp: str
    total_gc: str

# --- RUTAS ---

@app.get("/")
def home():
    return {"status": "GameQuest API Online"}

@app.post("/api/analizar")
async def buylist_analisis(file: UploadFile = File(...), mode: str = Form("client")):
    content = await file.read()
    is_internal = (mode == "internal")
    res = procesar_csv_manabox(content, internal_mode=is_internal)
    if isinstance(res, dict) and "error" in res:
        raise HTTPException(400, res["error"])
    return {"data": res}

@app.post("/api/enviar_buylist")
def submit_buylist(payload: BuylistSubmitRequest):
    return enviar_correo_buylist(payload.cliente, payload.cartas, payload.total_clp, payload.total_gc)

@app.get("/api/saldo/{email}")
def consultar_saldo(email: str, db: Session = Depends(get_db)):
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email.strip().lower()).first()
    return {"email": email, "saldo": user.saldo if user else 0}

@app.post("/api/canjear")
def canjear_puntos(payload: CanjeRequest, db: Session = Depends(get_db)):
    email = payload.email.strip().lower()
    monto = int(payload.monto)
    
    if monto <= 0:
        raise HTTPException(400, "Monto inválido")
    
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
    
    if not user or user.saldo < monto:
        raise HTTPException(400, "Saldo insuficiente")
    
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    codigo = f"GC-{suffix}"
    
    if crear_cupom_jumpseller(codigo, monto):
        user.saldo -= monto
        db.commit()
        return {"status": "ok", "codigo": codigo, "nuevo_saldo": user.saldo}
    else:
        raise HTTPException(500, "Error creando cupón en la tienda")


@app.get("/admin/users")
def listar_usuarios(auth: bool = Depends(verificar_admin), db: Session = Depends(get_db)):
    return db.query(GameCoinUser).order_by(GameCoinUser.updated_at.desc()).all()

@app.post("/admin/update")
def actualizar_saldo(payload: UpdateRequest, auth: bool = Depends(verificar_admin), db: Session = Depends(get_db)):
    email = payload.email.strip().lower()
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
    
    if not user:
        if payload.accion == "restar":
            raise HTTPException(404, "Usuario nuevo, no se puede restar saldo.")
        user = GameCoinUser(email=email, saldo=0)
        db.add(user)
    
    if payload.accion == "sumar":
        user.saldo += payload.monto
    elif payload.accion == "restar":
        user.saldo -= payload.monto
        if user.saldo < 0: user.saldo = 0
    
    db.commit()
    return {"msg": "Actualizado", "nuevo_saldo": user.saldo}

@app.post("/admin/sync_clients")
def sync_jumpseller(auth: bool = Depends(verificar_admin), db: Session = Depends(get_db)):
    return sincronizar_clientes_jumpseller(db, GameCoinUser)

@app.post("/admin/delete")
def delete_user(payload: DeleteRequest, auth: bool = Depends(verificar_admin), db: Session = Depends(get_db)):
    db.query(GameCoinUser).filter(GameCoinUser.id == payload.user_id).delete()
    db.commit()
    return {"msg": "Eliminado"}


@app.get("/admin/hard_reset_db_emergency") 
def reset_database_emergency():              
    try:
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)
        return {"status": "ok", "msg": "TABLAS CREADAS. Base de datos lista."}
    except Exception as e:
        return {"status": "error", "msg": str(e)}
@app.post("/webhook/order_created")
async def procesar_pago_gamecoins(request: Request, db: Session = Depends(get_db)):
    body_bytes = await request.body()
    signature = request.headers.get("Jumpseller-Hmac-Sha256")
    
    if JUMPSELLER_HOOKS_TOKEN and signature:
        calculated = base64.b64encode(
            hmac.new(JUMPSELLER_HOOKS_TOKEN.encode(), body_bytes, hashlib.sha256).digest()
        ).decode()
        if signature != calculated:
            print(f"ALERTA SEGURIDAD: Firma inválida. Recibido: {signature}")
            return {"status": "ignored", "reason": "invalid_signature"}

    try:
        payload = await request.json()
    except:
        return {"status": "error", "msg": "JSON inválido"}

    order = payload.get("order", {})
    order_id = order.get("id")
    payment_method = order.get("payment_method_name", "")
    status = order.get("status", "")
    
    if "Pending" in status and "GameCoins" in payment_method:
        customer_email = order.get("customer", {}).get("email", "").strip().lower()
        total_order = float(order.get("total", 0))
        
        user = db.query(GameCoinUser).filter(GameCoinUser.email == customer_email).first()
        
        if user and user.saldo >= total_order:
            user.saldo -= int(total_order)
            db.commit()
            nota = f"PAGO EXITOSO GAMECOINS. Descontado: ${int(total_order)}. Restante: ${user.saldo}"
            actualizar_orden_jumpseller(order_id, "paid", nota)
        else:
            saldo_actual = user.saldo if user else 0
            msg = f"Rechazado. Saldo insuficiente (Tiene: ${saldo_actual}, Requiere: ${int(total_order)})"
            actualizar_orden_jumpseller(order_id, "canceled", msg)

    return {"status": "ok"}
