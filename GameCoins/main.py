import logging
import uuid
from fastapi import FastAPI, HTTPException, Depends, Header, UploadFile, File, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional

from .database import engine, Base, get_db, SessionLocal
from .models import GameCoinUser
from .config import settings
from .schemas import LoginRequest, BalanceAdjustment, CanjeRequest, TokenResponse
from . import services

# Configuración básica de logs
logging.basicConfig(level=logging.INFO)

# --- INICIALIZACIÓN DE DB --
def run_migrations():
    try:
        Base.metadata.create_all(bind=engine)
        db = SessionLocal()
        db.execute(text("ALTER TABLE gamecoins ADD COLUMN IF NOT EXISTS historico_canjeado INTEGER DEFAULT 0;"))
        db.execute(text("ALTER TABLE gamecoins ADD COLUMN IF NOT EXISTS historico_acumulado INTEGER DEFAULT 0;"))
        db.commit()
        db.close()
    except Exception as e:
        logging.error(f"Migration Error: {e}")
run_migrations()

app = FastAPI(title="GameQuest Vault API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- SEGURIDAD (TOKEN STORAGE EN MEMORIA) ---
active_sessions = {}

@app.post("/api/auth/login", response_model=TokenResponse)
def login(creds: LoginRequest):
    if creds.username == settings.ADMIN_USER and creds.password == settings.ADMIN_PASS:
        token = str(uuid.uuid4())
        active_sessions[token] = creds.username
        return {"access_token": token, "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Credenciales incorrectas")

def verify_admin_token(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Token requerido")
    try:
        scheme, token = authorization.split()
        if scheme.lower() != 'bearer':
            raise HTTPException(status_code=401, detail="Formato de token inválido")
        if token not in active_sessions:
            raise HTTPException(status_code=403, detail="Sesión expirada o inválida")
        return active_sessions[token]
    except ValueError:
        raise HTTPException(status_code=401, detail="Token malformado")

@app.post("/api/auth/logout")
def logout(authorization: str = Header(None)):
    try:
        scheme, token = authorization.split()
        if token in active_sessions:
            del active_sessions[token]
        return {"status": "logged_out"}
    except:
        return {"status": "ignored"}

# --- ENDPOINTS ADMINISTRATIVOS (PROTEGIDOS) ---

@app.get("/admin/users")
def list_users(db: Session = Depends(get_db), admin: str = Depends(verify_admin_token)):
    users = db.query(GameCoinUser).all()
    return {
        "users": users,
        "totalPointsInVault": sum(u.saldo for u in users)
    }

@app.post("/admin/adjust_balance")
def adjust_balance(req: BalanceAdjustment, db: Session = Depends(get_db), admin: str = Depends(verify_admin_token)):
    email = req.email.lower().strip()
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
    
    if not user:
        user = GameCoinUser(email=email, saldo=0)
        db.add(user)
    
    if req.operation == 'add':
        user.saldo += req.amount
        user.historico_acumulado += req.amount
    elif req.operation == 'subtract':
        user.saldo = max(0, user.saldo - req.amount)
        user.historico_canjeado += req.amount
    
    db.commit()
    return {"status": "success", "new_balance": user.saldo, "user": email}

# --- ENDPOINTS PÚBLICOS (CLIENTE) ---

@app.get("/health")
def health():
    return {"status": "online", "mode": "Manabox-Only"}

@app.get("/api/public/balance/{email}")
def get_public_balance(email: str, db: Session = Depends(get_db)):
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email.lower().strip()).first()
    return {"saldo": user.saldo if user else 0}

@app.post("/api/public/analyze_buylist")
async def analyze_buylist_endpoint(file: UploadFile = File(...), db: Session = Depends(get_db)):
    content = await file.read()
    return await services.analizar_manabox_ck(content, db)

# --- ENDPOINT DE CANJE REAL (INTEGRACIÓN JUMPSELLER) ---

@app.post("/api/canje")
async def procesar_canje(req: CanjeRequest, db: Session = Depends(get_db), x_store_token: str = Header(None)):
    """
    1. Verifica Token de Tienda y Mantenimiento.
    2. Verifica Saldo del Usuario.
    3. Descuenta Puntos (Atómico en DB).
    4. Crea Cupón en Jumpseller API.
    5. Retorna Código al Cliente.
    """
    # 1. Validaciones Iniciales
    if x_store_token != settings.STORE_TOKEN:
         raise HTTPException(status_code=403, detail="Store Token Inválido")
    
    if settings.MAINTENANCE_MODE_CANJE:
        raise HTTPException(status_code=503, detail="Sistema de canje en mantenimiento por inventario.")

    # 2. Validación de Saldo
    user = db.query(GameCoinUser).filter(GameCoinUser.email == req.email.lower().strip()).first()
    
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado en sistema de puntos.")
        
    if user.saldo < req.monto:
        raise HTTPException(status_code=400, detail="Saldo insuficiente para este canje.")
    
    if req.monto < settings.MIN_CANJE:
        raise HTTPException(status_code=400, detail=f"El canje mínimo es de ${settings.MIN_CANJE} CLP")

    # 3. Transacción: Descuento de Puntos
    # Primero descontamos para evitar duplicidad si Jumpseller falla (es más seguro devolver que regalar)
    try:
        user.saldo -= req.monto
        user.historico_canjeado += req.monto
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail="Error en base de datos al procesar puntos.")

    # 4. Llamada a API Jumpseller
    cupon_codigo = await services.create_jumpseller_coupon(user.email, req.monto)
    
    if not cupon_codigo:
        # ROLLBACK MANUAL: Si falla Jumpseller, devolvemos los puntos
        user.saldo += req.monto
        user.historico_canjeado -= req.monto
        db.commit()
        raise HTTPException(status_code=502, detail="Error comunicando con Jumpseller. Puntos devueltos.")

    # 5. Éxito
    return {
        "status": "ok", 
        "cupon_codigo": cupon_codigo, 
        "nuevo_saldo": user.saldo,
        "mensaje": f"Canje exitoso. Tu código es {cupon_codigo}"
    }