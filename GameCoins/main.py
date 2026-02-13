import logging
import uuid
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text, func, or_
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from typing import Optional

# --- IMPORTS RELATIVOS ---
from .database import engine, Base, get_db, SessionLocal
# USAMOS LA NUEVA CLASE GamePointUser
from .models import GamePointUser, SystemConfig
from .config import settings
from .schemas import LoginRequest, BalanceAdjustment, CanjeRequest, TokenResponse
from . import services

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- MIGRACIONES SEGURAS ---
def run_migrations():
    """Crea las tablas al iniciar."""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Tablas verificadas/creadas (GamePointUser & SystemConfig)")
    except Exception as e:
        logger.error(f"❌ Error crítico en migración: {e}")

# --- SCHEDULER ---
scheduler = AsyncIOScheduler()

async def auto_sync_job():
    logger.info("⏰ Ejecutando Sincronización Automática (23:30)...")
    db = SessionLocal()
    try:
        await services.sync_users_to_db(db)
    except Exception as e:
        logger.error(f"Error en Auto-Sync: {e}")
    finally:
        db.close()

# --- LIFESPAN (ARRANQUE) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Crear tablas al iniciar
    run_migrations()
    
    # 2. Iniciar tareas programadas
    scheduler.add_job(auto_sync_job, 'cron', hour=23, minute=30)
    scheduler.start()
    logger.info("✅ Servidor Online y Scheduler iniciado")
    yield
    scheduler.shutdown()

app = FastAPI(title="GameQuest Vault API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- LOGIN ---
@app.post("/api/auth/login", response_model=TokenResponse)
def login(creds: LoginRequest, db: Session = Depends(get_db)):
    if creds.username == settings.ADMIN_USER and creds.password == settings.ADMIN_PASS:
        token = str(uuid.uuid4())
        session_entry = db.query(SystemConfig).filter(SystemConfig.key == "admin_token").first()
        if not session_entry:
            session_entry = SystemConfig(key="admin_token", value=token)
            db.add(session_entry)
        else:
            session_entry.value = token
        db.commit()
        return {"access_token": token, "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Credenciales incorrectas")

def verify_admin_token(authorization: str = Header(None), db: Session = Depends(get_db)):
    if not authorization: raise HTTPException(401, "Token requerido")
    try:
        scheme, token = authorization.split()
        if scheme.lower() != 'bearer': raise HTTPException(401, "Esquema inválido")
        stored_session = db.query(SystemConfig).filter(SystemConfig.key == "admin_token").first()
        if not stored_session or stored_session.value != token:
            raise HTTPException(403, "Sesión inválida")
        return settings.ADMIN_USER
    except:
        raise HTTPException(401, "Token malformado")

# --- ENDPOINTS PRINCIPALES (GamePointUser) ---

@app.get("/admin/users")
def list_users(limit: int = 100, search: Optional[str] = None, only_balance: bool = False, db: Session = Depends(get_db), admin: str = Depends(verify_admin_token)):
    query = db.query(GamePointUser)

    if only_balance:
        query = query.filter(GamePointUser.saldo > 0)

    if search and len(search.strip()) > 0:
        term = f"%{search.strip().lower()}%"
        query = query.filter(
            or_(
                GamePointUser.email.ilike(term),
                GamePointUser.name.ilike(term),
                GamePointUser.surname.ilike(term)
            )
        )

    # Métricas Globales
    total_points = db.query(func.sum(GamePointUser.saldo)).scalar() or 0
    total_count = db.query(func.count(GamePointUser.id)).scalar() or 0
    total_redeemed = db.query(func.sum(GamePointUser.historico_canjeado)).scalar() or 0
    
    users = query.order_by(GamePointUser.saldo.desc()).limit(limit).all()
    
    return {
        "users": users,
        "totalPointsInVault": total_points,
        "totalCount": total_count,
        "totalRedeemed": total_redeemed
    }

@app.post("/admin/adjust_balance")
def adjust_balance(req: BalanceAdjustment, db: Session = Depends(get_db), admin: str = Depends(verify_admin_token)):
    email = req.email.lower().strip()
    user = db.query(GamePointUser).filter(GamePointUser.email == email).first()
    
    # Si no existe, se crea al vuelo (aunque el sync debería haberlo hecho)
    if not user:
        user = GamePointUser(email=email, saldo=0)
        db.add(user)
    
    if req.operation == 'add':
        user.saldo += req.amount
        user.historico_acumulado += req.amount
    elif req.operation == 'subtract':
        user.saldo = max(0, user.saldo - req.amount)
        user.historico_canjeado += req.amount
    db.commit()
    return {"status": "success", "new_balance": user.saldo}

@app.post("/admin/sync_users")
async def manual_sync(db: Session = Depends(get_db), admin: str = Depends(verify_admin_token)):
    """Sincroniza y agrega clientes faltantes."""
    result = await services.sync_users_to_db(db)
    return {"status": "success", "details": result}

# --- ENDPOINTS PÚBLICOS ---

@app.get("/health")
def health(): return {"status": "online"}

@app.get("/api/public/balance/{email}")
def get_public_balance(email: str, db: Session = Depends(get_db)):
    user = db.query(GamePointUser).filter(GamePointUser.email == email.lower().strip()).first()
    return {"saldo": user.saldo if user else 0}

@app.post("/api/public/analyze_buylist")
async def analyze_buylist_endpoint(file: UploadFile = File(...), db: Session = Depends(get_db)):
    content = await file.read()
    return await services.analizar_manabox_ck(content, db)

@app.post("/api/canje")
async def procesar_canje(req: CanjeRequest, db: Session = Depends(get_db), x_store_token: str = Header(None)):
    if x_store_token != settings.STORE_TOKEN: raise HTTPException(403, "Token Inválido")
    if settings.MAINTENANCE_MODE_CANJE: raise HTTPException(503, "Mantenimiento")

    user = db.query(GamePointUser).filter(GamePointUser.email == req.email.lower().strip()).first()
    if not user or user.saldo < req.monto: raise HTTPException(400, "Saldo insuficiente")
    if req.monto < settings.MIN_CANJE: raise HTTPException(400, f"Mínimo ${settings.MIN_CANJE}")

    try:
        user.saldo -= req.monto
        user.historico_canjeado += req.monto
        db.commit()
    except:
        db.rollback()
        raise HTTPException(500, "Error DB")

    cupon = await services.create_jumpseller_coupon(user.email, req.monto)
    if not cupon:
        user.saldo += req.monto 
        user.historico_canjeado -= req.monto
        db.commit()
        raise HTTPException(502, "Error Jumpseller")

    return {"status": "ok", "cupon_codigo": cupon, "nuevo_saldo": user.saldo}

# --- ENDPOINT DE EMERGENCIA (Usar si no aparece la tabla) ---
@app.get("/admin/force_create_tables")
def force_create_tables(db: Session = Depends(get_db)):
    try:
        Base.metadata.create_all(bind=engine)
        return {"status": "success", "message": "Tablas (re)creadas forzosamente. Revisa DBeaver."}
    except Exception as e:
        raise HTTPException(500, f"Error creando tablas: {str(e)}")