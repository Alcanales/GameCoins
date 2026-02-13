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

# --- IMPORTS RELATIVOS (ESTRUCTURA PARA RENDER) ---
from .database import engine, Base, get_db, SessionLocal
from .models import GameCoinUser, SystemConfig
from .config import settings
from .schemas import LoginRequest, BalanceAdjustment, CanjeRequest, TokenResponse
from . import services

# Configuración de Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- MIGRACIÓN SEGURA ---
def run_migrations():
    """Crea las tablas si no existen y maneja el reset único de historial."""
    try:
        # Asegura la creación de tablas (gamecoins, system_configs)
        Base.metadata.create_all(bind=engine)
        
        db = SessionLocal()
        # Verificar si ya se realizó el reset de puntos para 2026
        reset_done = db.query(SystemConfig).filter(SystemConfig.key == "reset_puntos_2026").first()
        
        if not reset_done:
            logger.info("⚠️ Ejecutando Reset Único de historico_canjeado...")
            # Reinicia el historial de canje a 0
            db.execute(text("UPDATE gamecoins SET historico_canjeado = 0;"))
            # Registra que el reset ya se hizo
            db.add(SystemConfig(key="reset_puntos_2026", value="completed"))
            db.commit()
            logger.info("✅ Reset histórico completado exitosamente.")
        
        db.close()
        logger.info("✅ Conexión a Base de Datos y Migraciones listas.")
    except Exception as e:
        logger.error(f"❌ Error en la fase de inicio/migración: {e}")

# --- SCHEDULER (TAREAS PROGRAMADAS) ---
scheduler = AsyncIOScheduler()

async def auto_sync_job():
    """Tarea automática de las 23:30 para sincronizar clientes con Jumpseller."""
    logger.info("⏰ Iniciando Sincronización Automática Programada...")
    db = SessionLocal()
    try:
        await services.sync_users_to_db(db)
        logger.info("✅ Auto-Sync completado.")
    except Exception as e:
        logger.error(f"❌ Error en Auto-Sync: {e}")
    finally:
        db.close()

# --- CICLO DE VIDA DE LA APP (LIFESPAN) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Ejecutar migraciones y verificaciones al arrancar
    run_migrations()
    
    # 2. Configurar y arrancar el programador de tareas
    scheduler.add_job(auto_sync_job, 'cron', hour=23, minute=30)
    scheduler.start()
    logger.info("🚀 Servidor GameQuest Online y Scheduler activo")
    
    yield
    
    # 3. Apagar scheduler al cerrar la app
    scheduler.shutdown()

# --- INICIALIZACIÓN DE FASTAPI ---
app = FastAPI(title="GameQuest Vault API", lifespan=lifespan)

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Permitir acceso desde Jumpseller
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- SISTEMA DE AUTENTICACIÓN ---

@app.post("/api/auth/login", response_model=TokenResponse)
def login(creds: LoginRequest, db: Session = Depends(get_db)):
    """Valida credenciales y persiste el token en la DB para soportar múltiples workers."""
    if creds.username == settings.ADMIN_USER and creds.password == settings.ADMIN_PASS:
        token = str(uuid.uuid4())
        
        # Guardar o actualizar el token en la tabla system_configs
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
    """Verificador de seguridad para endpoints administrativos."""
    if not authorization: 
        raise HTTPException(401, "Token de autorización requerido")
    
    try:
        scheme, token = authorization.split()
        if scheme.lower() != 'bearer':
            raise HTTPException(401, "Esquema de autenticación inválido")

        # Validar token contra el registro en la base de datos
        stored_session = db.query(SystemConfig).filter(SystemConfig.key == "admin_token").first()
        
        if not stored_session or stored_session.value != token:
            raise HTTPException(403, "Sesión inválida o expirada")
            
        return settings.ADMIN_USER
    except Exception:
        raise HTTPException(401, "Token malformado")

# --- ENDPOINTS ADMINISTRATIVOS ---

@app.get("/admin/users")
def list_users(
    limit: int = 100, 
    search: Optional[str] = None, 
    only_balance: bool = False,
    db: Session = Depends(get_db), 
    admin: str = Depends(verify_admin_token)
):
    """Listado y búsqueda de usuarios (Email, Nombre, Apellido)."""
    query = db.query(GameCoinUser)

    if only_balance:
        query = query.filter(GameCoinUser.saldo > 0)

    if search and len(search.strip()) > 0:
        term = f"%{search.strip().lower()}%"
        query = query.filter(
            or_(
                GameCoinUser.email.ilike(term),
                GameCoinUser.name.ilike(term),
                GameCoinUser.surname.ilike(term)
            )
        )

    # Cálculo de métricas globales para los indicadores de la Bóveda
    total_points = db.query(func.sum(GameCoinUser.saldo)).scalar() or 0
    total_count = db.query(func.count(GameCoinUser.id)).scalar() or 0
    total_redeemed = db.query(func.sum(GameCoinUser.historico_canjeado)).scalar() or 0
    
    users = query.order_by(GameCoinUser.saldo.desc()).limit(limit).all()
    
    return {
        "users": users,
        "totalPointsInVault": total_points,
        "totalCount": total_count,
        "totalRedeemed": total_redeemed
    }

@app.post("/admin/adjust_balance")
def adjust_balance(req: BalanceAdjustment, db: Session = Depends(get_db), admin: str = Depends(verify_admin_token)):
    """Ajuste manual de saldos desde el panel administrativo."""
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
    return {"status": "success", "new_balance": user.saldo}

@app.post("/admin/sync_users")
async def manual_sync(db: Session = Depends(get_db), admin: str = Depends(verify_admin_token)):
    """Sincronización manual masiva con la API de Jumpseller."""
    result = await services.sync_users_to_db(db)
    return {"status": "success", "details": result}

# --- ENDPOINTS PÚBLICOS ---

@app.get("/health")
def health(): 
    return {"status": "online"}

@app.get("/api/public/balance/{email}")
def get_public_balance(email: str, db: Session = Depends(get_db)):
    """Consulta de saldo rápida para clientes."""
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email.lower().strip()).first()
    return {"saldo": user.saldo if user else 0}

@app.post("/api/public/analyze_buylist")
async def analyze_buylist_endpoint(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """Procesamiento de archivos CSV de Buylist (ManaBox/CK)."""
    content = await file.read()
    return await services.analizar_manabox_ck(content, db)