import logging
import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func, or_
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from typing import Optional

from .database import engine, Base, get_db, SessionLocal
from .models import GamePointUser, SystemConfig, GamePointTransaction
from .config import settings
from .schemas import LoginRequest, BalanceAdjustment, CanjeRequest, TokenResponse
from . import services

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = AsyncIOScheduler()
    scheduler.add_job(auto_sync_job, 'cron', hour=23, minute=30)
    scheduler.start()
    logger.info("✅ GameQuest Online v1.0")
    yield
    scheduler.shutdown()

async def auto_sync_job():
    db = SessionLocal()
    try: await services.sync_users_to_db(db)
    finally: db.close()

app = FastAPI(title="GameQuest API", lifespan=lifespan)

# CORS UNIVERSAL PARA EVITAR BLOQUEOS DE LA BÓVEDA
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# RETROCOMPATIBILIDAD BÓVEDA ANTIGUA + TOKENS NUEVOS
def verify_admin(
    x_admin_user: Optional[str] = Header(None),
    x_admin_pass: Optional[str] = Header(None),
    authorization: Optional[str] = Header(None), 
    db: Session = Depends(get_db)
):
    if x_admin_user == settings.ADMIN_USER and x_admin_pass == settings.ADMIN_PASS:
        return "admin_legacy"
        
    if authorization:
        try:
            token = authorization.split()[1]
            conf = db.query(SystemConfig).filter(SystemConfig.key == "admin_token").first()
            if conf and conf.value == token:
                return "admin_token"
        except:
            pass
            
    raise HTTPException(401, "No autorizado. Credenciales inválidas.")

@app.post("/api/auth/login", response_model=TokenResponse)
def login(creds: LoginRequest, db: Session = Depends(get_db)):
    if creds.username == settings.ADMIN_USER and creds.password == settings.ADMIN_PASS:
        token = str(uuid.uuid4())
        conf = db.query(SystemConfig).filter(SystemConfig.key == "admin_token").first()
        if not conf:
            conf = SystemConfig(key="admin_token", value=token)
            db.add(conf)
        else:
            conf.value = token
        db.commit()
        return {"access_token": token, "token_type": "bearer"}
    raise HTTPException(401, "Credenciales inválidas")

@app.get("/health")
def health():
    return {"status": "online"}

@app.post("/admin/init_db")
def init_database(admin: str = Depends(verify_admin)):
    try:
        Base.metadata.create_all(bind=engine)
        return {"status": "Tablas verificadas/creadas correctamente"}
    except Exception as e:
        raise HTTPException(500, f"Error DB: {e}")

@app.get("/admin/users")
def list_users(limit: int = 100, search: Optional[str] = None, only_balance: bool = False, db: Session = Depends(get_db), admin: str = Depends(verify_admin)):
    q = db.query(GamePointUser)
    if only_balance: q = q.filter(GamePointUser.saldo > 0)
    if search:
        term = f"%{search.strip().lower()}%"
        q = q.filter(or_(GamePointUser.email.ilike(term), GamePointUser.name.ilike(term)))
    
    users = q.order_by(GamePointUser.saldo.desc()).limit(limit).all()
    stats = {
        "totalPoints": db.query(func.sum(GamePointUser.saldo)).scalar() or 0,
        "usersCount": db.query(func.count(GamePointUser.id)).scalar() or 0
    }
    return {"users": users, **stats}

@app.post("/admin/adjust_balance")
def adjust_balance(req: BalanceAdjustment, db: Session = Depends(get_db), admin: str = Depends(verify_admin)):
    user = db.query(GamePointUser).filter(GamePointUser.email == req.email.lower().strip()).first()
    if not user:
        user = GamePointUser(email=req.email.lower(), saldo=0)
        db.add(user)
        db.flush() 
    
    tx = GamePointTransaction(
        user_id=user.id,
        amount=req.amount,
        operation='CREDIT' if req.operation == 'add' else 'DEBIT',
        source='MANUAL_ADMIN',
        description=req.motive or "Ajuste manual"
    )
    db.add(tx)

    if req.operation == 'add':
        user.saldo += req.amount
        user.historico_acumulado += req.amount
    else:
        user.saldo = max(0, user.saldo - req.amount)
    
    db.commit()
    return {"status": "ok", "saldo": user.saldo}

@app.post("/api/canje")
async def procesar_canje(req: CanjeRequest, db: Session = Depends(get_db), x_store_token: str = Header(None)):
    if x_store_token != settings.STORE_TOKEN: raise HTTPException(403, "Token Tienda Inválido")
    if settings.MAINTENANCE_MODE_CANJE: raise HTTPException(503, "Mantenimiento")

    user = db.query(GamePointUser).filter(GamePointUser.email == req.email.lower().strip()).first()
    
    if not user: raise HTTPException(404, "Usuario no encontrado")
    if user.saldo < req.monto: raise HTTPException(400, "Saldo insuficiente")
    if req.monto < settings.MIN_CANJE: raise HTTPException(400, f"Mínimo ${settings.MIN_CANJE}")

    try:
        user.saldo -= req.monto
        user.historico_canjeado += req.monto
        
        tx = GamePointTransaction(
            user_id=user.id,
            amount=req.monto,
            operation='DEBIT',
            source='CANJE_WEB',
            description="Generación Cupón Descuento"
        )
        db.add(tx)
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Error DB Canje: {e}")
        raise HTTPException(500, "Error interno procesando saldo")

    user_name = f"{user.name or ''} {user.surname or ''}".strip() or "Cliente"
    cupon = await services.create_jumpseller_coupon(user.email, req.monto, user_name)

    if not cupon:
        logger.error(f"Fallo Cupón para {user.email}. Reembolsando...")
        user.saldo += req.monto
        user.historico_canjeado -= req.monto
        tx_refund = GamePointTransaction(
            user_id=user.id, 
            amount=req.monto, 
            operation='CREDIT', 
            source='SYSTEM_REFUND', 
            description="Fallo API Jumpseller - Devolución Automática"
        )
        db.add(tx_refund)
        db.commit()
        raise HTTPException(502, "Error al comunicar con Jumpseller (Saldo devuelto).")

    return {"status": "ok", "cupon_codigo": cupon, "nuevo_saldo": user.saldo}

@app.get("/api/public/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    user = db.query(GamePointUser).filter(GamePointUser.email == email.lower().strip()).first()
    return {"saldo": user.saldo if user else 0}

@app.post("/api/public/analyze_buylist")
async def analyze_buylist(file: UploadFile = File(...)):
    content = await file.read()
    return await services.analizar_manabox_ck(content)

@app.post("/admin/sync_users")
async def trigger_sync(db: Session = Depends(get_db), admin: str = Depends(verify_admin)):
    return await services.sync_users_to_db(db)
