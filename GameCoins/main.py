from fastapi import FastAPI, Depends, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from sqlalchemy import text, exc, func
from pydantic import BaseModel
from decimal import Decimal # <--- IMPORTANTE

from .database import get_db, engine, Base
from .vault import VaultController
from .schemas import CanjeRequest, LoginRequest, TokenResponse
from .config import settings

# Inicialización segura
try:
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS public"))
        conn.commit()
    Base.metadata.create_all(bind=engine)
except Exception: pass

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class AdminAdjustReq(BaseModel):
    email: str
    amount: int

security = HTTPBearer()

def verify_admin(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != settings.STORE_TOKEN:
        raise HTTPException(status_code=401, detail="Token inválido")
    return True

# RUTAS DE SALDO (Con alias para evitar 404)
@app.get("/api/balance/{email}")
@app.get("/api/saldo/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    from .models import Gampoint
    user = db.query(Gampoint).filter(Gampoint.email == email.lower()).first()
    return {"saldo": float(user.saldo if user else 0)}

@app.post("/api/canje")
async def execute_canje(req: CanjeRequest, db: Session = Depends(get_db)):
    return await VaultController.process_canje(db, req.email, req.monto)

@app.post("/api/auth/login", response_model=TokenResponse)
def login(req: LoginRequest):
    if req.username == settings.ADMIN_USER and req.password == settings.ADMIN_PASS:
        return {"access_token": settings.STORE_TOKEN, "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Credenciales inválidas")

@app.get("/api/admin/metrics", dependencies=[Depends(verify_admin)])
def get_metrics(db: Session = Depends(get_db)):
    from .models import Gampoint
    circulante = db.query(func.sum(Gampoint.saldo)).scalar() or 0
    canjeado = db.query(func.sum(Gampoint.historico_canjeado)).scalar() or 0
    total_users = db.query(Gampoint).count()
    return {"total_circulante": float(circulante), "total_canjeado": float(canjeado), "total_users": total_users}

@app.get("/api/admin/users", dependencies=[Depends(verify_admin)])
def get_users(db: Session = Depends(get_db)):
    from .models import Gampoint
    return db.query(Gampoint).order_by(Gampoint.saldo.desc()).all()

@app.post("/api/admin/adjust", dependencies=[Depends(verify_admin)])
def adjust_balance(req: AdminAdjustReq, db: Session = Depends(get_db)):
    from .models import Gampoint
    user = db.query(Gampoint).filter(Gampoint.email == req.email.lower()).first()
    if not user: raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    # CORRECCIÓN DE ERROR 500: Convertir a Decimal
    user.saldo += Decimal(req.amount)
    db.commit()
    return {"status": "ok", "nuevo_saldo": float(user.saldo)}

@app.post("/api/admin/sync_users", dependencies=[Depends(verify_admin)])
async def trigger_sync(db: Session = Depends(get_db)):
    from .services import sync_users_to_db
    return await sync_users_to_db(db)

@app.get("/health")
def health(): return {"status": "ok"}