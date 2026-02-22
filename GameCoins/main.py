from fastapi import FastAPI, Depends, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from sqlalchemy import text, exc, func
from pydantic import BaseModel

# Importaciones relativas
from .database import get_db, engine, Base
from .vault import VaultController
from .schemas import CanjeRequest, LoginRequest, TokenResponse
from .config import settings

# --- CREACIÓN SEGURA DE ESQUEMA Y TABLAS ---
try:
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS public"))
        conn.commit()
except exc.ProgrammingError:
    pass

try:
    Base.metadata.create_all(bind=engine)
except exc.IntegrityError:
    pass
# ---------------------------------------------

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_methods=["*"],
    allow_headers=["*"],
)

class CanjeReq(BaseModel):
    email: str
    monto: int

class AdminAdjustReq(BaseModel):
    email: str
    amount: float

# --- SEGURIDAD DE LA BÓVEDA ---
security = HTTPBearer()

def verify_admin(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = settings.STORE_TOKEN if settings.STORE_TOKEN else "gamecoins-admin-secret"
    if credentials.credentials != token:
        raise HTTPException(status_code=401, detail="Token inválido o expirado")
    return True

# --- RUTAS PÚBLICAS (JUMPSELLER) ---

@app.get("/api/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    from .models import Gampoint
    user = db.query(Gampoint).filter(Gampoint.email == email.lower()).first()
    return {"saldo": float(user.saldo if user else 0)}

@app.post("/api/canje")
async def execute_canje(req: CanjeReq, db: Session = Depends(get_db)):
    return await VaultController.process_canje(db, req.email, req.monto)

@app.post("/webhook/sync")
async def jumpseller_sync(request: Request, db: Session = Depends(get_db)):
    data = await request.json()
    VaultController.sync_user(db, data.get("customer", {}))
    return {"status": "synced"}

# --- RUTAS DE LA BÓVEDA (ADMINISTRACIÓN) ---

@app.post("/api/auth/login", response_model=TokenResponse)
def login(req: LoginRequest):
    if req.username == settings.ADMIN_USER and req.password == settings.ADMIN_PASS:
        token = settings.STORE_TOKEN if settings.STORE_TOKEN else "gamecoins-admin-secret"
        return {"access_token": token, "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Credenciales inválidas")

@app.get("/api/admin/metrics", dependencies=[Depends(verify_admin)])
def get_metrics(db: Session = Depends(get_db)):
    from .models import Gampoint
    circulante = db.query(func.sum(Gampoint.saldo)).scalar() or 0
    canjeado = db.query(func.sum(Gampoint.historico_canjeado)).scalar() or 0
    total_users = db.query(Gampoint).count()
    return {
        "total_circulante": float(circulante),
        "total_canjeado": float(canjeado),
        "total_users": total_users
    }

@app.get("/api/admin/users", dependencies=[Depends(verify_admin)])
def get_users(db: Session = Depends(get_db)):
    from .models import Gampoint
    users = db.query(Gampoint).order_by(Gampoint.saldo.desc()).all()
    return users

@app.post("/api/admin/adjust", dependencies=[Depends(verify_admin)])
def adjust_balance(req: AdminAdjustReq, db: Session = Depends(get_db)):
    from .models import Gampoint
    user = db.query(Gampoint).filter(Gampoint.email == req.email.lower()).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    user.saldo += req.amount # Suma o resta dependiendo de si el número es negativo
    db.commit()
    return {"status": "ok", "nuevo_saldo": user.saldo}

@app.post("/api/admin/sync_users", dependencies=[Depends(verify_admin)])
async def trigger_sync(db: Session = Depends(get_db)):
    from .services import sync_users_to_db
    result = await sync_users_to_db(db)
    return result

# --- HEALTH CHECK ---
@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/")
def read_root():
    return {"status": "ok"}