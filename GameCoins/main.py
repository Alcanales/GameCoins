from fastapi import FastAPI, Depends, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from pydantic import BaseModel
from decimal import Decimal
from typing import Optional

from .database import get_db, engine, Base
from .vault import VaultController
from .schemas import CanjeRequest, LoginRequest, TokenResponse
from .config import settings

try:
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS public"))
        conn.commit()
    Base.metadata.create_all(bind=engine)
except Exception as e:
    print(f"[WARN] DB init: {e}")

app = FastAPI(title="GameCoins API", version="2.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
class AdminAdjustReq(BaseModel):
    email: str
    amount: int
    operation: str = "add"
    motive: Optional[str] = "Manual Admin Adjustment"


security = HTTPBearer()

def verify_admin(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != settings.STORE_TOKEN:
        raise HTTPException(status_code=401, detail="Token inválido")
    return True

def verify_store_token(x_store_token: Optional[str] = Header(default=None)):
    """
    ✅ FIX #2: /api/canje no tenía autenticación.
    Valida el header x-store-token enviado desde account.liquid.
    """
    if x_store_token != settings.STORE_TOKEN:
        raise HTTPException(status_code=401, detail="Token de tienda inválido")
    return True


@app.get("/api/balance/{email}")
@app.get("/api/saldo/{email}")
@app.get("/api/public/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    from .models import Gampoint
    try:
        user = db.query(Gampoint).filter(Gampoint.email == email.lower().strip()).first()
        return {
            "saldo": float(user.saldo) if user and user.saldo else 0.0,
            "historico_canjeado": float(user.historico_canjeado) if user and user.historico_canjeado else 0.0,
        }
    except Exception:
        return {"saldo": 0.0, "historico_canjeado": 0.0}

@app.post("/api/canje", dependencies=[Depends(verify_store_token)])
async def execute_canje(req: CanjeRequest, db: Session = Depends(get_db)):
    return await VaultController.process_canje(db, req.email, req.monto)

@app.post("/api/auth/login", response_model=TokenResponse)
def login(req: LoginRequest):
    if req.username == settings.ADMIN_USER and req.password == settings.ADMIN_PASS:
        return {"access_token": settings.STORE_TOKEN, "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Credenciales inválidas")

@app.get("/admin/users", dependencies=[Depends(verify_admin)])
@app.get("/api/admin/users", dependencies=[Depends(verify_admin)])
def get_users(
    db: Session = Depends(get_db),
    search: Optional[str] = None,
    limit: int = 100,
    only_balance: bool = False
):
    from .models import Gampoint
    query = db.query(Gampoint)

    if search:
        term = f"%{search.lower()}%"
        query = query.filter(
            Gampoint.email.ilike(term) |
            Gampoint.name.ilike(term) |
            Gampoint.surname.ilike(term)
        )
    if only_balance:
        query = query.filter(Gampoint.saldo > 0)

    users = query.order_by(Gampoint.saldo.desc()).limit(limit).all()

    # Estadísticas globales (siempre sin filtros para reflejar totales reales)
    total_circulante = db.query(func.sum(Gampoint.saldo)).scalar() or 0
    total_canjeado = db.query(func.sum(Gampoint.historico_canjeado)).scalar() or 0
    total_users = db.query(func.count(Gampoint.email)).scalar() or 0

    return {
        "users": [
            {
                "email": u.email,
                "name": u.name,
                "surname": u.surname,
                "saldo": float(u.saldo or 0),
                "historico_canjeado": float(u.historico_canjeado or 0),
                "historico_acumulado": float(u.historico_acumulado or 0),
                "jumpseller_id": u.jumpseller_id,
            }
            for u in users
        ],
        "totalCount": total_users,
        "totalPointsInVault": float(total_circulante),
        "totalRedeemed": float(total_canjeado),
    }

@app.post("/admin/sync_users", dependencies=[Depends(verify_admin)])
@app.post("/api/admin/sync_users", dependencies=[Depends(verify_admin)])
async def trigger_sync(db: Session = Depends(get_db)):
    from .services import sync_users_to_db
    result = await sync_users_to_db(db)
    return {"status": "success", "details": result}

@app.post("/admin/adjust_balance", dependencies=[Depends(verify_admin)])
@app.post("/api/admin/adjust", dependencies=[Depends(verify_admin)])
def adjust_balance(req: AdminAdjustReq, db: Session = Depends(get_db)):
    from .models import Gampoint
    user = db.query(Gampoint).filter(Gampoint.email == req.email.lower()).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    delta = Decimal(req.amount)

    if req.operation == "add":
        user.saldo += delta
        user.historico_acumulado += delta
    elif req.operation == "subtract":
        if user.saldo < delta:
            raise HTTPException(status_code=400, detail="Saldo insuficiente para descontar")
        user.saldo -= delta
    else:
        raise HTTPException(status_code=400, detail=f"Operación inválida: {req.operation}")

    db.commit()
    return {"status": "ok", "nuevo_saldo": float(user.saldo)}

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


@app.get("/health")
def health():
    return {"status": "ok"}
