from fastapi import FastAPI, Depends, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text, exc  
from pydantic import BaseModel
from .database import get_db, engine, Base
from .vault import VaultController
from .schemas import CanjeRequest

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

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://gamequest.cl", "https://*.jumpseller.com"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class CanjeReq(BaseModel):
    email: str
    monto: int

# --- RUTAS DE LA API ---

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

# --- ENDPOINTS DE HEALTH CHECK PARA RENDER ---

@app.get("/health")
def health_check():
    return {"status": "ok", "message": "GameCoins API is running"}

@app.get("/")
def read_root():
    return {"status": "ok", "message": "Welcome to GameCoins API"}