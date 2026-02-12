# GameCoins/main.py
import logging
import os
from fastapi import FastAPI, HTTPException, Depends, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

from .database import engine, Base, get_db, SessionLocal
from .models import GameCoinUser, PriceCache, SystemConfig
from .config import settings
from . import services
from . import tcg_logic

logging.basicConfig(level=logging.INFO)

# --- MODELOS DE DATOS ---
class BalanceAdjustment(BaseModel):
    email: str
    amount: int
    operation: str # 'add' o 'subtract'
    motive: Optional[str] = "Manual"

class CanjeRequest(BaseModel):
    email: str
    monto: int

# --- SCRIPT DE PARCHEO POSTGRESQL ---
def run_db_patches():
    db = SessionLocal()
    try:
        Base.metadata.create_all(bind=engine)
        # Parche para asegurar columnas de la Bóveda de Créditos
        queries = [
            "ALTER TABLE game_coin_users ADD COLUMN IF NOT EXISTS historico_acumulado INTEGER DEFAULT 0;",
            "ALTER TABLE game_coin_users ADD COLUMN IF NOT EXISTS historico_canjeado INTEGER DEFAULT 0;",
            "ALTER TABLE game_coin_users ADD COLUMN IF NOT EXISTS name VARCHAR;",
            "ALTER TABLE game_coin_users ADD COLUMN IF NOT EXISTS surname VARCHAR;"
        ]
        for q in queries:
            db.execute(text(q))
        db.commit()
        logging.info("Base de datos PostgreSQL sincronizada.")
    except Exception as e:
        logging.error(f"Error en parcheo: {e}")
    finally:
        db.close()

run_db_patches()

app = FastAPI(title="GameQuest Vault API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health(): return {"status": "online", "db": "connected"}

# --- GESTIÓN DE CRÉDITOS (BÓVEDA) ---

@app.get("/admin/users")
def list_users(db: Session = Depends(get_db), x_admin_user: str = Header(None), x_admin_pass: str = Header(None)):
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        raise HTTPException(status_code=401)
    users = db.query(GameCoinUser).all()
    return {
        "users": users,
        "totalPointsInVault": sum(u.saldo for u in users)
    }

@app.post("/admin/adjust_balance")
def adjust(req: BalanceAdjustment, db: Session = Depends(get_db), x_admin_user: str = Header(None), x_admin_pass: str = Header(None)):
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        raise HTTPException(status_code=401)
    
    email = req.email.lower().strip()
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
    if not user:
        user = GameCoinUser(email=email, saldo=0)
        db.add(user)
    
    if req.operation == 'add':
        user.saldo += req.amount
        user.historico_acumulado += req.amount
    else:
        user.saldo = max(0, user.saldo - req.amount)
        user.historico_canjeado += req.amount
    
    db.commit()
    return {"status": "success", "new_balance": user.saldo}

# --- ANALIZADOR DE BUYLIST (MANABOX / CARD KINGDOM) ---

@app.post("/api/public/analyze_buylist")
async def analyze_buylist(file: UploadFile = File(...), db: Session = Depends(get_db)):
    content = await file.read()
    # services.analizar_manabox_ck debe usar el Scryfall ID para el caché
    return await services.analizar_manabox_ck(content, db)