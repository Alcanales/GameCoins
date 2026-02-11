import logging
from fastapi import FastAPI, HTTPException, Depends, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func
from database import engine, Base, get_db, SessionLocal
from models import GameCoinUser, SystemConfig
from config import settings
from schemas import CanjeRequest, ConfigRequest
import services
import tcg_logic

logging.basicConfig(level=logging.INFO)
Base.metadata.create_all(bind=engine)

# Inicializar Bóveda desde ENV si existen
def init_vault():
    db = SessionLocal()
    keys = ["JUMPSELLER_API_TOKEN", "JUMPSELLER_STORE", "JUMPSELLER_HOOKS_TOKEN"]
    for k in keys:
        val = os.getenv(k)
        if val and not db.query(SystemConfig).filter(SystemConfig.key==k).first():
            db.add(SystemConfig(key=k, value=val))
    db.commit()
    db.close()

import os; init_vault()

app = FastAPI(title="GameQuest API", version="1.3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/public/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    email_clean = email.lower().strip()
    u = db.query(GameCoinUser).filter(func.lower(GameCoinUser.email) == email_clean).first()
    return {"saldo": int(u.saldo if u else 0)}

@app.post("/api/canje")
async def request_canje(req: CanjeRequest, db: Session = Depends(get_db), x_store_token: str = Header(None)):
    if x_store_token != settings.STORE_TOKEN: raise HTTPException(401, "Token inválido")
    if req.monto < settings.MIN_CANJE: return {"status": "error", "detail": f"Mínimo ${settings.MIN_CANJE}"}
    return await services.procesar_canje_atomico(req.email.lower().strip(), req.monto, db)

@app.post("/admin/analyze_csv")
async def admin_analyze_csv(file: UploadFile = File(...), x_admin_user: str = Header(None), x_admin_pass: str = Header(None), db: Session = Depends(get_db)):
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS: raise HTTPException(401)
    return (await services.analizar_csv_con_stock_real(await file.read(), db)).to_dict(orient="records")

@app.post("/api/public/analyze_buylist")
async def public_analyze_buylist(file: UploadFile = File(...)):
    return tcg_logic.analizar_csv_simple(await file.read()).to_dict(orient="records")

@app.get("/")
def health(): return {"status": "online"}