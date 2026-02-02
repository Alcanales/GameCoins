import json, hmac, hashlib, secrets, aiohttp, magic
from fastapi import FastAPI, HTTPException, Depends, Header, Request, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func
from database import engine, Base, get_db
from models import GameCoinUser, SystemConfig
from config import settings
import services as logic
import tcg_logic

Base.metadata.create_all(bind=engine)
app = FastAPI(title="GameQuest Gold-Standard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # En producción, restringir a dominios específicos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/api/canje")
async def canje_publico(req: logic.CanjeRequest, db: Session = Depends(get_db), x_store_token: str = Header(None)):
    if x_store_token != settings.STORE_TOKEN: raise HTTPException(401)
    if settings.MAINTENANCE_MODE_CANJE: raise HTTPException(503, "Canjes en mantenimiento")

    email_clean = req.email.lower().strip()
    try:
        # BLOQUEO QUIRÚRGICO: Evita Race Conditions
        u = db.query(GameCoinUser).filter(func.lower(GameCoinUser.email) == email_clean).with_for_update().first()
        if not u or u.saldo < req.monto: raise HTTPException(400, "Saldo insuficiente")

        u.saldo -= req.monto
        db.commit()
        
        # Generación de cupón en Jumpseller
        code = f"GQ-{secrets.token_hex(3).upper()}"
        async with aiohttp.ClientSession() as s:
            res = await logic.crear_cupon_jumpseller(s, code, req.monto, email_clean, db)
            if not res:
                # Rollback compensatorio
                u.saldo += req.monto
                db.commit()
                return {"status": "error", "mensaje": "Error Jumpseller. Saldo devuelto."}
        
        return {"status": "ok", "cupon_codigo": code, "nuevo_saldo": u.saldo}
    except Exception as e:
        db.rollback()
        raise HTTPException(500, str(e))