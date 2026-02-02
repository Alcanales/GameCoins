from fastapi import FastAPI, HTTPException, Depends, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func
import secrets
from database import engine, Base, get_db
from models import GameCoinUser
from config import settings
import services as logic
import tcg_logic

Base.metadata.create_all(bind=engine)
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.post("/api/canje")
async def canje(req: CanjeRequest, db: Session = Depends(get_db), x_store_token: str = Header(None)):
    if x_store_token != settings.STORE_TOKEN: raise HTTPException(401)
    if settings.MAINTENANCE_MODE_CANJE: raise HTTPException(503, "Mantenimiento")
    u = db.query(GameCoinUser).filter(func.lower(GameCoinUser.email) == req.email.lower()).with_for_update().first()
    if not u or u.saldo < req.monto: raise HTTPException(400, "Saldo insuficiente")
    u.saldo -= req.monto
    db.commit()
    code = f"GQ-{secrets.token_hex(3).upper()}"
    if not await logic.crear_cupon_jumpseller(code, req.monto, req.email, db):
        u.saldo += req.monto; db.commit(); raise HTTPException(500, "Error Jumpseller")
    return {"status": "ok", "cupon_codigo": code, "nuevo_saldo": u.saldo}

@app.get("/api/public/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    u = db.query(GameCoinUser).filter(func.lower(GameCoinUser.email) == email.lower().strip()).first()
    return {"saldo": int(u.saldo if u else 0)}

@app.post("/admin/analyze_csv")
async def analyze(file: UploadFile = File(...)):
    res = tcg_logic.analizar_csv_estacas(await file.read())
    return {"status": "ok", "detalle": res.to_dict(orient="records")}
