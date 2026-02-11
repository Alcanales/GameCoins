import logging
import os
from fastapi import FastAPI, HTTPException, Depends, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Dict, Any

# IMPORTS CORREGIDOS (Relativos)
from .database import engine, Base, get_db, SessionLocal
from .models import GameCoinUser, SystemConfig
from .config import settings
from . import services
from . import tcg_logic

logging.basicConfig(level=logging.INFO)
Base.metadata.create_all(bind=engine)

def inicializar_boveda():
    db = SessionLocal()
    try:
        keys = ["JUMPSELLER_API_TOKEN", "JUMPSELLER_STORE", "JUMPSELLER_HOOKS_TOKEN"]
        for k in keys:
            val = os.getenv(k)
            if val and not db.query(SystemConfig).filter(SystemConfig.key == k).first():
                db.add(SystemConfig(key=k, value=val))
        db.commit()
    finally:
        db.close()

inicializar_boveda()

app = FastAPI(title="GameQuest API Final")

@app.get("/health")
def health_check():
    return {"status": "ok"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class BuylistSubmission(BaseModel):
    nombre: str
    apellido: str
    rut: str
    telefono: str
    email: str
    pago: str
    cartas: List[Dict[str, Any]]

@app.get("/api/public/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    u = db.query(GameCoinUser).filter(GameCoinUser.email == email.lower().strip()).first()
    return {"saldo": int(u.saldo if u else 0)}

@app.post("/api/canje")
async def request_canje(req: Any, db: Session = Depends(get_db), x_store_token: str = Header(None)):
    if x_store_token != settings.STORE_TOKEN:
        raise HTTPException(status_code=401, detail="Token inválido")
    return await services.procesar_canje_atomico(req.email.lower().strip(), req.monto, db)

@app.post("/api/public/submit_buylist")
async def submit_buylist(data: BuylistSubmission):
    if services.enviar_correo_cotizacion(data.dict()):
        return {"status": "ok"}
    raise HTTPException(status_code=500, detail="Error de envío")

@app.post("/admin/analyze_csv")
async def admin_analyze_csv(file: UploadFile = File(...), x_admin_user: str = Header(None), x_admin_pass: str = Header(None), db: Session = Depends(get_db)):
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        raise HTTPException(status_code=401)
    return (await services.analizar_csv_con_stock_real(await file.read(), db)).to_dict(orient="records")

@app.post("/api/public/analyze_buylist")
async def public_analyze_buylist(file: UploadFile = File(...)):
    return tcg_logic.analizar_csv_simple(await file.read()).to_dict(orient="records")

# --- WEBHOOK: FIDELIZACIÓN POR COMPRAS (1%) ---
@app.post("/api/webhooks/order_paid")
async def handle_order_paid(payload: Dict[str, Any], db: Session = Depends(get_db)):
    try:
        order = payload.get("order", payload)
        status = order.get("status", "").lower()
        
        if status != "paid":
            return {"status": "ignored", "reason": f"Status is {status}"}

        customer = order.get("customer", {})
        email = customer.get("email", "").strip().lower()
        
        if not email:
            return {"status": "error", "reason": "No email"}

        total = float(order.get("total", 0))
        # Usa la variable del 1% definida en config
        puntos = int(total * settings.LOYALTY_ACCUMULATION_RATE)

        if puntos > 0:
            user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
            if not user:
                user = GameCoinUser(email=email, saldo=0, name=customer.get("name"), surname=customer.get("surname"))
                db.add(user)
            
            user.saldo += puntos
            db.commit()
            logging.info(f"Fidelización: {email} +{puntos} QP (Orden #{order.get('id')})")

        return {"status": "success", "added": puntos}
    except Exception as e:
        logging.error(f"Webhook Error: {e}")
        return {"status": "error", "detail": str(e)}