import logging
import os
from fastapi import FastAPI, HTTPException, Depends, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Dict, Any

# IMPORTS
from .database import engine, Base, get_db, SessionLocal
from .models import GameCoinUser, SystemConfig
from .config import settings
from . import services
from . import tcg_logic

logging.basicConfig(level=logging.INFO)
Base.metadata.create_all(bind=engine)

# --- BÓVEDA ---
def inicializar_boveda():
    db = SessionLocal()
    try:
        keys = ["JUMPSELLER_API_TOKEN", "JUMPSELLER_STORE", "JUMPSELLER_HOOKS_TOKEN"]
        for key in keys:
            env_val = os.getenv(key)
            if env_val:
                db_item = db.query(SystemConfig).filter(SystemConfig.key == key).first()
                if db_item:
                    if db_item.value != env_val:
                        db_item.value = env_val
                else:
                    db.add(SystemConfig(key=key, value=env_val))
        db.commit()
    except Exception as e:
        logging.error(f"Error Bóveda: {e}")
    finally:
        db.close()

inicializar_boveda()

app = FastAPI(title="GameQuest API Final")

# Health Check
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

# --- MODELOS DE DATOS ---
class BuylistSubmission(BaseModel):
    nombre: str
    apellido: str
    rut: str
    telefono: str
    email: str
    pago: str
    cartas: List[Dict[str, Any]]

class CanjeRequest(BaseModel):
    email: str
    monto: int

# --- ENDPOINTS ---
@app.get("/api/public/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    u = db.query(GameCoinUser).filter(GameCoinUser.email == email.lower().strip()).first()
    return {"saldo": int(u.saldo if u else 0)}

@app.post("/api/canje")
async def request_canje(req: CanjeRequest, db: Session = Depends(get_db), x_store_token: str = Header(None)):
    if x_store_token != settings.STORE_TOKEN:
        raise HTTPException(status_code=401, detail="Token inválido")
    return await services.procesar_canje_atomico(req.email.lower().strip(), req.monto, db)

@app.post("/api/public/submit_buylist")
async def submit_buylist(data: BuylistSubmission):
    if services.enviar_correo_cotizacion(data.dict()):
        return {"status": "ok"}
    raise HTTPException(status_code=500, detail="Error enviando correo")

@app.post("/admin/analyze_csv")
async def admin_analyze_csv(file: UploadFile = File(...), x_admin_user: str = Header(None), x_admin_pass: str = Header(None), db: Session = Depends(get_db)):
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        raise HTTPException(status_code=401)
    content = await file.read()
    return (await services.analizar_csv_con_stock_real(content, db)).to_dict(orient="records")

@app.post("/api/public/analyze_buylist")
async def public_analyze_buylist(file: UploadFile = File(...)):
    content = await file.read()
    result = await tcg_logic.analizar_csv_simple(content)
    if isinstance(result, dict) and "error" in result:
        return result
    return result.to_dict(orient="records")

@app.post("/api/webhooks/order_paid")
async def handle_order_paid(payload: Dict[str, Any], db: Session = Depends(get_db)):
    try:
        order = payload.get("order", payload)
        if order.get("status", "").lower() != "paid":
            return {"status": "ignored"}
        
        email = order.get("customer", {}).get("email", "").strip().lower()
        if not email: return {"status": "error", "reason": "No email"}

        total = float(order.get("total", 0))
        puntos = int(total * settings.LOYALTY_ACCUMULATION_RATE)
        
        if puntos > 0:
            user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
            if not user:
                user = GameCoinUser(email=email, saldo=0)
                db.add(user)
            user.saldo += puntos
            db.commit()
            logging.info(f"Fidelización: {email} +{puntos}")

        return {"status": "success", "added": puntos}
    except Exception as e:
        return {"status": "error", "detail": str(e)}