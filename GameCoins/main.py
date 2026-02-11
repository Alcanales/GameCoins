import logging
import os
from fastapi import FastAPI, HTTPException, Depends, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func
from pydantic import BaseModel
from typing import List, Dict, Any

from database import engine, Base, get_db, SessionLocal
from models import GameCoinUser, SystemConfig
from GameCoins.config import settings
import services
import tcg_logic

logging.basicConfig(level=logging.INFO)
Base.metadata.create_all(bind=engine)

def inicializar_boveda():
    """Inicializa credenciales desde el entorno si la DB está vacía."""
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
    u = db.query(GameCoinUser).filter(func.lower(GameCoinUser.email) == email.lower().strip()).first()
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
# --- NUEVO ENDPOINT PARA FIDELIZACIÓN (Jumpseller Webhook) ---
@app.post("/api/webhooks/order_paid")
async def handle_order_paid(payload: Dict[str, Any], db: Session = Depends(get_db), x_jumpseller_signature: str = Header(None)):
    """
    Recibe notificación de Jumpseller cuando una orden es Pagada.
    Suma puntos basados en LOYALTY_ACCUMULATION_RATE (1%).
    """
    try:
        # 1. Verificar estado de la orden
        status = payload.get("status") # Jumpseller envía 'paid'
        order_data = payload.get("order", payload) # A veces viene anidado, a veces directo
        
        # Solo procesamos si está pagada
        current_status = order_data.get("status", "").lower()
        if current_status != "paid":
            return {"status": "ignored", "reason": f"Order status is {current_status}"}

        # 2. Obtener datos del cliente
        customer = order_data.get("customer", {})
        email = customer.get("email", "").strip().lower()
        
        if not email:
            return {"status": "error", "reason": "No email provided"}

        # 3. Calcular puntos (Total de la orden * 0.01)
        # Usamos el total final (incluyendo descuentos/envío si aplica, o subtotal según prefieras)
        total_value = float(order_data.get("total", 0))
        puntos_a_sumar = int(total_value * settings.LOYALTY_ACCUMULATION_RATE)

        if puntos_a_sumar <= 0:
            return {"status": "ignored", "reason": "Zero points calculated"}

        # 4. Actualizar o Crear Usuario en DB
        user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
        if not user:
            user = GameCoinUser(email=email, saldo=0, name=customer.get("name", ""), surname=customer.get("surname", ""))
            db.add(user)
        
        user.saldo += puntos_a_sumar
        db.commit()
        
        logging.info(f"Fidelización: {email} ganó {puntos_a_sumar} puntos por orden #{order_data.get('id')}")
        return {"status": "success", "added_points": puntos_a_sumar}

    except Exception as e:
        logging.error(f"Error processing webhook: {str(e)}")
        # Retornamos 200 para que Jumpseller no reintente infinitamente si es un error lógico nuestro
        return {"status": "error", "detail": str(e)}