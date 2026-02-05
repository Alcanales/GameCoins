import logging
import os
import smtplib
from typing import List
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from fastapi import FastAPI, HTTPException, Depends, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func
# --- CORRECCIÓN CRÍTICA: Importar BaseModel ---
from pydantic import BaseModel 

# Imports locales
from database import engine, Base, get_db, SessionLocal
from models import GameCoinUser, SystemConfig
from config import settings
from schemas import CanjeRequest, ConfigRequest
import services

# --- CONFIGURACIÓN DE LOGS ---
logging.basicConfig(level=logging.INFO)

# --- INICIALIZACIÓN DE DB ---
Base.metadata.create_all(bind=engine)

# --- INICIALIZAR BÓVEDA (Auto-llenado) ---
def inicializar_boveda():
    """
    Carga las credenciales desde las variables de entorno a la base de datos
    si la tabla está vacía.
    """
    db = SessionLocal()
    try:
        keys_to_check = {
            "JUMPSELLER_API_TOKEN": os.getenv("JUMPSELLER_API_TOKEN"),
            "JUMPSELLER_STORE": os.getenv("JUMPSELLER_STORE"),
            "JUMPSELLER_HOOKS_TOKEN": os.getenv("JUMPSELLER_HOOKS_TOKEN")
        }
        updated = False
        for key, value in keys_to_check.items():
            if value:
                exists = db.query(SystemConfig).filter(SystemConfig.key == key).first()
                if not exists:
                    logging.info(f"🔑 Sembrando Bóveda: {key}")
                    db.add(SystemConfig(key=key, value=value))
                    updated = True
        if updated:
            db.commit()
            logging.info("✅ Bóveda inicializada correctamente.")
    except Exception as e:
        logging.error(f"❌ Error inicializando Bóveda: {e}")
    finally:
        db.close()

# Ejecutar inicialización al arranque
inicializar_boveda()

# --- DEFINICIÓN DE LA APP ---
app = FastAPI(title="GameQuest Points API", version="1.0.5-FIX")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://gamequest.cl", "https://www.gamequest.cl", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ENDPOINTS ---

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "GameQuest API Online"}

@app.get("/api/public/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    email_clean = email.lower().strip()
    u = db.query(GameCoinUser).filter(func.lower(GameCoinUser.email) == email_clean).first()
    return {"saldo": int(u.saldo if u else 0)}

@app.post("/api/canje")
async def request_canje(req: CanjeRequest, db: Session = Depends(get_db), x_store_token: str = Header(None)):
    if x_store_token != settings.STORE_TOKEN:
        raise HTTPException(status_code=401, detail="Token inválido")
    
    email_clean = req.email.lower().strip()
    if req.monto < settings.MIN_PURCHASE_USD:
        return {"status": "error", "detail": "Monto mínimo no alcanzado"}
    
    return await services.procesar_canje_atomico(email_clean, req.monto, db)

@app.post("/api/public/analyze_buylist")
async def public_analyze_csv(file: UploadFile = File(...)):
    content = await file.read()
    res = await services.analizar_csv_estacas(content)
    
    if isinstance(res, dict) and "error" in res:
        raise HTTPException(status_code=400, detail=res["error"])
    
    return res.to_dict(orient="records")

@app.post("/api/webhook")
async def jumpseller_webhook(payload: dict, x_hooks_token: str = Header(None), db: Session = Depends(get_db)):
    # Validar token desde la Bóveda
    hooks_config = db.query(SystemConfig).filter(SystemConfig.key == "JUMPSELLER_HOOKS_TOKEN").first()
    secret_token = hooks_config.value if hooks_config else ""
    
    if x_hooks_token != secret_token:
        raise HTTPException(status_code=401, detail="Token inválido")
    
    if payload.get('event') != 'order_paid':
        return {"status": "ignored"}
    
    try:
        order_data = payload.get('data', {}).get('order', {})
        email = order_data.get('customer', {}).get('email')
        monto = order_data.get('total')
    except AttributeError:
        raise HTTPException(status_code=400, detail="Payload incorrecto")
    
    if not email or not monto:
        raise HTTPException(status_code=400, detail="Datos faltantes")
    
    email_clean = email.lower().strip()
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email_clean).first()
    
    if not user:
        user = GameCoinUser(email=email_clean)
        db.add(user)
    
    multiplier = settings.GAMECOIN_MULTIPLIER
    user.saldo += int(monto * multiplier)
    db.commit()
    
    return {"status": "ok"}

# --- ADMIN ENDPOINTS ---

@app.post("/admin/analyze_csv")
async def admin_analyze_csv(
    file: UploadFile = File(...), 
    x_admin_user: str = Header(None), 
    x_admin_pass: str = Header(None)
):
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Acceso denegado")
    
    content = await file.read()
    res = await services.analizar_csv_estacas(content)
    
    if isinstance(res, dict) and "error" in res:
        raise HTTPException(status_code=400, detail=res["error"])
        
    return res.to_dict(orient="records")

@app.post("/admin/config")
def admin_config(
    req: ConfigRequest, 
    db: Session = Depends(get_db), 
    x_admin_user: str = Header(None), 
    x_admin_pass: str = Header(None)
):
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Acceso denegado")
    
    configs = {
        "JUMPSELLER_API_TOKEN": req.api_token,
        "JUMPSELLER_STORE": req.store_login,
        "JUMPSELLER_HOOKS_TOKEN": req.hooks_token
    }
    
    for k, v in configs.items():
        item = db.query(SystemConfig).filter(SystemConfig.key == k).first()
        if item:
            item.value = v
        else:
            db.add(SystemConfig(key=k, value=v))
            
    db.commit()
    return {"status": "ok"}

@app.get("/admin/users")
def admin_get_users(
    db: Session = Depends(get_db), 
    x_admin_user: str = Header(None), 
    x_admin_pass: str = Header(None)
):
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Acceso denegado")
    
    users = db.query(GameCoinUser).all()
    return [
        {
            "name": u.name if u.name else "Sin Nombre", 
            "email": u.email, 
            "saldo": u.saldo, 
            "historico_canjeado": u.historico_canjeado
        } 
        for u in users
    ]

# --- SYNC JUMPSELLER ---
@app.post("/admin/sync_customers")
async def admin_sync_customers(
    db: Session = Depends(get_db), 
    x_admin_user: str = Header(None), 
    x_admin_pass: str = Header(None)
):
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Acceso denegado")
    
    return await services.sincronizar_clientes_jumpseller(db)

# --- AJUSTE MANUAL DE SALDO ---
class BalanceAdjustment(BaseModel):
    email: str
    amount: int
    operation: str

@app.post("/admin/adjust_balance")
def admin_adjust_balance(
    req: BalanceAdjustment, 
    db: Session = Depends(get_db), 
    x_admin_user: str = Header(None), 
    x_admin_pass: str = Header(None)
):
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Acceso denegado")
    
    user = db.query(GameCoinUser).filter(GameCoinUser.email == req.email.lower().strip()).first()
    
    if not user:
        if req.operation == "add":
            # Crear usuario si no existe y estamos sumando
            user = GameCoinUser(email=req.email.lower().strip(), name="Cliente Manual")
            db.add(user)
        else:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    if req.operation == "add":
        user.saldo += req.amount
    elif req.operation == "subtract":
        user.saldo = max(0, user.saldo - req.amount)
        
    db.commit()
    return {"status": "ok", "new_balance": user.saldo}

# --- SMTP EMAIL SERVICE ---
class OfferRequest(BaseModel):
    email: str
    preference: str
    data: List[dict]

@app.post("/api/public/send_offer")
def send_offer_email(req: OfferRequest):
    try:
        smtp_server = "smtp.gmail.com"
        smtp_port = 587
        sender_email = os.getenv("SMTP_EMAIL")
        sender_password = os.getenv("SMTP_PASSWORD")
        target_email = os.getenv("TARGET_EMAIL")

        if not sender_email or not sender_password:
            return {"status": "error", "detail": "SMTP no configurado"}

        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = target_email
        msg['Subject'] = f"Nueva Solicitud Buylist de: {req.email}"

        body = f"Cliente: {req.email}\nPreferencia de Pago: {req.preference}\n\nResumen (Primeras 50):\n"
        for item in req.data[:50]:
            body += f"- {item.get('name')} | Oferta: ${item.get('price_normal')}\n"
        
        if len(req.data) > 50:
            body += f"\n... y {len(req.data) - 50} cartas más."

        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()

        return {"status": "ok", "message": "Correo enviado"}
    
    except Exception as e:
        logging.error(f"Error SMTP: {e}")
        raise HTTPException(status_code=500, detail="Error enviando correo")