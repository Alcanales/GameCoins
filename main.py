import logging
import os
import json
from fastapi import FastAPI, HTTPException, Depends, Header, Request, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, asc
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
app = FastAPI(title="GameQuest Points API", version="1.0.6-HOOKS")

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

# Endpoint dual para compatibilidad con widgets
@app.get("/api/saldo/{email}")
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

# --- EL HOOK (SISTEMA DE FIDELIDAD) ---
@app.post("/api/webhook")
async def jumpseller_webhook(
    request: Request, 
    db: Session = Depends(get_db)
):
    """
    Recibe la notificación de 'Order Paid' desde Jumpseller
    y abona los puntos correspondientes al cliente.
    """
    # 1. Obtener el secreto desde la Bóveda
    hooks_config = db.query(SystemConfig).filter(SystemConfig.key == "JUMPSELLER_HOOKS_TOKEN").first()
    secret_token = hooks_config.value if hooks_config else ""

    # 2. Leer el payload (JSON)
    try:
        payload = await request.json()
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="JSON inválido")

    # 3. Validación de Seguridad (Token en Headers o Query)
    # Jumpseller envía la firma en 'Jumpseller-Hmac-Sha256', pero si configuraste
    # un token simple en la URL del webhook (?token=...), lo validamos así:
    query_params = request.query_params
    received_token = query_params.get("token")
    
    # Si no viene en la URL, intentamos leer headers custom (x-hooks-token)
    if not received_token:
        received_token = request.headers.get("x-hooks-token")

    # Nota: Si Jumpseller no envía el token explícitamente, este check fallará.
    # Asegúrate de configurar el Webhook en Jumpseller así:
    # URL: https://tudominio.onrender.com/api/webhook?token=TU_TOKEN_SECRETO
    if received_token != secret_token:
        logging.warning(f"Intento de webhook no autorizado. Token recibido: {received_token}")
        raise HTTPException(status_code=401, detail="Token de webhook inválido")

    # 4. Filtrar evento: Solo nos interesa 'Order Paid' (o 'order_paid')
    event_type = payload.get('event')
    if not event_type or 'paid' not in str(event_type).lower():
        return {"status": "ignored", "reason": f"Evento {event_type} no es pago"}

    # 5. Extraer datos del pedido
    try:
        order = payload.get('order', {})
        if not order: # A veces Jumpseller envía la orden en 'data' -> 'order'
             order = payload.get('data', {}).get('order', {})

        customer_email = order.get('customer', {}).get('email')
        customer_name = f"{order.get('customer', {}).get('name', '')} {order.get('customer', {}).get('surname', '')}".strip()
        total_price = order.get('total')
        
    except Exception as e:
        logging.error(f"Error procesando payload webhook: {e}")
        raise HTTPException(status_code=400, detail="Estructura de pedido incorrecta")

    if not customer_email or not total_price:
        raise HTTPException(status_code=400, detail="Faltan email o monto")

    # 6. Abonar Puntos (Lógica de Negocio)
    email_clean = customer_email.lower().strip()
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email_clean).first()

    # Si el usuario no existe, lo creamos
    if not user:
        user = GameCoinUser(email=email_clean, name=customer_name)
        db.add(user)
    else:
        # Actualizamos el nombre si viene nuevo
        if customer_name:
            user.name = customer_name

    # Calcular puntos (Ej: 1000 CLP = 550 Puntos si el multiplier es 0.55)
    multiplier = settings.GAMECOIN_MULTIPLIER
    puntos_ganados = int(total_price * multiplier)
    
    user.saldo += puntos_ganados
    db.commit()

    logging.info(f"💰 HOOK EXITOSO: {puntos_ganados} puntos abonados a {email_clean} por orden de ${total_price}")
    return {"status": "ok", "puntos_abonados": puntos_ganados}

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
            # CAMBIO: Juntamos nombre y apellido para mostrarlo en la web
            "name": f"{u.name or ''} {u.surname or ''}".strip() or "Sin Nombre",
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
    data: list

@app.post("/api/public/send_offer")
def send_offer_email(req: OfferRequest):

    return {"status": "ok", "message": "Simulacion envio"}