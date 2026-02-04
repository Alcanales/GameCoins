from fastapi import FastAPI, HTTPException, Depends, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func
import logging

# Imports locales
from database import engine, Base, get_db
from models import GameCoinUser, SystemConfig
from config import settings
from schemas import CanjeRequest, ConfigRequest
import services

# Configuración de Logs
logging.basicConfig(level=logging.ERROR)

# Inicialización de DB (Crea tablas si no existen)
Base.metadata.create_all(bind=engine)

# Definición de la App
app = FastAPI(title="GameQuest Points API", version="1.0.3-PROD")

# Configuración CORS (Permite que tu frontend en Jumpseller/Web consuma la API)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://gamequest.cl", "https://www.gamequest.cl", "http://localhost:8000"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ENDPOINT DE SALUD (CRÍTICO PARA RENDER) ---
@app.get("/health")
def health_check():
    """
    Endpoint ligero usado por Render para verificar si el servicio está levantado.
    Retorna 200 OK.
    """
    return {"status": "ok", "service": "GameQuest API Online"}

# --- ENDPOINTS PÚBLICOS ---

@app.get("/api/public/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    """Consulta de saldo ultrarrápida para el frontend."""
    email_clean = email.lower().strip()
    u = db.query(GameCoinUser).filter(func.lower(GameCoinUser.email) == email_clean).first()
    return {"saldo": int(u.saldo if u else 0)}

@app.post("/api/canje")
async def request_canje(req: CanjeRequest, db: Session = Depends(get_db), x_store_token: str = Header(None)):
    """
    Endpoint transaccional. Delega la complejidad atómica a services.py.
    """
    # Validación simple de token de tienda (Seguridad)
    if x_store_token != settings.STORE_TOKEN:
        logging.error(f"Intento de acceso con token inválido: {x_store_token}")
        raise HTTPException(status_code=401, detail="Token de tienda inválido")
    
    email_clean = req.email.lower().strip()
    if req.monto < settings.MIN_PURCHASE_USD:
        return {"status": "error", "detail": "Monto mínimo no alcanzado"}
    
    # Llamada asíncrona al servicio atómico refactorizado
    return await services.procesar_canje_atomico(email_clean, req.monto, db)

@app.post("/api/public/analyze_buylist")
async def public_analyze_csv(file: UploadFile = File(...)):
    """
    Endpoint público para analizar buylists.
    IMPORTANTE: Ahora usa await porque services.analizar_csv_estacas es asíncrono.
    """
    content = await file.read()
    
    # AWAIT agregado aquí para compatibilidad con el nuevo services.py
    res = await services.analizar_csv_estacas(content)
    
    if isinstance(res, dict) and "error" in res:
        logging.error(f"Error en análisis público: {res['error']}")
        raise HTTPException(status_code=400, detail=res["error"])
    
    # Convertimos el DataFrame de pandas a lista de diccionarios para JSON
    return res.to_dict(orient="records")

@app.post("/api/webhook")
async def jumpseller_webhook(payload: dict, x_hooks_token: str = Header(None), db: Session = Depends(get_db)):
    """Webhook para acumular puntos desde Jumpseller (e.g., al pagar orden)."""
    # Obtener token de hooks desde la Bóveda (DB)
    hooks_token = db.query(SystemConfig).filter(SystemConfig.key == "JUMPSELLER_HOOKS_TOKEN").first()
    
    # Validar token
    if x_hooks_token != (hooks_token.value if hooks_token else ""):
        logging.error(f"Webhook con token inválido: {x_hooks_token}")
        raise HTTPException(status_code=401, detail="Token inválido")
    
    # Filtrar evento (ajustar según payload real de Jumpseller, ej: 'order_paid')
    if payload.get('event') != 'order_paid':
        return {"status": "ignored"}
    
    # Extraer datos seguros
    try:
        order_data = payload.get('data', {}).get('order', {})
        email = order_data.get('customer', {}).get('email')
        monto = order_data.get('total')
    except AttributeError:
        logging.error("Estructura de payload webhook inesperada")
        raise HTTPException(status_code=400, detail="Estructura inválida")
    
    if not email or not monto:
        logging.error("Payload de webhook inválido: faltan email o monto")
        raise HTTPException(status_code=400, detail="Datos faltantes")
    
    # Lógica de acumulación
    email_clean = email.lower().strip()
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email_clean).first()
    
    if not user:
        user = GameCoinUser(email=email_clean)
        db.add(user)
    
    multiplier = settings.GAMECOIN_MULTIPLIER
    user.saldo += int(monto * multiplier)
    db.commit()
    
    return {"status": "ok"}

# --- ENDPOINTS ADMINISTRATIVOS ---

@app.post("/admin/analyze_csv")
async def admin_analyze_csv(
    file: UploadFile = File(...), 
    x_admin_user: str = Header(None), 
    x_admin_pass: str = Header(None)
):
    """Sube un CSV de Manabox/Scryfall para detectar precios peligrosos (Admin)."""
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        logging.error(f"Intento de acceso admin denegado: user={x_admin_user}")
        raise HTTPException(status_code=401, detail="Acceso denegado")
    
    content = await file.read()
    
    # Llamada asíncrona optimizada
    res = await services.analizar_csv_estacas(content)
    
    if isinstance(res, dict) and "error" in res:
        logging.error(f"Error en análisis admin: {res['error']}")
        raise HTTPException(status_code=400, detail=res["error"])
        
    return res.to_dict(orient="records")

@app.post("/admin/config")
def admin_config(
    req: ConfigRequest, 
    db: Session = Depends(get_db), 
    x_admin_user: str = Header(None), 
    x_admin_pass: str = Header(None)
):
    """Actualiza tokens de Jumpseller en caliente (sin redeploy)."""
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        logging.error(f"Intento de config admin denegado: user={x_admin_user}")
        raise HTTPException(status_code=401, detail="Acceso denegado")
    
    # Actualización o Creación (Upsert) de configuraciones
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
    return {"status": "ok", "mensaje": "Bóveda actualizada correctamente"}