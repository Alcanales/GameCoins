import logging
import os
from fastapi import FastAPI, HTTPException, Depends, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func

# Imports locales (Asegúrate de que tus otros archivos estén actualizados)
from database import engine, Base, get_db, SessionLocal
from models import GameCoinUser, SystemConfig
from config import settings
from schemas import CanjeRequest, ConfigRequest
import services

# --- CONFIGURACIÓN DE LOGS ---
logging.basicConfig(level=logging.INFO) # Nivel INFO para ver la inicialización

# --- INICIALIZACIÓN DE DB ---
Base.metadata.create_all(bind=engine)

# --- FUNCIÓN CRÍTICA: INICIALIZAR BÓVEDA ---
def inicializar_boveda():
    """
    Se ejecuta al inicio. Verifica si la tabla de configuración (Bóveda) está vacía.
    Si lo está, inyecta las credenciales desde las variables de entorno de Render.
    Esto asegura que el Canje y los Webhooks funcionen sin configuración manual.
    """
    db = SessionLocal()
    try:
        # Mapeo: Clave en DB -> Variable de Entorno
        keys_to_check = {
            "JUMPSELLER_API_TOKEN": os.getenv("JUMPSELLER_API_TOKEN"),
            "JUMPSELLER_STORE": os.getenv("JUMPSELLER_STORE"), # Tu login/store code
            "JUMPSELLER_HOOKS_TOKEN": os.getenv("JUMPSELLER_HOOKS_TOKEN")
        }
        
        updated = False
        for key, value in keys_to_check.items():
            if value: # Solo si la variable existe en el entorno
                exists = db.query(SystemConfig).filter(SystemConfig.key == key).first()
                if not exists:
                    logging.info(f"🔑 Sembrando Bóveda: {key}")
                    db.add(SystemConfig(key=key, value=value))
                    updated = True
        
        if updated:
            db.commit()
            logging.info("✅ Bóveda inicializada y lista para operar.")
        else:
            logging.info("ℹ️ La Bóveda ya tenía credenciales.")
            
    except Exception as e:
        logging.error(f"❌ Error crítico inicializando Bóveda: {e}")
    finally:
        db.close()

# Ejecutamos la carga de credenciales ANTES de iniciar la app
inicializar_boveda()

# --- DEFINICIÓN DE LA APP ---
app = FastAPI(title="GameQuest Points API", version="1.0.4-RELEASE")

# --- CORS (Permite acceso desde tu tienda Jumpseller) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://gamequest.cl", "https://www.gamequest.cl", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 1. HEALTH CHECK (Para Render) ---
@app.get("/health")
def health_check():
    """Render llama a esto para saber si estamos vivos."""
    return {"status": "ok", "service": "GameQuest API Online"}

# --- 2. CONSULTA DE SALDO (Público) ---
@app.get("/api/public/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    """Muestra saldo en el frontend/widget."""
    email_clean = email.lower().strip()
    u = db.query(GameCoinUser).filter(func.lower(GameCoinUser.email) == email_clean).first()
    return {"saldo": int(u.saldo if u else 0)}

# --- 3. CANJE DE PUNTOS (Core Business) ---
@app.post("/api/canje")
async def request_canje(req: CanjeRequest, db: Session = Depends(get_db), x_store_token: str = Header(None)):
    """
    1. Valida token de tienda (STORE_TOKEN).
    2. Llama a services.procesar_canje_atomico.
    3. El servicio usa la Bóveda para crear el cupón en Jumpseller.
    """
    if x_store_token != settings.STORE_TOKEN:
        logging.error(f"Intento de canje con token inválido: {x_store_token}")
        raise HTTPException(status_code=401, detail="Token de tienda inválido")
    
    email_clean = req.email.lower().strip()
    if req.monto < settings.MIN_PURCHASE_USD:
        return {"status": "error", "detail": "Monto mínimo no alcanzado"}
    
    # Llamada asíncrona al servicio blindado
    return await services.procesar_canje_atomico(email_clean, req.monto, db)

# --- 4. BUYLIST PÚBLICA (Async & Optimizado) ---
@app.post("/api/public/analyze_buylist")
async def public_analyze_csv(file: UploadFile = File(...)):
    """
    Analiza CSVs de clientes usando Scryfall en paralelo.
    No bloquea el servidor gracias a 'async'.
    """
    content = await file.read()
    
    # Procesamiento asíncrono (requiere await)
    res = await services.analizar_csv_estacas(content)
    
    if isinstance(res, dict) and "error" in res:
        logging.error(f"Error en análisis público: {res['error']}")
        raise HTTPException(status_code=400, detail=res["error"])
    
    return res.to_dict(orient="records")

# --- 5. SISTEMA DE FIDELIDAD (Webhook) ---
@app.post("/api/webhook")
async def jumpseller_webhook(payload: dict, x_hooks_token: str = Header(None), db: Session = Depends(get_db)):
    """
    Recibe notificación de compra desde Jumpseller y abona puntos.
    Valida usando el token guardado en la Bóveda.
    """
    # 1. Obtener secreto desde la Bóveda
    hooks_config = db.query(SystemConfig).filter(SystemConfig.key == "JUMPSELLER_HOOKS_TOKEN").first()
    secret_token = hooks_config.value if hooks_config else ""
    
    # 2. Validar autenticidad
    if x_hooks_token != secret_token:
        logging.error(f"Webhook rechazado: Token {x_hooks_token} no coincide con Bóveda")
        raise HTTPException(status_code=401, detail="Token inválido")
    
    # 3. Filtrar evento (Ej: 'order_paid')
    if payload.get('event') != 'order_paid':
        return {"status": "ignored"}
    
    # 4. Extraer datos con seguridad
    try:
        order_data = payload.get('data', {}).get('order', {})
        email = order_data.get('customer', {}).get('email')
        monto = order_data.get('total')
    except AttributeError:
        raise HTTPException(status_code=400, detail="Estructura de payload incorrecta")
    
    if not email or not monto:
        logging.error("Webhook sin email o monto")
        raise HTTPException(status_code=400, detail="Datos faltantes")
    
    # 5. Abonar puntos (Upsert user)
    email_clean = email.lower().strip()
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email_clean).first()
    
    if not user:
        user = GameCoinUser(email=email_clean)
        db.add(user)
    
    multiplier = settings.GAMECOIN_MULTIPLIER
    puntos_ganados = int(monto * multiplier)
    user.saldo += puntos_ganados
    
    db.commit()
    logging.info(f"Fidelidad: {puntos_ganados} puntos abonados a {email_clean}")
    
    return {"status": "ok"}

# --- 6. BUYLIST ADMIN (Seguridad Extra) ---
@app.post("/admin/analyze_csv")
async def admin_analyze_csv(
    file: UploadFile = File(...), 
    x_admin_user: str = Header(None), 
    x_admin_pass: str = Header(None)
):
    """Endpoint administrativo para auditar listas grandes."""
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        logging.error(f"Acceso admin denegado a {x_admin_user}")
        raise HTTPException(status_code=401, detail="Acceso denegado")
    
    content = await file.read()
    res = await services.analizar_csv_estacas(content)
    
    if isinstance(res, dict) and "error" in res:
        raise HTTPException(status_code=400, detail=res["error"])
        
    return res.to_dict(orient="records")

# --- 7. CONFIGURACIÓN MANUAL DE BÓVEDA (Opcional) ---
@app.post("/admin/config")
def admin_config(
    req: ConfigRequest, 
    db: Session = Depends(get_db), 
    x_admin_user: str = Header(None), 
    x_admin_pass: str = Header(None)
):
    """Permite rotar credenciales sin reiniciar el servidor."""
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
    logging.info("Bóveda actualizada manualmente vía Admin API")
    return {"status": "ok", "mensaje": "Bóveda actualizada"}