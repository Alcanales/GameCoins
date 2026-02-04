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

logging.basicConfig(level=logging.ERROR)

# Inicialización de DB
Base.metadata.create_all(bind=engine)

app = FastAPI(title="GameQuest Points API", version="1.0.2-PROD")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://gamequest.cl", "https://www.gamequest.cl", "http://localhost:8000"],  # Agregué local para tests
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ENDPOINTS PÚBLICOS (Jumpseller consume estos) ---

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
    # Validación simple de token de tienda
    if x_store_token != settings.STORE_TOKEN:
        logging.error(f"Intento de acceso con token inválido: {x_store_token}")
        raise HTTPException(status_code=401, detail="Token de tienda inválido")
    
    email_clean = req.email.lower().strip()
    if req.monto < settings.MIN_PURCHASE_USD:
        return {"status": "error", "detail": "Monto mínimo no alcanzado"}
    return await services.procesar_canje_atomico(email_clean, req.monto, db)

@app.post("/api/public/analyze_buylist")
async def public_analyze_csv(file: UploadFile = File(...)):
    """Endpoint público para analizar buylists (solo resultados aprobados)."""
    content = await file.read()
    res = services.analizar_csv_estacas(content)
    if isinstance(res, dict) and "error" in res:
        logging.error(f"Error en análisis público: {res['error']}")
        raise HTTPException(status_code=400, detail=res["error"])
    return res.to_dict(orient="records")

@app.post("/api/webhook")
async def jumpseller_webhook(payload: dict, x_hooks_token: str = Header(None), db: Session = Depends(get_db)):
    """Webhook para acumular puntos desde Jumpseller (e.g., al pagar orden)."""
    hooks_token = db.query(SystemConfig).filter(SystemConfig.key == "JUMPSELLER_HOOKS_TOKEN").first()
    if x_hooks_token != (hooks_token.value if hooks_token else ""):
        logging.error(f"Webhook con token inválido: {x_hooks_token}")
        raise HTTPException(status_code=401, detail="Token inválido")
    
    # Asumir evento "order_paid" – ajusta según payload real de Jumpseller
    if payload.get('event') != 'order_paid':
        return {"status": "ignored"}
    
    email = payload.get('data', {}).get('order', {}).get('customer', {}).get('email')
    monto = payload.get('data', {}).get('order', {}).get('total')
    
    if not email or not monto:
        logging.error("Payload de webhook inválido: faltan email o monto")
        raise HTTPException(status_code=400, detail="Payload inválido")
    
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email.lower()).first()
    if not user:
        user = GameCoinUser(email=email.lower())
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
    """Sube un CSV de Manabox/Scryfall para detectar precios peligrosos."""
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        logging.error(f"Intento de acceso admin denegado: user={x_admin_user}")
        raise HTTPException(status_code=401, detail="Acceso denegado")
    
    content = await file.read()
    res = services.analizar_csv_estacas(content)
    
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
        raise HTTPException(status_code=401)
    
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
    return {"status": "ok", "mensaje": "Bóveda actualizada"}