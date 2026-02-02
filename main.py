from fastapi import FastAPI, HTTPException, Depends, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func

# Imports locales
from database import engine, Base, get_db
from models import GameCoinUser, SystemConfig
from config import settings
from schemas import CanjeRequest, ConfigRequest
import services

# Inicialización de DB
Base.metadata.create_all(bind=engine)

app = FastAPI(title="GameQuest Points API", version="1.0.2-PROD")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Restringir en el panel de Render si es posible
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
        raise HTTPException(status_code=401, detail="Token de tienda inválido")
    
    email_clean = req.email.lower().strip()
    return await services.procesar_canje_atomico(email_clean, req.monto, db)

# --- ENDPOINTS ADMINISTRATIVOS ---

@app.post("/admin/analyze_csv")
async def admin_analyze_csv(
    file: UploadFile = File(...), 
    x_admin_user: str = Header(None), 
    x_admin_pass: str = Header(None)
):
    """Sube un CSV de Manabox/Scryfall para detectar precios peligrosos."""
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Acceso denegado")
    
    content = await file.read()
    res = services.analizar_csv_estacas(content)
    
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
    """Actualiza tokens de Jumpseller en caliente (sin redeploy)."""
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
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