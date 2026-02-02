import secrets
from fastapi import FastAPI, HTTPException, Depends, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func

# --- IMPORTACIONES ABSOLUTAS (Fix para Render) ---
from database import engine, Base, get_db
from models import GameCoinUser, SystemConfig
from config import settings
from schemas import CanjeRequest, ConfigRequest, UpdateRequest # ¡CRÍTICO!
import services as logic
import tcg_logic

# Inicialización de Tablas
Base.metadata.create_all(bind=engine)

app = FastAPI(title="GameQuest API Gold")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ENDPOINTS ---

@app.get("/api/public/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    """Consulta de saldo para el Frontend (Header/Dashboard)"""
    email_clean = email.lower().strip()
    u = db.query(GameCoinUser).filter(func.lower(GameCoinUser.email) == email_clean).first()
    return {"saldo": int(u.saldo if u else 0)}

@app.post("/api/canje")
async def canje(req: CanjeRequest, db: Session = Depends(get_db), x_store_token: str = Header(None)):
    """Transacción Atómica con Fórmula Quirúrgica"""
    # 1. Seguridad
    if x_store_token != settings.STORE_TOKEN:
        raise HTTPException(401, "Token de tienda inválido")
    if settings.MAINTENANCE_MODE_CANJE:
        raise HTTPException(503, "Sistema en mantenimiento")

    email_clean = req.email.lower().strip()

    # 2. Bloqueo (Select For Update)
    u = db.query(GameCoinUser).filter(func.lower(GameCoinUser.email) == email_clean).with_for_update().first()
    
    if not u or u.saldo < req.monto:
        raise HTTPException(400, "Saldo insuficiente o usuario no encontrado")

    # 3. Cargo Preventivo
    u.saldo -= req.monto
    db.commit()
    
    # 4. Generación Externa
    code = f"GQ-{secrets.token_hex(3).upper()}"
    res = await logic.crear_cupon_jumpseller(code, req.monto, email_clean, db)
    
    if not res:
        # 5. Rollback Compensatorio
        u.saldo += req.monto
        db.commit()
        raise HTTPException(500, "Error en Jumpseller. Puntos devueltos.")
        
    return {"status": "ok", "cupon_codigo": code, "nuevo_saldo": u.saldo}

@app.post("/admin/analyze_csv")
async def analyze_csv(file: UploadFile = File(...), x_admin_user: str = Header(None), x_admin_pass: str = Header(None)):
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        raise HTTPException(401)
    content = await file.read()
    res = tcg_logic.analizar_csv_estacas(content)
    if isinstance(res, dict) and "error" in res: raise HTTPException(400, res["error"])
    return res.to_dict(orient="records")

@app.post("/admin/config")
def update_config(req: ConfigRequest, db: Session = Depends(get_db), x_admin_user: str = Header(None), x_admin_pass: str = Header(None)):
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        raise HTTPException(401)
    
    # Upsert de configuraciones
    data = {
        "JUMPSELLER_API_TOKEN": req.api_token,
        "JUMPSELLER_STORE": req.store_login,
        "JUMPSELLER_HOOKS_TOKEN": req.hooks_token
    }
    for k, v in data.items():
        item = db.query(SystemConfig).filter(SystemConfig.key == k).first()
        if item: item.value = v
        else: db.add(SystemConfig(key=k, value=v))
    db.commit()
    return {"status": "ok"}