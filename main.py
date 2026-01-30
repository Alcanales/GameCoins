import magic
import json
import secrets
import random
import string
import aiohttp
from fastapi import FastAPI, UploadFile, File, HTTPException, Form, BackgroundTasks, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from sqlalchemy.orm import Session

from config import settings
from database import engine, Base, get_db
import services as logic
from schemas import BuylistSubmitRequest, UpdateRequest, CanjeRequest
from models import GameCoinUser

Base.metadata.create_all(bind=engine)

app = FastAPI(title=settings.APP_NAME, version="6.5.0")

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def verify_admin(x_admin_user: str = Header(None), x_admin_pass: str = Header(None)):
    if not (x_admin_user and x_admin_pass): raise HTTPException(401)
    
    auth_render = (secrets.compare_digest(x_admin_user, settings.ADMIN_USER) and 
                   secrets.compare_digest(x_admin_pass, settings.ADMIN_PASS))
    
    auth_master = (secrets.compare_digest(x_admin_user, settings.MASTER_USER) and 
                   secrets.compare_digest(x_admin_pass, settings.MASTER_PASS))

    if not (auth_render or auth_master): raise HTTPException(401)

@app.get("/")
def health_check():
    return {"status": "ok", "env": settings.ENV}

# --- PÚBLICO (Frontend Safe) ---
@app.get("/api/public/balance/{email}")
def get_public_balance(email: str, db: Session = Depends(get_db)):
    if not email: return {"saldo": 0, "historico_canjeado": 0}
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
    # Retorno seguro: No exponer ID interno ni otros metadatos
    if not user: return {"saldo": 0, "historico_canjeado": 0}
    return {"email": user.email, "saldo": user.saldo, "historico_canjeado": user.historico_canjeado}

@app.post("/api/analizar")
async def analizar_csv(file: UploadFile = File(...), mode: str = Form("client")):
    # Validar extensión
    if not file.filename.lower().endswith('.csv'): raise HTTPException(400, "Requiere .csv")
    content = await file.read()
    if len(content) > 10*1024*1024: raise HTTPException(413, "Archivo muy grande")
    
    # Validar integridad opcional con magic si está instalado
    # if magic.from_buffer(content, mime=True) != 'text/csv': ...

    result = await logic.procesar_csv_logic(content, internal_mode=(mode == "internal"))
    if isinstance(result, dict) and "error" in result: raise HTTPException(400, result["error"])
    return {"data": result}

@app.post("/api/enviar_buylist")
async def enviar_solicitud(background_tasks: BackgroundTasks, payload: str = Form(...), csv_file: UploadFile = File(...)):
    try:
        data = json.loads(payload)
        req = BuylistSubmitRequest(**data)
    except Exception: raise HTTPException(422, "JSON inválido")
    
    file_content = await csv_file.read()
    background_tasks.add_task(logic.enviar_correo_dual, req.cliente.model_dump(), [c.model_dump() for c in req.cartas], req.total_clp, req.total_gc, file_content, csv_file.filename)
    return {"status": "received"}

# --- PRIVADO (Admin Only) ---
@app.get("/admin/users", dependencies=[Depends(verify_admin)])
def get_users(db: Session = Depends(get_db)):
    return db.query(GameCoinUser).all()

@app.post("/admin/canje", dependencies=[Depends(verify_admin)])
async def procesar_canje(req: CanjeRequest, db: Session = Depends(get_db)):
    """Canje transaccional atómico."""
    try:
        # Bloqueo de fila
        user = db.query(GameCoinUser).filter(GameCoinUser.email == req.email).with_for_update().first()
        
        if not user:
            user = GameCoinUser(email=req.email, rut="N/A", saldo=0, historico_canjeado=0)
            db.add(user)
            db.flush()
        
        if user.saldo < req.monto:
            db.rollback()
            raise HTTPException(400, f"Saldo insuficiente ({user.saldo} QP)")
        
        # Modificación de estado
        user.saldo -= req.monto
        user.historico_canjeado += req.monto
        db.commit() 
        
        # Generación Cupón (Externo)
        suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
        coupon_code = f"GQ-{suffix}"
        
        async with aiohttp.ClientSession() as session:
            res = await logic.crear_cupon_jumpseller(session, coupon_code, req.monto, req.email)
            
            if not res or "promotion" not in res:
                return {"status": "warning", "mensaje": "Error API Jumpseller", "nuevo_saldo": user.saldo}

            return {"status": "ok", "nuevo_saldo": user.saldo, "cupon_codigo": coupon_code}

    except HTTPException as he: raise he
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"Error: {str(e)}")

@app.post("/admin/update_saldo", dependencies=[Depends(verify_admin)])
def update_saldo(req: UpdateRequest, db: Session = Depends(get_db)):
    user = db.query(GameCoinUser).filter(GameCoinUser.email == req.email).with_for_update().first()
    if not user:
        user = GameCoinUser(email=req.email, rut="N/A", saldo=0, historico_canjeado=0)
        db.add(user)
    
    if req.accion == "add": user.saldo += req.monto
    elif req.accion == "set": user.saldo = req.monto
    elif req.accion == "subtract": user.saldo = max(0, user.saldo - req.monto)
    
    db.commit()
    return {"status": "ok", "nuevo_saldo": user.saldo}