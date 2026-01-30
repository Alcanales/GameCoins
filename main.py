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

app = FastAPI(title=settings.APP_NAME, version="6.1.0")

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- SEGURIDAD DUAL ---
def verify_admin(x_admin_user: str = Header(None), x_admin_pass: str = Header(None)):
    if not (x_admin_user and x_admin_pass): 
        raise HTTPException(401, detail="Credenciales faltantes")
    
    auth_render = (secrets.compare_digest(x_admin_user, settings.ADMIN_USER) and 
                   secrets.compare_digest(x_admin_pass, settings.ADMIN_PASS))
    
    auth_master = (secrets.compare_digest(x_admin_user, settings.MASTER_USER) and 
                   secrets.compare_digest(x_admin_pass, settings.MASTER_PASS))

    if not (auth_render or auth_master):
        raise HTTPException(401, detail="Credenciales incorrectas")

@app.get("/")
def health_check():
    return {"status": "ok", "env": settings.ENV}

# --- Buylist Endpoints ---
@app.post("/api/analizar")
async def analizar_csv(file: UploadFile = File(...), mode: str = Form("client")):
    if not file.filename.lower().endswith('.csv'): raise HTTPException(400, "Debe ser CSV")
    content = await file.read()
    if len(content) > 10*1024*1024: raise HTTPException(413, "Max 10MB")
    
    # Llama a la lógica optimizada de services.py
    result = await logic.procesar_csv_logic(content, internal_mode=(mode == "internal"))
    if isinstance(result, dict) and "error" in result: raise HTTPException(400, result["error"])
    return {"data": result}

@app.post("/api/enviar_buylist")
async def enviar_solicitud(background_tasks: BackgroundTasks, payload: str = Form(...), csv_file: UploadFile = File(...)):
    try:
        data = json.loads(payload)
        req = BuylistSubmitRequest(**data)
    except Exception as e: raise HTTPException(422, f"JSON: {str(e)}")
    
    file_content = await csv_file.read()
    # Usa la función de correo dual con los nuevos términos
    background_tasks.add_task(logic.enviar_correo_dual, req.cliente.model_dump(), [c.model_dump() for c in req.cartas], req.total_clp, req.total_gc, file_content, csv_file.filename)
    return {"status": "received"}

# --- BÓVEDA & CUPONES (TAREA 3) ---

@app.get("/admin/users", dependencies=[Depends(verify_admin)])
def get_users(db: Session = Depends(get_db)):
    return db.query(GameCoinUser).all()

@app.post("/admin/canje", dependencies=[Depends(verify_admin)])
async def procesar_canje(req: CanjeRequest, db: Session = Depends(get_db)):
    """
    Gestión transaccional del canje:
    1. Verifica saldo.
    2. Resta saldo en DB (Commit).
    3. Llama API Jumpseller.
    4. Si API falla, se maneja el error (el saldo ya se restó para evitar fraude).
    """
    user = db.query(GameCoinUser).filter(GameCoinUser.email == req.email).first()
    if not user:
        user = GameCoinUser(email=req.email, rut="N/A", saldo=0, historico_canjeado=0)
        db.add(user)
    
    # Validación de Seguridad
    if user.saldo < req.monto:
         raise HTTPException(400, f"Saldo insuficiente. Disponible: {user.saldo} QP")
    
    # 1. Operación Atómica en DB (Prevenir doble gasto)
    user.saldo -= req.monto
    user.historico_canjeado += req.monto
    db.commit() # Confirmamos la resta ANTES de llamar a la API externa

    # 2. Generación del Cupón Externo
    try:
        # Generar código único aleatorio: GQ-XXXXX
        suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
        coupon_code = f"GQ-{suffix}"
        
        async with aiohttp.ClientSession() as session:
            # Llama al servicio de creación de cupones
            res = await logic.crear_cupon_jumpseller(session, coupon_code, req.monto, req.email)
            
            if not res or "promotion" not in res:
                # Caso extremo: Se cobró saldo pero falló Jumpseller.
                # No hacemos rollback automático para obligar revisión manual (seguridad financiera).
                return {
                    "status": "warning", 
                    "mensaje": "Saldo descontado pero error en Jumpseller. Contactar soporte.", 
                    "nuevo_saldo": user.saldo,
                    "cupon": "ERROR_API"
                }

            return {
                "status": "ok", 
                "nuevo_saldo": user.saldo, 
                "cupon_codigo": coupon_code,
                "mensaje": f"Canje exitoso. Cupón: {coupon_code}"
            }
            
    except Exception as e:
        # Error grave de sistema
        return {"status": "error", "mensaje": f"Error crítico: {str(e)}", "nuevo_saldo": user.saldo}

@app.post("/admin/update_saldo", dependencies=[Depends(verify_admin)])
def update_saldo(req: UpdateRequest, db: Session = Depends(get_db)):
    user = db.query(GameCoinUser).filter(GameCoinUser.email == req.email).first()
    if not user:
        user = GameCoinUser(email=req.email, rut="N/A", saldo=0, historico_canjeado=0)
        db.add(user)
    
    if req.accion == "add": user.saldo += req.monto
    elif req.accion == "set": user.saldo = req.monto
    elif req.accion == "subtract": user.saldo = max(0, user.saldo - req.monto)
    
    db.commit()
    return {"status": "ok", "nuevo_saldo": user.saldo}