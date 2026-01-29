import magic
import json
import secrets
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

app = FastAPI(title=settings.APP_NAME, version="5.4.0")

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

    if not (auth_render or auth_master):
        raise HTTPException(401)

@app.get("/")
def health_check():
    return {"status": "ok", "env": settings.ENV}

@app.post("/api/analizar")
async def analizar_csv(file: UploadFile = File(...), mode: str = Form("client")):
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(400, "El archivo debe ser un CSV.")
        
    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(413, "Archivo excede 10MB")
    
    result = await logic.procesar_csv_logic(content, internal_mode=(mode == "internal"))
    
    if isinstance(result, dict) and "error" in result:
        raise HTTPException(400, result["error"])
        
    return {"data": result}

@app.post("/api/enviar_buylist")
async def enviar_solicitud(background_tasks: BackgroundTasks, payload: str = Form(...), csv_file: UploadFile = File(...)):
    try:
        data = json.loads(payload)
        req = BuylistSubmitRequest(**data)
    except Exception as e:
        raise HTTPException(422, f"JSON inválido: {str(e)}")

    file_content = await csv_file.read()
    
    background_tasks.add_task(
        logic.enviar_correo_dual,
        req.cliente.model_dump(),
        [c.model_dump() for c in req.cartas],
        req.total_clp,
        req.total_gc,
        file_content,
        csv_file.filename
    )
    return {"status": "received", "message": "Procesando solicitud"}

# --- RUTAS DE ADMINISTRACIÓN (Corregidas: sin /api) ---

@app.get("/admin/users", dependencies=[Depends(verify_admin)])
def get_users(db: Session = Depends(get_db)):
    return db.query(GameCoinUser).all()

@app.post("/admin/canje", dependencies=[Depends(verify_admin)])
def procesar_canje(req: CanjeRequest, db: Session = Depends(get_db)):
    user = db.query(GameCoinUser).filter(GameCoinUser.email == req.email).first()
    if not user:
        user = GameCoinUser(email=req.email, saldo_actual=0, historico_canjeado=0)
        db.add(user)
    
    if user.saldo_actual < req.monto:
         raise HTTPException(400, "Saldo insuficiente")
         
    user.saldo_actual -= req.monto
    user.historico_canjeado += req.monto
    db.commit()
    return {"status": "ok", "nuevo_saldo": user.saldo_actual}

@app.post("/admin/update_saldo", dependencies=[Depends(verify_admin)])
def update_saldo(req: UpdateRequest, db: Session = Depends(get_db)):
    user = db.query(GameCoinUser).filter(GameCoinUser.email == req.email).first()
    if not user:
        user = GameCoinUser(email=req.email, saldo_actual=0, historico_canjeado=0)
        db.add(user)
    
    if req.accion == "add":
        user.saldo_actual += req.monto
    elif req.accion == "set":
        user.saldo_actual = req.monto
    elif req.accion == "subtract":
        user.saldo_actual = max(0, user.saldo_actual - req.monto)
        
    db.commit()
    return {"status": "ok", "nuevo_saldo": user.saldo_actual}