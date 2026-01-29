import magic
import json
import secrets
import random
import string
from fastapi import FastAPI, UploadFile, File, HTTPException, Form, BackgroundTasks, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from sqlalchemy.orm import Session

from config import settings
from database import engine, Base, get_db
import services as logic
from schemas import BuylistSubmitRequest, UpdateRequest, CanjeRequest
from models import GameCoinUser

Base.metadata.create_all(bind=engine)

app = FastAPI(title=settings.APP_NAME, version="5.0.0")

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

def verify_admin(x_admin_user: str = Header(None), x_admin_pass: str = Header(None)):
    if not (x_admin_user and x_admin_pass): raise HTTPException(401)
    if not (secrets.compare_digest(x_admin_user, settings.ADMIN_USER) and 
            secrets.compare_digest(x_admin_pass, settings.ADMIN_PASS)):
        raise HTTPException(401)

@app.get("/")
def health_check():
    return {"status": "ok", "env": settings.ENV}

@app.post("/api/analizar")
async def analizar_csv(file: UploadFile = File(...), mode: str = Form("client")):
    content = await file.read()
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(413, "Archivo excede 5MB")
    
    try:
        mime = magic.from_buffer(content, mime=True)
    except Exception:
        pass

    result = await logic.procesar_csv_logic(content, internal_mode=(mode == "internal"))
    
    if isinstance(result, dict) and "error" in result:
        raise HTTPException(400, result["error"])
        
    return {"data": result}

@app.post("/api/enviar_buylist")
async def enviar_solicitud(
    background_tasks: BackgroundTasks,
    payload: str = Form(...),
    csv_file: UploadFile = File(...)
):
    try:
        data = json.loads(payload)
        req = BuylistSubmitRequest(**data)
    except Exception as e:
        raise HTTPException(422, f"Datos inválidos: {str(e)}")

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

@app.get("/api/saldo/{email}")
def consultar_saldo(email: str, db: Session = Depends(get_db)):
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email.strip().lower()).first()
    return {"email": email, "saldo": user.saldo if user else 0}

@app.post("/api/canjear")
def canjear_puntos(payload: CanjeRequest, db: Session = Depends(get_db)):
    email = payload.email.strip().lower()
    monto = int(payload.monto)
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email).with_for_update().first()
    
    if not user or user.saldo < monto:
        raise HTTPException(400, "Saldo insuficiente")
    
    user.saldo -= monto
    try:
        db.commit()
        codigo = f"GC-{''.join(random.choices(string.ascii_uppercase + string.digits, k=6))}"
        if logic.crear_cupon_jumpseller(codigo, monto):
            return {"status": "ok", "codigo": codigo, "nuevo_saldo": user.saldo}
        else:
            user.saldo += monto
            db.commit()
            raise HTTPException(502, "Error al crear cupón")
    except Exception:
        db.rollback()
        raise HTTPException(500, "Error interno")

@app.get("/admin/users", dependencies=[Depends(verify_admin)])
def listar_usuarios(db: Session = Depends(get_db)):
    return db.query(GameCoinUser).all()

@app.post("/admin/update", dependencies=[Depends(verify_admin)])
def actualizar_saldo_manual(payload: UpdateRequest, db: Session = Depends(get_db)):
    email = payload.email.strip().lower()
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
    if not user:
        if payload.accion == "restar": raise HTTPException(404, "Usuario no encontrado")
        user = GameCoinUser(email=email, saldo=0, rut=f"MAN-{email}")
        db.add(user)
    
    if payload.accion == "sumar": user.saldo += payload.monto
    elif payload.accion == "restar": user.saldo = max(0, user.saldo - payload.monto)
    db.commit()
    return {"msg": "OK", "nuevo_saldo": user.saldo}

@app.post("/admin/sync_clients", dependencies=[Depends(verify_admin)])
def trigger_sync(db: Session = Depends(get_db)):
    return logic.sincronizar_clientes_jumpseller(db, GameCoinUser)
