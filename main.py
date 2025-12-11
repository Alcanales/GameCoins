import os
from fastapi import FastAPI, UploadFile, File, HTTPException, Header, Depends, Body
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel
from database import engine, Base, get_db
from models import GameCoinUser
from logic import procesar_csv_manabox, sincronizar_clientes_jumpseller

Base.metadata.create_all(bind=engine)
app = FastAPI()

ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "PASSWORD_TEMPORAL")

origins = [
    "https://www.gamequest.cl",
    "https://gamequest.cl",
    "https://gamequest.jumpseller.com",
    "https://www.pelvium.cl",
    "http://localhost:8000",
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class UpdateRequest(BaseModel):
    email: str
    monto: int
    accion: str

class DeleteRequest(BaseModel):
    user_id: int

@app.get("/")
def home(): return {"status": "GameQuest API Online"}

@app.post("/api/analizar")
async def buylist_analisis(file: UploadFile = File(...)):
    content = await file.read()
    res = procesar_csv_manabox(content)
    if "error" in res: raise HTTPException(400, res["error"])
    return {"data": res}

@app.get("/admin/users")
def listar_usuarios(x_admin_key: str = Header(None), db: Session = Depends(get_db)):
    if x_admin_key != ADMIN_PASSWORD: raise HTTPException(401, "Clave incorrecta")
    return db.query(GameCoinUser).order_by(GameCoinUser.updated_at.desc()).all()

@app.post("/admin/update")
def actualizar_saldo(payload: UpdateRequest, x_admin_key: str = Header(None), db: Session = Depends(get_db)):
    if x_admin_key != ADMIN_PASSWORD: raise HTTPException(401, "Clave incorrecta")
    
    email = payload.email.strip().lower()
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
    
    if not user:
        if payload.accion == "restar": raise HTTPException(404, "Usuario nuevo, no se puede restar.")
        user = GameCoinUser(email=email, saldo=0)
        db.add(user)
    
    if payload.accion == "sumar": user.saldo += payload.monto
    elif payload.accion == "restar": 
        user.saldo -= payload.monto
        if user.saldo < 0: user.saldo = 0
    
    db.commit()
    return {"msg": "Saldo actualizado", "nuevo_saldo": user.saldo}

@app.post("/admin/sync_clients")
def sync_jumpseller(x_admin_key: str = Header(None), db: Session = Depends(get_db)):
    if x_admin_key != ADMIN_PASSWORD: raise HTTPException(401, "Clave incorrecta")
    return sincronizar_clientes_jumpseller(db, GameCoinUser)

@app.post("/admin/delete")
def delete_user(payload: DeleteRequest, x_admin_key: str = Header(None), db: Session = Depends(get_db)):
    if x_admin_key != ADMIN_PASSWORD: raise HTTPException(401, "Clave incorrecta")
    db.query(GameCoinUser).filter(GameCoinUser.id == payload.user_id).delete()
    db.commit()
    return {"msg": "Eliminado"}

@app.get("/api/saldo/{email}")
def consultar_saldo_publico(email: str, db: Session = Depends(get_db)):
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email.strip().lower()).first()
    return {"email": email, "saldo": user.saldo if user else 0}