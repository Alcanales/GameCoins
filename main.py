import magic # Requiere python-magic
import json
from fastapi import FastAPI, UploadFile, File, HTTPException, Form, BackgroundTasks, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from config import settings
from database import engine, Base, get_db
import services as logic
from schemas import BuylistSubmitRequest, UpdateRequest, CanjeRequest # Importamos DTOs
from models import GameCoinUser

Base.metadata.create_all(bind=engine)

app = FastAPI(title=settings.APP_NAME, version="5.0.0")

# CORS Seguro
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://gamequest.cl", "https://www.gamequest.cl", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Auth Admin Middleware
def verify_admin(x_admin_user: str = Header(None), x_admin_pass: str = Header(None)):
    import secrets
    if not (x_admin_user and x_admin_pass): raise HTTPException(401)
    if not (secrets.compare_digest(x_admin_user, settings.ADMIN_USER) and 
            secrets.compare_digest(x_admin_pass, settings.ADMIN_PASS)):
        raise HTTPException(401)

@app.get("/")
def health_check():
    return {"status": "ok", "env": settings.ENV}

@app.post("/api/analizar")
async def analizar_csv(file: UploadFile = File(...), mode: str = Form("client")):
    # 1. Validación de Seguridad (Magic Bytes)
    content = await file.read()
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(413, "Archivo excede 5MB")
    
    try:
        mime = magic.from_buffer(content, mime=True)
        if "text" not in mime and "csv" not in mime and "application/octet-stream" not in mime:
             # CSVs a veces son detectados como octet-stream o text/plain
            raise HTTPException(400, f"Tipo de archivo inválido: {mime}")
    except Exception:
        pass # Fallback si magic falla en alpine linux sin libs

    # 2. Procesamiento Async
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
    # 1. Parseo y Validación de JSON
    try:
        data = json.loads(payload)
        req = BuylistSubmitRequest(**data) # Valida con Pydantic
    except Exception as e:
        raise HTTPException(422, f"Datos inválidos: {str(e)}")

    # 2. Lectura Archivo
    file_content = await csv_file.read()
    
    # 3. Enviar a Background (No bloquear respuesta)
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
