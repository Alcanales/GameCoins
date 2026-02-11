import logging
import os
from fastapi import FastAPI, HTTPException, Depends, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Dict, Any

# IMPORTS RELATIVOS (Estructura de Paquete Correcta)
from .database import engine, Base, get_db, SessionLocal
from .models import GameCoinUser, SystemConfig
from .config import settings
from . import services
from . import tcg_logic

# Configuración de Logs
logging.basicConfig(level=logging.INFO)

# Crear tablas en DB si no existen
Base.metadata.create_all(bind=engine)

# --- BÓVEDA INTELIGENTE (SOLUCIÓN DEFINITIVA) ---
def inicializar_boveda():
    """
    Sincroniza las credenciales de Render hacia la Base de Datos al arrancar.
    Garantiza que si cambias una clave en Render, la DB se actualice sola.
    """
    db = SessionLocal()
    try:
        # Claves que services.py necesita leer de la DB
        keys_to_sync = [
            "JUMPSELLER_API_TOKEN", 
            "JUMPSELLER_STORE", 
            "JUMPSELLER_HOOKS_TOKEN"
        ]
        
        cambios = 0
        logging.info("🔐 Bóveda: Iniciando sincronización de credenciales...")

        for key in keys_to_sync:
            env_val = os.getenv(key)
            
            # Si la variable no está en Render, avisamos (pero no rompemos nada)
            if not env_val:
                logging.warning(f"⚠️ Bóveda: Variable {key} no encontrada en el entorno (Render).")
                continue

            # Buscar la clave en la Base de Datos
            db_item = db.query(SystemConfig).filter(SystemConfig.key == key).first()
            
            if db_item:
                # Si existe, verificamos si el valor ha cambiado
                if db_item.value != env_val:
                    old_val_preview = db_item.value[:5] + "..." if db_item.value else "None"
                    db_item.value = env_val
                    cambios += 1
                    logging.info(f"🔄 Bóveda: Actualizando {key} (Antes: {old_val_preview})")
            else:
                # Si no existe, la creamos
                new_item = SystemConfig(key=key, value=env_val)
                db.add(new_item)
                cambios += 1
                logging.info(f"➕ Bóveda: Insertando nueva clave {key}")
        
        if cambios > 0:
            db.commit()
            logging.info(f"✅ Bóveda: Sincronización completada ({cambios} cambios).")
        else:
            logging.info("✅ Bóveda: Todo al día. No se requirieron cambios.")
            
    except Exception as e:
        logging.error(f"❌ ERROR CRÍTICO EN BÓVEDA: {str(e)}")
        # No hacemos raise para que el servidor arranque igual, aunque sin credenciales funcionales
    finally:
        db.close()

# Ejecutar la sincronización al inicio
inicializar_boveda()

app = FastAPI(title="GameQuest API Final")

# Health Check (Vital para que Render sepa que estamos vivos)
@app.get("/health")
def health_check():
    return {"status": "ok", "system": "GameQuest API Live"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class BuylistSubmission(BaseModel):
    nombre: str
    apellido: str
    rut: str
    telefono: str
    email: str
    pago: str
    cartas: List[Dict[str, Any]]

# --- ENDPOINTS ---

@app.get("/api/public/balance/{email}")
def get_balance(email: str, db: Session = Depends(get_db)):
    u = db.query(GameCoinUser).filter(GameCoinUser.email == email.lower().strip()).first()
    return {"saldo": int(u.saldo if u else 0)}

@app.post("/api/canje")
async def request_canje(req: Any, db: Session = Depends(get_db), x_store_token: str = Header(None)):
    if x_store_token != settings.STORE_TOKEN:
        raise HTTPException(status_code=401, detail="Token inválido")
    # req podría venir como dict o objeto, services maneja la lógica
    return await services.procesar_canje_atomico(req.email.lower().strip(), req.monto, db)

@app.post("/api/public/submit_buylist")
async def submit_buylist(data: BuylistSubmission):
    if services.enviar_correo_cotizacion(data.dict()):
        return {"status": "ok"}
    raise HTTPException(status_code=500, detail="Error enviando correo")

@app.post("/admin/analyze_csv")
async def admin_analyze_csv(file: UploadFile = File(...), x_admin_user: str = Header(None), x_admin_pass: str = Header(None), db: Session = Depends(get_db)):
    if x_admin_user != settings.ADMIN_USER or x_admin_pass != settings.ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")
    content = await file.read()
    df_result = await services.analizar_csv_con_stock_real(content, db)
    if isinstance(df_result, dict) and "error" in df_result:
        raise HTTPException(status_code=400, detail=df_result["error"])
    return df_result.to_dict(orient="records")

@app.post("/api/public/analyze_buylist")
async def public_analyze_buylist(file: UploadFile = File(...)):
    content = await file.read()
    # Usamos await porque tcg_logic ahora es asíncrono para consultar Scryfall
    result = await tcg_logic.analizar_csv_simple(content)
    
    if isinstance(result, dict) and "error" in result:
        return result # Retorna el error JSON
    return result.to_dict(orient="records")

# --- WEBHOOK: FIDELIZACIÓN (1%) ---
@app.post("/api/webhooks/order_paid")
async def handle_order_paid(payload: Dict[str, Any], db: Session = Depends(get_db)):
    try:
        # Jumpseller a veces envía el objeto 'order' anidado, a veces plano
        order = payload.get("order", payload)
        status = order.get("status", "").lower()
        
        if status != "paid":
            return {"status": "ignored", "reason": f"Order status is {status}"}

        customer = order.get("customer", {})
        email = customer.get("email", "").strip().lower()
        
        if not email:
            return {"status": "error", "reason": "No email in order"}

        total = float(order.get("total", 0))
        # Cálculo del 1% usando la configuración
        puntos = int(total * settings.LOYALTY_ACCUMULATION_RATE)

        if puntos > 0:
            user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
            if not user:
                user = GameCoinUser(
                    email=email, 
                    saldo=0, 
                    name=customer.get("name", ""), 
                    surname=customer.get("surname", "")
                )
                db.add(user)
            
            user.saldo += puntos
            db.commit()
            logging.info(f"💎 Fidelización: {email} ganó {puntos} QP por Orden #{order.get('id')}")

        return {"status": "success", "added_points": puntos}
    except Exception as e:
        logging.error(f"Webhook Error: {e}")
        return {"status": "error", "detail": str(e)}