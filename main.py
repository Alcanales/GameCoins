import secrets
import random
import string
import hmac
import hashlib
import base64
from fastapi import FastAPI, UploadFile, File, HTTPException, Header, Depends, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel
from config import settings
import services as logic
from database import engine, Base, get_db
from models import GameCoinUser 

Base.metadata.create_all(bind=engine)

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        return response

app = FastAPI(title="GameQuest GameCoins API", version="3.1")

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=["gamecoins.onrender.com", "gamequest.cl", "*.gamequest.cl", "localhost", "127.0.0.1"])

origins = [
    "http://localhost", "http://localhost:8000",
    "https://game-quest.jumpseller.com", "https://game-quest.cl",       
    "https://www.game-quest.cl", "https://gamequest.cl", "https://www.gamequest.cl"    
]
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def verificar_admin(x_admin_user: str = Header(None), x_admin_pass: str = Header(None)):
    if not (x_admin_user and x_admin_pass): raise HTTPException(401)
    if not (secrets.compare_digest(x_admin_user, settings.ADMIN_USER) and secrets.compare_digest(x_admin_pass, settings.ADMIN_PASS)): raise HTTPException(401)
    return True

class UpdateRequest(BaseModel):
    email: str; monto: int; accion: str

class CanjeRequest(BaseModel):
    email: str; monto: int

class BuylistSubmitRequest(BaseModel):
    cliente: dict; cartas: list; total_clp: str; total_gc: str

@app.get("/")
def home(): return {"status": "Online"}

@app.post("/api/analizar")
def buylist_analisis(file: UploadFile = File(...), mode: str = Form("client")):
    content = file.file.read()
    if len(content) > 5*1024*1024: raise HTTPException(413, "Max 5MB")
    res = logic.procesar_csv_manabox(content, internal_mode=(mode == "internal"))
    if isinstance(res, dict) and "error" in res: raise HTTPException(400, res["error"])
    return {"data": res}

@app.post("/api/enviar_buylist")
def submit_buylist(payload: BuylistSubmitRequest):
    return logic.enviar_correo_buylist(payload.cliente, payload.cartas, payload.total_clp, payload.total_gc)

@app.get("/api/saldo/{email}")
def consultar_saldo(email: str, db: Session = Depends(get_db)):
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email.strip().lower()).first()
    return {"email": email, "saldo": user.saldo if user else 0}

@app.post("/api/canjear")
def canjear_puntos(payload: CanjeRequest, db: Session = Depends(get_db)):
    """
    Ruta Principal para Pagos Mixtos:
    1. Descuenta saldo.
    2. Crea cupón en Jumpseller.
    3. Cliente usa cupón + Webpay en checkout.
    """
    email = payload.email.strip().lower(); monto = int(payload.monto)
    if monto <= 0: raise HTTPException(400, "Inválido")
    
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email).with_for_update().first()
    if not user or user.saldo < monto: raise HTTPException(400, "Saldo insuficiente")
    
    user.saldo -= monto
    try:
        db.commit() 
        codigo = f"GC-{''.join(random.choices(string.ascii_uppercase + string.digits, k=4))}"
        if logic.crear_cupon_jumpseller(codigo, monto):
            return {"status": "ok", "codigo": codigo, "nuevo_saldo": user.saldo}
        else:
            user.saldo += monto; db.commit()
            raise HTTPException(502, "Error Jumpseller")
    except Exception:
        db.rollback(); raise HTTPException(500)

@app.get("/admin/users", dependencies=[Depends(verificar_admin)])
def listar_usuarios(db: Session = Depends(get_db)):
    return db.query(GameCoinUser).order_by(GameCoinUser.updated_at.desc()).limit(200).all()

@app.post("/admin/update", dependencies=[Depends(verificar_admin)])
def actualizar_saldo_manual(payload: UpdateRequest, db: Session = Depends(get_db)):
    email = payload.email.strip().lower()
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
    if not user:
        if payload.accion == "restar": raise HTTPException(404)
        user = GameCoinUser(email=email, saldo=0, rut=f"MAN-{email}")
        db.add(user)
    
    if payload.accion == "sumar": user.saldo += payload.monto
    elif payload.accion == "restar": user.saldo = max(0, user.saldo - payload.monto)
    db.commit()
    return {"msg": "OK", "nuevo_saldo": user.saldo}

@app.post("/admin/sync_clients", dependencies=[Depends(verificar_admin)])
def trigger_sync(db: Session = Depends(get_db)):
    return logic.sincronizar_clientes_jumpseller(db, GameCoinUser)

@app.post("/webhook/order_created")
async def procesar_pago_gamecoins(request: Request, db: Session = Depends(get_db)):
    print("--- INICIO WEBHOOK GAMECOINS ---") # Log 1
    
    # 1. Leer el cuerpo
    body = await request.body()
    try:
        payload = await request.json()
    except:
        print("ERROR: No se pudo leer el JSON del webhook")
        return {"status": "error"}

    # 2. Verificar Seguridad (Si hay token configurado)
    if settings.JUMPSELLER_HOOKS_TOKEN:
        sig = request.headers.get("Jumpseller-Hmac-Sha256")
        # Calculamos la firma
        calc = base64.b64encode(hmac.new(settings.JUMPSELLER_HOOKS_TOKEN.encode(), body, hashlib.sha256).digest()).decode()
        
        if not secrets.compare_digest(sig, calc):
            print(f"ERROR DE SEGURIDAD: Firma recibida ({sig}) no coincide con calculada ({calc})")
            return {"status": "ignored_signature_mismatch"}
        else:
            print("SEGURIDAD OK: Firma válida.")

    # 3. Analizar la Orden
    order = payload.get("order", {})
    order_id = order.get("id", "Desconocido")
    payment_method = order.get("payment_method_name", "")
    status = order.get("status", "")
    
    print(f"PROCESANDO ORDEN #{order_id}")
    print(f" - Metodo Pago: '{payment_method}'")
    print(f" - Estado: '{status}'")

    # 4. Validar si es GameCoins
    if "GameCoins" in payment_method:
        if status == "Pending":
            print(" -> DETECTADO PAGO GAMECOINS PENDIENTE. PROCESANDO...")
            email = order.get("customer", {}).get("email", "").strip().lower()
            total = float(order.get("total", 0))
            
            user = db.query(GameCoinUser).filter(GameCoinUser.email == email).with_for_update().first()
            
            if user:
                print(f" -> Usuario encontrado: {email}. Saldo: {user.saldo}, Total Orden: {total}")
                if user.saldo >= total:
                    user.saldo -= int(total)
                    db.commit()
                    print(" -> EXITO: Saldo descontado. Actualizando orden a Pagada...")
                    logic.actualizar_orden_jumpseller(order.get("id"), "Paid", f"Pago Total GC: -${int(total)}")
                else:
                    print(" -> FALLO: Saldo insuficiente.")
                    logic.actualizar_orden_jumpseller(order.get("id"), "Canceled", "Saldo insuficiente para pago total")
            else:
                print(f" -> ERROR: Usuario {email} no existe en base de datos GameCoins.")
        else:
            print(f" -> IGNORADO: El estado es '{status}', se esperaba 'Pending'.")
    else:
        print(f" -> IGNORADO: El metodo de pago '{payment_method}' no contiene la palabra 'GameCoins'.")

    print("--- FIN WEBHOOK ---")
    return {"status": "ok"}