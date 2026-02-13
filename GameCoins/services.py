import pandas as pd
import io
import logging
import uuid
import aiohttp
import asyncio
import re
from sqlalchemy.orm import Session
from .config import settings
from .models import GameCoinUser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 1. FUNCIÓN DE SANITIZACIÓN INTERNACIONAL ---
def sanitize_name(text):
    """
    Limpia y formatea nombres respetando costumbres internacionales.
    - Permite Ñ, ñ, Tildes (áéíóú).
    - Elimina espacios dobles.
    - Convierte a formato Título (Juan Perez).
    """
    if not text or pd.isna(text): 
        return ""
    
    # Convertir a string y quitar espacios extremos
    text = str(text).strip()
    
    # Eliminar caracteres que NO sean letras, espacios, guiones o apóstrofes
    # (Mantenemos soporte unicode para Ñ y tildes automáticamente en Python 3)
    
    # Colapsar espacios múltiples (ej: "Juan    Perez" -> "Juan Perez")
    text = re.sub(r'\s+', ' ', text)
    
    # Formato Título: "JUAN perez" -> "Juan Perez"
    return text.title()

# --- JUMPSELLER SYNC ROBUSTO ---
async def fetch_jumpseller_customers():
    """Descarga TODOS los clientes de Jumpseller."""
    url = f"{settings.JUMPSELLER_API_BASE}/customers.json"
    params = {
        "login": settings.JUMPSELLER_LOGIN,
        "authtoken": settings.JUMPSELLER_API_TOKEN,
        "limit": 50,
        "page": 1,
        "status": "all"
    }
    
    all_customers = []
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200: break
                    data = await resp.json()
                    if not data: break 
                    
                    all_customers.extend(data)
                    if len(data) < 50: break 
                    params["page"] += 1
                    await asyncio.sleep(0.2) 
            except: break
    return all_customers

async def sync_users_to_db(db: Session):
    logger.info("🚀 Iniciando Sync: Búsqueda Profunda + Sanitización (Soporte Ñ)...")
    customers = await fetch_jumpseller_customers()
    
    if not customers:
        return {"status": "empty", "details": "No se descargaron clientes."}

    added = 0
    updated = 0
    
    for c in customers:
        customer_data = c.get('customer', {})
        email = customer_data.get('email', '').strip().lower()
        if not email: continue

        # --- 2. ESTRATEGIA DE BÚSQUEDA DE NOMBRE COMPLETA ---
        
        # Extraemos las posibles fuentes de nombre
        billing = customer_data.get('billing_address', {})
        shipping = customer_data.get('shipping_address', {})
        
        # Lista de candidatos en orden de prioridad (El primero que sirva gana)
        candidates = [
            (billing.get('name'), billing.get('surname')),      # 1. Facturación (Más confiable)
            (customer_data.get('name'), customer_data.get('surname')), # 2. Ficha Cliente
            (shipping.get('name'), shipping.get('surname'))     # 3. Envío
        ]

        final_name = ""
        final_surname = ""

        # Buscamos el primer par de nombres válido
        for n, s in candidates:
            clean_n = sanitize_name(n)
            clean_s = sanitize_name(s)
            
            # Si encontramos al menos un nombre, nos quedamos con estos datos
            if clean_n: 
                final_name = clean_n
                final_surname = clean_s
                break
        
        # 3. Fallback: Si NADA funcionó, rescatar del email
        if not final_name:
            # Ejemplo: "juan.perez_99@gmail.com" -> "Juan Perez 99"
            username = email.split('@')[0]
            # Reemplazamos puntos, guiones y guiones bajos por espacios
            username_clean = re.sub(r'[._-]', ' ', username)
            final_name = sanitize_name(username_clean)
            final_surname = "" # Apellido queda vacío si viene del mail

        # --- PERSISTENCIA EN BASE DE DATOS ---
        user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()

        if not user:
            new_user = GameCoinUser(
                email=email,
                name=final_name,
                surname=final_surname,
                saldo=0 
            )
            db.add(new_user)
            added += 1
        else:
            # Actualizamos SIEMPRE para "limpiar" nombres antiguos sucios
            # o rellenar los que estaban vacíos
            if user.name != final_name or user.surname != final_surname:
                user.name = final_name
                user.surname = final_surname
                updated += 1
    
    db.commit()
    logger.info(f"✅ Sync Terminado: {added} nuevos, {updated} corregidos/actualizados.")
    return {"added": added, "updated": updated, "total_scanned": len(customers)}

# --- OTRAS FUNCIONES (MANTENER IGUAL) ---
async def create_jumpseller_coupon(email: str, amount: int):
    code = f"GQ-{uuid.uuid4().hex[:8].upper()}"
    url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
    params = {"login": settings.JUMPSELLER_LOGIN, "authtoken": settings.JUMPSELLER_API_TOKEN}
    payload = {
        "promotion": {
            "name": f"Canje QuestPoints - {email}",
            "code": code,
            "enabled": True,
            "discount_type": "fixed",
            "value": amount,
            "usage_limit": 1,
            "minimum_order_amount": 0
        }
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, params=params, json=payload) as resp:
                if resp.status not in [200, 201]: return None
                data = await resp.json()
                return data.get("promotion", {}).get("code", code)
        except: return None

def clean_currency(value):
    if pd.isna(value): return 0.0
    if isinstance(value, (int, float)): return float(value)
    val_str = str(value).replace('$', '').replace(',', '').strip()
    try: return float(val_str)
    except: return 0.0

def get_pricing_tier(price_usd: float) -> float:
    if price_usd <= 0: return 0.0
    if price_usd < 3.0: return 0.30
    elif price_usd < 10.0: return 0.45
    elif price_usd < 50.0: return 0.55
    else: return 0.65

async def analizar_manabox_ck(content: bytes, db):
    try:
        df = pd.read_csv(io.BytesIO(content))
        df.columns = [c.strip().lower() for c in df.columns]
        col_name = next((c for c in df.columns if 'name' in c), 'name')
        col_price = next((c for c in df.columns if 'card kingdom' in c), 
                    next((c for c in df.columns if 'market' in c), 
                    next((c for c in df.columns if 'price' in c), None)))      
        col_qty = next((c for c in df.columns if 'quantity' in c or 'qty' in c or 'count' in c), 'quantity')
        if not col_price: return {"error": "Sin precio Card Kingdom detectado."}
        results = []
        for _, row in df.iterrows():
            price_usd = clean_currency(row.get(col_price, 0))
            if price_usd <= 0.05: continue
            qty = int(clean_currency(row.get(col_qty, 1)))
            multiplier = get_pricing_tier(price_usd)
            offer_unit = int(price_usd * settings.USD_TO_CLP * multiplier)
            results.append({
                "name": row.get(col_name, 'Unknown'),
                "qty": qty,
                "price_usd_ref": price_usd,
                "offer_clp_unit": offer_unit,
                "offer_clp_total": offer_unit * qty,
                "status": "HIGH END 💎" if price_usd >= 50 else ("BULK" if price_usd < 1 else "APROBADO")
            })
        results.sort(key=lambda x: x['price_usd_ref'], reverse=True)
        return results
    except Exception as e:
        return {"error": str(e)}