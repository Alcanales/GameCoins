import pandas as pd
import io
import logging
import uuid
import aiohttp
import asyncio
from sqlalchemy.orm import Session
from .config import settings
from .models import GameCoinUser

# Configuración de Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- JUMPSELLER SYNC (NUEVO) ---

async def fetch_jumpseller_customers():
    """Descarga todos los clientes de Jumpseller paginando."""
    url = f"{settings.JUMPSELLER_API_BASE}/customers.json"
    params = {
        "login": settings.JUMPSELLER_LOGIN,
        "authtoken": settings.JUMPSELLER_API_TOKEN,
        "limit": 50,
        "page": 1
    }
    
    all_customers = []
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        logger.error(f"Error Jumpseller Sync: {resp.status}")
                        break
                        
                    data = await resp.json()
                    if not data:
                        break # No hay más clientes
                        
                    all_customers.extend(data)
                    
                    if len(data) < 50:
                        break # Última página
                    
                    params["page"] += 1
                    
            except Exception as e:
                logger.error(f"Excepción en Sync Jumpseller: {e}")
                break
                
    return all_customers

async def sync_users_to_db(db: Session):
    """Lógica principal: Trae clientes de Jumpseller y los crea en GameCoins DB."""
    logger.info("Iniciando Sincronización de Usuarios Jumpseller...")
    customers = await fetch_jumpseller_customers()
    
    added = 0
    updated = 0
    
    for c in customers:
        customer_data = c.get('customer', {})
        email = customer_data.get('email', '').strip().lower()
        
        if not email: continue

        # Buscar si ya existe
        user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
        
        # Datos útiles
        name = customer_data.get('name', '')
        surname = customer_data.get('surname', '')
        # Jumpseller a veces guarda el RUT en campos adicionales o taxid, 
        # aquí asumimos que podría venir en 'taxid' o lo dejamos null si no está claro.
        rut = customer_data.get('taxid', None) 

        if not user:
            # CREAR NUEVO
            new_user = GameCoinUser(
                email=email,
                name=name,
                surname=surname,
                rut=rut,
                saldo=0 # Empieza con 0 puntos
            )
            db.add(new_user)
            added += 1
        else:
            # ACTUALIZAR DATOS (Si cambió nombre o RUT)
            if user.name != name or user.surname != surname:
                user.name = name
                user.surname = surname
                if rut: user.rut = rut
                updated += 1
    
    db.commit()
    logger.info(f"Sync Finalizado. Agregados: {added}, Actualizados: {updated}")
    return {"added": added, "updated": updated, "total_scanned": len(customers)}

# --- JUMPSELLER COUPONS (MANTENIDO) ---

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
                if resp.status not in [200, 201]:
                    return None
                data = await resp.json()
                return data.get("promotion", {}).get("code", code)
        except:
            return None

# --- BUYLIST LOGIC (MANTENIDA) ---

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