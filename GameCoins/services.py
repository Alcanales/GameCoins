import pandas as pd
import io
import logging
import uuid
import aiohttp
import asyncio
import re
from sqlalchemy.orm import Session
from .config import settings
from .models import GamePointUser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    logger.info("🚀 Iniciando Sincronización y Corrección de Nombres...")
    customers = await fetch_jumpseller_customers()
    
    if not customers:
        return {"status": "empty", "details": "No se descargaron clientes de Jumpseller"}

    added = 0
    updated = 0
    
    for c in customers:
        customer_data = c.get('customer', {})
        email = customer_data.get('email', '').strip().lower()
        if not email: continue

        # Extraer datos asegurando que no sean None
        raw_name = customer_data.get('name')
        raw_surname = customer_data.get('surname')
        
        # Si Jumpseller devuelve None, usamos string vacío, pero intentamos 'fullname' si existe
        name = raw_name if raw_name else ''
        surname = raw_surname if raw_surname else ''
        
        # Buscar usuario en DB
        user = db.query(GamePointUser).filter(GamePointUser.email == email).first()

        if not user:
            # CREAR NUEVO
            new_user = GamePointUser(
                email=email,
                name=name,
                surname=surname,
                saldo=0
            )
            db.add(new_user)
            added += 1
        else:
            # ACTUALIZAR SIEMPRE SI EL NOMBRE EN DB ESTÁ VACÍO
            # Esto soluciona el problema de "Sin Nombre"
            db_name_empty = not user.name or user.name.strip() == ''
            name_changed = user.name != name or user.surname != surname
            
            if (db_name_empty and name) or name_changed:
                user.name = name
                user.surname = surname
                updated += 1
    
    db.commit()
    logger.info(f"✅ Sync Terminado: {added} nuevos, {updated} nombres corregidos.")
    return {"added": added, "updated": updated, "total_scanned": len(customers)}

# ... (El resto de funciones create_jumpseller_coupon y analizar_manabox se mantienen igual) ...
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