import pandas as pd
import io
import logging
import uuid
import aiohttp
import asyncio
import re
from datetime import datetime
from sqlalchemy.orm import Session
from .config import settings
from .models import GamePointUser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def sanitize_name(text):
    if not text or pd.isna(text): return ""
    text = str(text).strip()
    return re.sub(r'\s+', ' ', text).title()

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

async def analizar_manabox_ck(content: bytes):
    try:
        df = pd.read_csv(io.BytesIO(content))
        df.columns = [c.strip().lower() for c in df.columns]
        
        col_name = next((c for c in df.columns if 'name' in c), 'name')
        col_qty = next((c for c in df.columns if c in ['quantity', 'qty', 'count', 'amount']), 'quantity')
        
        posibles_precios = [c for c in df.columns if 'card kingdom' in c or 'cardkingdom' in c or 'ck' in c]
        if not posibles_precios:
            col_price = next((c for c in df.columns if 'price' in c or 'market' in c), None)
            if not col_price: return {"error": "No se detectó columna de precio (Card Kingdom/Price)."}
        else:
            col_price = posibles_precios[0]

        results = []
        for _, row in df.iterrows():
            price_usd = clean_currency(row.get(col_price, 0))
            if price_usd <= 0.05: continue
            qty = int(clean_currency(row.get(col_qty, 1)))
            if qty < 1: continue

            multiplier = get_pricing_tier(price_usd)
            offer_unit = int(price_usd * settings.USD_TO_CLP * multiplier)
            
            status_label = "APROBADO"
            if price_usd >= 50: status_label = "HIGH END 💎"
            elif price_usd < settings.MIN_PURCHASE_USD: status_label = "BULK"

            results.append({
                "name": row.get(col_name, 'Unknown'),
                "qty": qty,
                "price_usd_ref": price_usd,
                "offer_clp_unit": offer_unit,
                "offer_clp_total": offer_unit * qty,
                "multiplier_used": f"{int(multiplier*100)}%",
                "status": status_label
            })

        results.sort(key=lambda x: x['price_usd_ref'], reverse=True)
        return results
    except Exception as e:
        return {"error": str(e)}

async def fetch_jumpseller_customers():
    url = f"{settings.JUMPSELLER_API_BASE}/customers.json"
    params = {
        "login": settings.JS_LOGIN_CODE,
        "authtoken": settings.JS_AUTH_TOKEN,
        "limit": 50,
        "page": 1
    }
    all_customers = []
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(url, params=params) as resp:
                    if resp.status == 429: 
                        await asyncio.sleep(2)
                        continue
                    if resp.status != 200: break
                    data = await resp.json()
                    if not data: break 
                    all_customers.extend(data)
                    if len(data) < 50: break 
                    params["page"] += 1
                    await asyncio.sleep(0.5) 
            except: break
    return all_customers

async def sync_users_to_db(db: Session):
    customers = await fetch_jumpseller_customers()
    if not customers: return {"status": "empty"}

    added, updated = 0, 0
    for c in customers:
        cust = c.get('customer', {})
        email = cust.get('email', '').strip().lower()
        if not email: continue

        final_name = sanitize_name(cust.get('name') or cust.get('billing_address', {}).get('name'))
        final_surname = sanitize_name(cust.get('surname') or cust.get('billing_address', {}).get('surname'))
        if not final_name: final_name = email.split('@')[0]

        user = db.query(GamePointUser).filter(GamePointUser.email == email).first()
        if not user:
            new_user = GamePointUser(email=email, name=final_name, surname=final_surname)
            db.add(new_user)
            added += 1
        else:
            if user.name != final_name:
                user.name = final_name
                updated += 1
    
    db.commit()
    return {"added": added, "updated": updated}

async def create_jumpseller_coupon(email: str, amount: int, user_name: str, max_retries: int = 3):
    unique_suffix = uuid.uuid4().hex[:6].upper()
    code = f"GQ-{unique_suffix}"
    
    fecha_hoy = datetime.now().strftime("%d/%m/%Y")
    promotion_name = f"Canje: {user_name} ({email}) - {fecha_hoy}"

    url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
    params = {
        "login": settings.JS_LOGIN_CODE,
        "authtoken": settings.JS_AUTH_TOKEN
    }
    
    monto_puro = int(amount)
    # PAYLOAD OMNIBUS
    payload = {
        "promotion": {
            "name": promotion_name,
            "code": code,
            "enabled": True,
            "type": "fixed",                
            "discount_target": "order",     
            "amount": monto_puro,
            "discount": monto_puro,
            "discount_amount": monto_puro,
            "discount_value": monto_puro,
            "value": monto_puro,
            "usage_limit": 1
        }
    }
    
    async with aiohttp.ClientSession() as session:
        for attempt in range(max_retries):
            try:
                async with session.post(url, params=params, json=payload) as resp:
                    if resp.status == 429:
                        wait_time = 2 ** attempt
                        logger.warning(f"Jumpseller Rate Limit. Reintentando en {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        continue
                    
                    if resp.status not in [200, 201]:
                        err = await resp.text()
                        logger.error(f"Jumpseller Error {resp.status}: {err}")
                        return None
                    
                    data = await resp.json()
                    return data.get("promotion", {}).get("code", code)
            except Exception as e:
                logger.error(f"Error conexión: {e}")
                if attempt == max_retries - 1:
                    return None
                await asyncio.sleep(1)
                
        return None
