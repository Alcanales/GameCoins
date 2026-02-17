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

# --- UTILIDADES ---
def sanitize_name(text):
    if not text or pd.isna(text): return ""
    text = str(text).strip()
    text = re.sub(r'\s+', ' ', text)
    return text.title()

def clean_currency(value):
    """Convierte strings de dinero ($1,200.50) a float."""
    if pd.isna(value): return 0.0
    if isinstance(value, (int, float)): return float(value)
    val_str = str(value).replace('$', '').replace(',', '').strip()
    try: return float(val_str)
    except: return 0.0

def get_pricing_tier(price_usd: float) -> float:
    """Define el porcentaje de compra basado en el valor de la carta."""
    if price_usd <= 0: return 0.0
    # Tier List Ajustable
    if price_usd < 3.0: return 0.30   # 30% para cartas baratas
    elif price_usd < 10.0: return 0.45 # 45% standard
    elif price_usd < 50.0: return 0.55 # 55% mid-range
    else: return 0.65                  # 65% high-end

# --- ANALISIS BUYLIST (MANABOX -> CARD KINGDOM) ---
async def analizar_manabox_ck(content: bytes):
    try:
        # 1. Leer CSV
        df = pd.read_csv(io.BytesIO(content))
        
        # 2. Normalizar columnas (lowercase, strip) para búsqueda flexible
        df.columns = [c.strip().lower() for c in df.columns]
        
        # 3. Detectar columnas clave
        # Busca columna de nombre
        col_name = next((c for c in df.columns if 'name' in c), 'name')
        
        # Busca columna de cantidad
        col_qty = next((c for c in df.columns if c in ['quantity', 'qty', 'count', 'amount']), 'quantity')
        
        # 4. LÓGICA ESPECÍFICA MANABOX/CARD KINGDOM
        # Buscamos columnas que contengan "card kingdom" o "ck"
        posibles_precios = [c for c in df.columns if 'card kingdom' in c or 'cardkingdom' in c]
        
        # Fallback: si no hay CK explícito, buscamos "price" o "market" pero advertimos
        if not posibles_precios:
            col_price = next((c for c in df.columns if 'price' in c or 'market' in c), None)
            if not col_price:
                return {"error": "No se detectó columna de precio 'Card Kingdom' en el CSV."}
        else:
            # Tomamos la primera coincidencia de Card Kingdom (ej: 'price (card kingdom)')
            col_price = posibles_precios[0]

        results = []
        
        for _, row in df.iterrows():
            # Obtener precio base CK
            price_usd = clean_currency(row.get(col_price, 0))
            
            # Filtro: Ignorar cartas de menos de 5 centavos o sin precio
            if price_usd <= 0.05: continue
            
            qty = int(clean_currency(row.get(col_qty, 1)))
            if qty < 1: continue

            # Cálculo de Oferta
            multiplier = get_pricing_tier(price_usd)
            
            # Fórmula: (Precio USD * Dolar Tienda * Multiplicador)
            offer_unit = int(price_usd * settings.USD_TO_CLP * multiplier)
            
            # Etiquetado
            status_label = "APROBADO"
            if price_usd >= 50: status_label = "HIGH END 💎"
            elif price_usd < 1: status_label = "BULK"

            results.append({
                "name": row.get(col_name, 'Unknown'),
                "qty": qty,
                "price_usd_ref": price_usd,          # Precio Card Kingdom
                "offer_clp_unit": offer_unit,        # Oferta unitaria
                "offer_clp_total": offer_unit * qty, # Total línea
                "multiplier_used": f"{int(multiplier*100)}%",
                "status": status_label
            })

        # Ordenar por valor (más caras primero)
        results.sort(key=lambda x: x['price_usd_ref'], reverse=True)
        return results

    except Exception as e:
        logger.error(f"Error procesando CSV: {str(e)}")
        return {"error": f"Error al procesar el archivo: {str(e)}"}

# --- JUMPSELLER SYNC & COUPONS ---
async def fetch_jumpseller_customers():
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
    logger.info("🚀 Iniciando Sincronización...")
    customers = await fetch_jumpseller_customers()
    
    if not customers:
        return {"status": "empty", "details": "No se descargaron clientes."}

    added, updated = 0, 0
    
    for c in customers:
        cust = c.get('customer', {})
        email = cust.get('email', '').strip().lower()
        if not email: continue

        # Lógica de nombres: Billing > Customer > Shipping
        billing = cust.get('billing_address', {})
        shipping = cust.get('shipping_address', {})
        
        candidates = [
            (billing.get('name'), billing.get('surname')),
            (cust.get('name'), cust.get('surname')),
            (shipping.get('name'), shipping.get('surname'))
        ]

        final_name, final_surname = "", ""
        for n, s in candidates:
            cn, cs = sanitize_name(n), sanitize_name(s)
            if cn: 
                final_name, final_surname = cn, cs
                break
        
        if not final_name:
            final_name = sanitize_name(email.split('@')[0])

        # Guardado en DB (ID basado en Email)
        user = db.query(GamePointUser).filter(GamePointUser.email == email).first()

        if not user:
            new_user = GamePointUser(email=email, name=final_name, surname=final_surname, saldo=0)
            db.add(new_user)
            added += 1
        else:
            if user.name != final_name or user.surname != final_surname:
                user.name = final_name
                user.surname = final_surname
                updated += 1
    
    db.commit()
    return {"added": added, "updated": updated, "total_scanned": len(customers)}

async def create_jumpseller_coupon(email: str, amount: int):
    code = f"GQ-{uuid.uuid4().hex[:8].upper()}"
    url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
    params = {"login": settings.JUMPSELLER_LOGIN, "authtoken": settings.JUMPSELLER_API_TOKEN}
    
    # Payload optimizado para cupón de uso único
    payload = {
        "promotion": {
            "name": f"Canje Puntos - {email}",
            "code": code,
            "enabled": True,
            "discount_type": "fixed",
            "value": amount,
            "usage_limit": 1,  # Importante: Solo se usa una vez
            "minimum_order_amount": 0,
            # Opcional: Vincular el cupón solo a este cliente si Jumpseller lo permite en tu plan
            # "customer_categories": [...] 
        }
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, params=params, json=payload) as resp:
                if resp.status not in [200, 201]: 
                    logger.error(f"Fallo Jumpseller API: {resp.status}")
                    return None
                data = await resp.json()
                return data.get("promotion", {}).get("code", code)
        except Exception as e: 
            logger.error(f"Excepción conectando a Jumpseller: {e}")
            return None