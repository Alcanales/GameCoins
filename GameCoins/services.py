import pandas as pd
import io
import logging
import uuid
import aiohttp
from .config import settings

# Configuración de Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- LÓGICA DE JUMPSELLER (NUEVO) ---

async def create_jumpseller_coupon(email: str, amount: int):
    """
    Crea un cupón de descuento en Jumpseller por el monto especificado.
    Retorna el código del cupón generado.
    """
    code = f"GQ-{uuid.uuid4().hex[:8].upper()}" # Ej: GQ-A1B2C3D4
    
    url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
    params = {
        "login": settings.JUMPSELLER_LOGIN,
        "authtoken": settings.JUMPSELLER_API_TOKEN
    }
    
    # Configuración del Cupón Jumpseller
    payload = {
        "promotion": {
            "name": f"Canje QuestPoints - {email}",
            "code": code,
            "enabled": True,
            "discount_type": "fixed", # Descuento en dinero fijo ($)
            "value": amount,
            "usage_limit": 1, # Solo 1 uso
            "minimum_order_amount": 0,
            # Opcional: Podríamos expirar el cupón en 1 año si quisieras
            # "expires_at": "2025-12-31" 
        }
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, params=params, json=payload) as resp:
                if resp.status not in [200, 201]:
                    text = await resp.text()
                    logger.error(f"Error Jumpseller ({resp.status}): {text}")
                    return None
                
                data = await resp.json()
                # Jumpseller retorna el objeto creado, confirmamos el código
                return data.get("promotion", {}).get("code", code)
                
        except Exception as e:
            logger.error(f"Excepción conectando a Jumpseller: {e}")
            return None

# --- LÓGICA DE BUYLIST (MANTENIDA) ---

def clean_currency(value):
    """Convierte formatos como '$1,200.50' o '1.200,50' a float estándar."""
    if pd.isna(value): return 0.0
    if isinstance(value, (int, float)): return float(value)
    
    val_str = str(value).replace('$', '').strip()
    val_str = val_str.replace(',', '') 
    try:
        return float(val_str)
    except ValueError:
        return 0.0

def get_pricing_tier(price_usd: float) -> float:
    """Tiered Pricing Strategy"""
    if price_usd <= 0: return 0.0
    if price_usd < 3.0: return 0.30     # Tier 1
    elif price_usd < 10.0: return 0.45  # Tier 2
    elif price_usd < 50.0: return 0.55  # Tier 3
    else: return 0.65                   # Tier 4

async def analizar_manabox_ck(content: bytes, db):
    try:
        df = pd.read_csv(io.BytesIO(content))
        df.columns = [c.strip().lower() for c in df.columns]
        
        col_name = next((c for c in df.columns if 'name' in c), 'name')
        col_price = next((c for c in df.columns if 'card kingdom' in c), 
                    next((c for c in df.columns if 'market' in c), 
                    next((c for c in df.columns if 'price' in c), None)))      
        col_qty = next((c for c in df.columns if 'quantity' in c or 'qty' in c or 'count' in c), 'quantity')
        col_condition = next((c for c in df.columns if 'condition' in c), 'condition')
        col_foil = next((c for c in df.columns if 'foil' in c), None)

        if not col_price:
            return {"error": "El archivo no contiene precios. Exporta desde ManaBox seleccionando 'Card Kingdom Price'."}

        results = []
        
        for _, row in df.iterrows():
            name = row.get(col_name, 'Unknown')
            qty = int(clean_currency(row.get(col_qty, 1)))
            price_usd = clean_currency(row.get(col_price, 0))
            
            is_foil = False
            if col_foil:
                val_foil = str(row.get(col_foil, '')).lower()
                is_foil = val_foil in ['true', 'yes', 'foil', '1']

            if price_usd <= 0.05: continue

            multiplier = get_pricing_tier(price_usd)
            offer_clp_unit = int(price_usd * settings.USD_TO_CLP * multiplier)
            offer_clp_total = offer_clp_unit * qty

            status = "APROBADO"
            if price_usd >= 50.0: status = "HIGH END 💎"
            elif price_usd < 1.0: status = "BULK"

            results.append({
                "name": name,
                "qty": qty,
                "price_usd_ref": price_usd,
                "offer_clp_unit": offer_clp_unit,
                "offer_clp_total": offer_clp_total,
                "condition": row.get(col_condition, 'NM'),
                "is_foil": is_foil,
                "multiplier_label": f"{int(multiplier*100)}%",
                "status": status,
                "source": "ManaBox/CK"
            })

        results.sort(key=lambda x: x['price_usd_ref'], reverse=True)
        return results

    except Exception as e:
        logger.error(f"Error analizando CSV: {e}")
        return {"error": f"Error al procesar el archivo: {str(e)}"}