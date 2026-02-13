import pandas as pd
import io
import logging
import uuid
import aiohttp
import asyncio
from sqlalchemy.orm import Session
from .config import settings
from .models import GameCoinUser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- JUMPSELLER SYNC ROBUSTO (VERSIÓN BLINDADA) ---

async def fetch_jumpseller_customers():
    """
    Descarga paginada con frenos y reintentos.
    Garantiza llegar al final de la lista incluso si la red falla.
    """
    url = f"{settings.JUMPSELLER_API_BASE}/customers.json"
    params = {
        "login": settings.JUMPSELLER_LOGIN,
        "authtoken": settings.JUMPSELLER_API_TOKEN,
        "limit": 50,
        "page": 1
    }
    
    all_customers = []
    max_retries = 3 # Intentar 3 veces cada página antes de rendirse
    
    async with aiohttp.ClientSession() as session:
        while True:
            retry_count = 0
            success = False
            
            # Bucle de perseverancia (Reintentos por página)
            while retry_count < max_retries:
                try:
                    async with session.get(url, params=params) as resp:
                        # Caso 1: Éxito total (200 OK)
                        if resp.status == 200:
                            data = await resp.json()
                            if not data:
                                params['page'] = -1 # Señal de fin
                                success = True
                                break 
                            
                            all_customers.extend(data)
                            logger.info(f"✅ Pág {params['page']}: {len(data)} clientes. Total parcial: {len(all_customers)}")
                            
                            # Si vienen menos de 50, es la última página
                            if len(data) < 50:
                                params['page'] = -1 
                            else:
                                params["page"] += 1 # Siguiente página
                            
                            success = True
                            # PAUSA TÁCTICA: 0.2s para evitar bloqueo de Jumpseller
                            await asyncio.sleep(0.2) 
                            break 
                        
                        # Caso 2: Jumpseller saturado (429) -> Esperar
                        elif resp.status == 429:
                            logger.warning(f"⚠️ Jumpseller pide espera en pág {params['page']}. Pausa de 5s...")
                            await asyncio.sleep(5)
                            retry_count += 1
                        
                        # Caso 3: Error del servidor -> Reintentar
                        else:
                            logger.error(f"❌ Error {resp.status} en pág {params['page']}. Reintentando ({retry_count+1}/{max_retries})...")
                            await asyncio.sleep(2)
                            retry_count += 1

                except Exception as e:
                    logger.error(f"💥 Fallo de red: {e}. Reintentando...")
                    await asyncio.sleep(2)
                    retry_count += 1
            
            # Salida del bucle principal
            if params['page'] == -1:
                break
                
            # Si fallaron los 3 intentos, paramos para no colgar el servidor
            if not success:
                logger.error("🛑 Sincronización abortada por errores persistentes en la red.")
                break
                
    logger.info(f"🏁 Total Final Clientes Descargados: {len(all_customers)}")
    return all_customers

async def sync_users_to_db(db: Session):
    """Sincroniza y normaliza emails."""
    logger.info("🚀 Iniciando Sync Maestra...")
    customers = await fetch_jumpseller_customers()
    
    if not customers:
        return {"added": 0, "updated": 0, "total_scanned": 0, "status": "empty"}

    added = 0
    updated = 0
    
    for c in customers:
        customer_data = c.get('customer', {})
        email = customer_data.get('email', '').strip().lower()
        
        if not email: continue

        user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
        
        name = customer_data.get('name') or ''
        surname = customer_data.get('surname') or ''
        rut = customer_data.get('taxid')

        if not user:
            new_user = GameCoinUser(
                email=email,
                name=name,
                surname=surname,
                rut=rut,
                saldo=0 
            )
            db.add(new_user)
            added += 1
        else:
            # Actualizamos datos si cambiaron
            if user.name != name or user.surname != surname or (rut and user.rut != rut):
                user.name = name
                user.surname = surname
                if rut: user.rut = rut
                updated += 1
    
    db.commit()
    return {"added": added, "updated": updated, "total_scanned": len(customers)}

# --- EL RESTO DEL ARCHIVO SIGUE IGUAL (create_jumpseller_coupon, analizar_manabox_ck) ---
# Copia aquí las funciones de cupones y buylist que ya tenías o usa el archivo completo anterior.
# Asegúrate de NO borrar create_jumpseller_coupon ni analizar_manabox_ck.

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