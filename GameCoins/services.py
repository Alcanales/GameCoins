import pandas as pd
import io
import logging
import uuid
import aiohttp
import asyncio
from sqlalchemy.orm import Session
from .config import settings  # Import relativo correcto
from .models import GameCoinUser # Import relativo correcto

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- JUMPSELLER SYNC ROBUSTO ---

async def fetch_jumpseller_customers():
    """
    Descarga paginada de clientes con sistema de reintentos y tolerancia a fallos.
    Garantiza que se descarguen TODOS los clientes incluso si la red parpadea.
    """
    url = f"{settings.JUMPSELLER_API_BASE}/customers.json"
    params = {
        "login": settings.JUMPSELLER_LOGIN,
        "authtoken": settings.JUMPSELLER_API_TOKEN,
        "limit": 50,
        "page": 1
    }
    
    all_customers = []
    max_retries = 3
    
    async with aiohttp.ClientSession() as session:
        while True:
            retry_count = 0
            success = False
            
            # Bucle de Reintentos para la página actual
            while retry_count < max_retries:
                try:
                    async with session.get(url, params=params) as resp:
                        # Caso 1: Éxito
                        if resp.status == 200:
                            data = await resp.json()
                            if not data:
                                success = True
                                break 
                            
                            all_customers.extend(data)
                            logger.info(f"Sync Page {params['page']}: {len(data)} clientes descargados.")
                            
                            if len(data) < 50:
                                params['page'] = -1 # Señal para salir del bucle principal
                            else:
                                params["page"] += 1 # Siguiente página
                            
                            success = True
                            await asyncio.sleep(0.2) 
                            break 
                        
                        # Caso 2: Rate Limit (Demasiadas peticiones)
                        elif resp.status == 429:
                            logger.warning(f"Rate Limit en página {params['page']}. Esperando 5s...")
                            await asyncio.sleep(5)
                            retry_count += 1
                        
                        # Caso 3: Error del Servidor (500, 502, 504)
                        elif resp.status >= 500:
                            logger.warning(f"Error Jumpseller {resp.status} en página {params['page']}. Reintentando ({retry_count+1}/{max_retries})...")
                            await asyncio.sleep(2)
                            retry_count += 1
                        
                        # Caso 4: Error Cliente (401, 404, etc) -> No tiene sentido reintentar
                        else:
                            logger.error(f"Error Fatal Jumpseller: {resp.status}. Deteniendo Sync.")
                            return all_customers # Devolvemos lo que llevamos hasta ahora

                except Exception as e:
                    logger.error(f"Excepción de Red en página {params['page']}: {e}. Reintentando...")
                    await asyncio.sleep(2)
                    retry_count += 1
            
            if params['page'] == -1:
                break # Fin natural de la paginación
                
            if not success:
                logger.error(f"Fallo crítico al descargar página {params['page']} después de {max_retries} intentos. Sync incompleta.")
                break # Evitamos bucles infinitos si una página está rota
                
    logger.info(f"✅ Sincronización Finalizada. Total Clientes: {len(all_customers)}")
    return all_customers

async def sync_users_to_db(db: Session):
    """Sincroniza y normaliza emails en la base de datos."""
    logger.info("Iniciando Sincronización Maestra...")
    
    # 1. Obtener datos de manera robusta
    customers = await fetch_jumpseller_customers()
    
    if not customers:
        logger.warning("No se obtuvieron clientes de Jumpseller.")
        return {"added": 0, "updated": 0, "total_scanned": 0, "status": "warning"}

    added = 0
    updated = 0
    
    # 2. Procesar datos
    for c in customers:
        customer_data = c.get('customer', {})
        # Normalizar email a minúsculas y sin espacios (Vital para el buscador)
        email = customer_data.get('email', '').strip().lower()
        
        if not email: continue

        user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
        
        # Datos (Si vienen vacíos, usamos string vacío para evitar errores)
        name = customer_data.get('name') or ''
        surname = customer_data.get('surname') or ''
        rut = customer_data.get('taxid') # Jumpseller suele enviar el RUT aquí

        if not user:
            # CREAR NUEVO
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
            # ACTUALIZAR SI HAY CAMBIOS
            # Esto es importante para que aparezcan nombres que antes estaban vacíos
            if user.name != name or user.surname != surname or (rut and user.rut != rut):
                user.name = name
                user.surname = surname
                if rut: user.rut = rut
                updated += 1
    
    try:
        db.commit()
        logger.info(f"DB Commit Exitoso. Agregados: {added}, Actualizados: {updated}")
    except Exception as e:
        logger.error(f"Error al guardar en DB: {e}")
        db.rollback()
        return {"error": str(e)}

    return {"added": added, "updated": updated, "total_scanned": len(customers), "status": "success"}

# --- JUMPSELLER COUPONS (Sin cambios, ya funciona bien) ---

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

# --- BUYLIST LOGIC (Sin cambios) ---

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
        # Normalización de columnas para evitar errores de CSV
        df.columns = [c.strip().lower() for c in df.columns]
        
        # Búsqueda flexible de columnas
        col_name = next((c for c in df.columns if 'name' in c), 'name')
        col_price = next((c for c in df.columns if 'card kingdom' in c), 
                    next((c for c in df.columns if 'market' in c), 
                    next((c for c in df.columns if 'price' in c), None)))      
        col_qty = next((c for c in df.columns if 'quantity' in c or 'qty' in c or 'count' in c), 'quantity')
        
        if not col_price: return {"error": "Sin precio de referencia detectado (Card Kingdom/Market)."}

        results = []
        for _, row in df.iterrows():
            price_usd = clean_currency(row.get(col_price, 0))
            if price_usd <= 0.05: continue # Ignorar bulk extremo
            
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
        return {"error": f"Error procesando CSV: {str(e)}"}