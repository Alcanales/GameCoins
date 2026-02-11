import logging
import asyncio
import os
import aiohttp
import pandas as pd
from io import BytesIO
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from models import SystemConfig, GameCoinUser
from config import settings

logging.basicConfig(level=logging.ERROR)

# --- JUMPSELLER SERVICES ---

async def crear_cupon_jumpseller(codigo: str, monto: int, email: str, db: Session):
    token = db.query(SystemConfig).filter(SystemConfig.key == "JUMPSELLER_API_TOKEN").first()
    store = db.query(SystemConfig).filter(SystemConfig.key == "JUMPSELLER_STORE").first()
    
    if not token or not store:
        logging.error("ERROR CRÍTICO: Credenciales Jumpseller no configuradas en Bóveda.")
        return None
    
    url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
    
    payload = {
        "promotion": {
            "name": f"Canje GQ {codigo}",
            "code": codigo,
            "discount_amount": monto,
            "status": "active",
            "usage_limit": 1,
            "customer_emails": [email],
            "begins_at": datetime.now().strftime("%Y-%m-%d")
        }
    }
    
    async with aiohttp.ClientSession() as s:
        try:
            headers = {"Content-Type": "application/json"}
            params = {"login": store.value, "authtoken": token.value}
            
            async with s.post(url, params=params, json=payload, headers=headers, timeout=10) as r:
                if r.status == 201:
                    return await r.json()
                else:
                    error_text = await r.text()
                    logging.error(f"Error Jumpseller ({r.status}): {error_text}")
                    return None
        except Exception as e:
            logging.error(f"Excepción de red Jumpseller: {str(e)}")
            return None

async def procesar_canje_atomico(email: str, monto: int, db: Session):
    if settings.MAINTENANCE_MODE_CANJE:
        return {"status": "error", "detail": "Modo mantenimiento activado"}
    
    # 1. Verificación de Saldo
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
    if not user or user.saldo < monto:
        return {"status": "error", "detail": "Saldo insuficiente"}
    
    try:
        codigo = f"GQ-{email.split('@')[0]}-{monto}"
    except IndexError:
        return {"status": "error", "detail": "Email inválido"}

    # 2. Bloqueo Optimista
    try:
        user.saldo -= monto
        user.historico_canjeado += monto
        db.flush()

        # 3. Llamada API Externa
        cupon = await crear_cupon_jumpseller(codigo, monto, email, db)
        
        if not cupon:
            db.rollback()
            return {"status": "error", "detail": "Fallo en Jumpseller, puntos devueltos"}
        
        # 4. Confirmación final
        db.commit()
        return {"status": "ok", "cupon_codigo": codigo}

    except Exception as e:
        db.rollback()
        logging.error(f"Error transacción canje: {str(e)}")
        return {"status": "error", "detail": "Error interno del servidor"}

# --- MTG / SCRYFALL SERVICES ---

async def fetch_scryfall_prices(session, scryfall_id: str):
    if not scryfall_id or str(scryfall_id).lower() == 'nan':
         return {'price_normal': 0.0, 'price_foil': 0.0}

    url = f"https://api.scryfall.com/cards/{scryfall_id}"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                prices = data.get('prices', {})
                return {
                    'price_normal': float(prices.get('usd', 0) or 0.0),
                    'price_foil': float(prices.get('usd_foil', 0) or 0.0)
                }
            return {'price_normal': 0.0, 'price_foil': 0.0}
    except Exception:
        return {'price_normal': 0.0, 'price_foil': 0.0}

async def analizar_csv_estacas(file_content: bytes):
    try:
        df = pd.read_csv(BytesIO(file_content))
        df.columns = [str(c).lower().strip() for c in df.columns]

        # Validaciones
        required = ['name', 'scryfall id']
        if not all(col in df.columns for col in required):
            missing = [c for c in required if c not in df.columns]
            return {"error": f"Faltan columnas: {', '.join(missing)}"}

        # Configuración ENV
        stock_default = int(os.getenv('STOCK_LIMIT_DEFAULT', 8))
        stock_high = int(os.getenv('STOCK_LIMIT_HIGH_DEMAND', 20))
        min_spread = float(os.getenv('MIN_STAKE_SPREAD', 10.0))
        stake_min_spread = float(os.getenv('STAKE_MIN_SPREAD', 25.0))
        ratio_threshold = float(os.getenv('STAKE_RATIO_THRESHOLD', 2.5))

        # Fetch Asíncrono de Scryfall
        rows_to_fetch = []
        for idx, row in df.iterrows():
            if 'price_normal' not in row or 'price_foil' not in row:
                rows_to_fetch.append((idx, row.get('scryfall id')))
        
        if rows_to_fetch:
            async with aiohttp.ClientSession() as session:
                tasks = [fetch_scryfall_prices(session, rid) for _, rid in rows_to_fetch]
                results = await asyncio.gather(*tasks)
            
            for (idx, _), prices in zip(rows_to_fetch, results):
                df.at[idx, 'price_normal'] = prices['price_normal']
                df.at[idx, 'price_foil'] = prices['price_foil']
        else:
            df['price_normal'] = df.get('price_normal', 0.0).fillna(0.0)
            df['price_foil'] = df.get('price_foil', 0.0).fillna(0.0)

        # Lógica de Negocio
        resultados = []
        for _, row in df.iterrows():
            pn = float(row['price_normal'])
            pf = float(row['price_foil'])
            current_stock = int(row.get('quantity', 0))
            nombre = row.get('name', 'Unknown')

            status, razon = "APROBADO", "OK"
            
            stock_limit = stock_high if pn >= 20.0 else stock_default
            spread = abs(pf - pn)
            
            if spread < min_spread:
                status, razon = "RECHAZADO (SPREAD BAJO)", f"Spread {spread:.2f} < {min_spread}"
            elif pn < 20.0 and spread < stake_min_spread:
                 status, razon = "RECHAZADO (STAKE SPREAD)", f"Spread bajo para carta barata"
            
            if pf >= 20.0 and pn < 20.0:
                ratio = pf / pn if pn > 0 else 999
                if ratio > ratio_threshold and spread > min_spread:
                    status, razon = "RECHAZADO (ESTACA)", f"Ratio {ratio:.1f}x sospechoso"
            
            if current_stock >= stock_limit:
                 status, razon = "RECHAZADO (STOCK FULL)", f"Stock {current_stock} >= {stock_limit}"

            # --- CÁLCULO DE OFERTAS (AGREGADO) ---
            cash_normal = round(pn * settings.CASH_MULTIPLIER)
            gc_normal = round(pn * settings.GAMECOIN_MULTIPLIER)
            cash_foil = round(pf * settings.CASH_MULTIPLIER)
            gc_foil = round(pf * settings.GAMECOIN_MULTIPLIER)
            # -------------------------------------

            resultados.append({
                "name": nombre,
                "price_normal": pn,
                "price_foil": pf,
                "current_stock": current_stock,
                "stock_limit": stock_limit,
                "status": status,
                "razon": razon,
                "cash_normal": cash_normal,
                "gc_normal": gc_normal,
                "cash_foil": cash_foil,
                "gc_foil": gc_foil
            })

        return pd.DataFrame(resultados)

    except Exception as e:
        logging.error(f"Error procesando CSV: {str(e)}")
        return {"error": f"Excepción interna: {str(e)}"}

async def sincronizar_clientes_jumpseller(db: Session):
    token = db.query(SystemConfig).filter(SystemConfig.key == "JUMPSELLER_API_TOKEN").first()
    store = db.query(SystemConfig).filter(SystemConfig.key == "JUMPSELLER_STORE").first()
    
    if not token or not store:
        return {"status": "error", "detail": "Credenciales no configuradas"}

    base_url = f"{settings.JUMPSELLER_API_BASE}/customers.json"
    page = 1
    total_synced = 0
    nuevos = 0
    
    async with aiohttp.ClientSession() as session:
        while True:
            params = {
                "login": store.value, 
                "authtoken": token.value,
                "page": page,
                "limit": 50
            }
            try:
                async with session.get(base_url, params=params) as r:
                    if r.status != 200:
                        logging.error(f"Error Jumpseller Sync: {r.status}")
                        break     
                    data = await r.json()
                    if not data: break
                    
                    for customer in data:
                        email = customer.get('email', '').strip().lower()
                        fname = customer.get('name', '').strip()
                        lname = customer.get('surname', '').strip()
                        
                        if email:
                            user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
                            if not user:
                                user = GameCoinUser(email=email, name=fname, surname=lname, saldo=0)
                                db.add(user)
                                nuevos += 1
                            else:
                                user.name = fname
                                user.surname = lname
                            
                            total_synced += 1
                    
                    db.commit()
                    page += 1
            except Exception as e:
                logging.error(f"Error Sync: {e}")
                break

    return {"status": "ok", "total": total_synced, "nuevos": nuevos}