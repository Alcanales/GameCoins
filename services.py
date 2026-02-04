import logging
from datetime import datetime
from sqlalchemy.orm import Session
from models import SystemConfig, GameCoinUser
from config import settings
from sqlalchemy.exc import IntegrityError

logging.basicConfig(level=logging.ERROR)

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
            "usage_limit": 1,              # Solo 1 uso total
            "customer_emails": [email],    # Restringido al cliente
            "begins_at": datetime.now().strftime("%Y-%m-%d")
        }
    }
    
    async with aiohttp.ClientSession() as s:
        try:
            params = {"login": store.value, "authtoken": token.value}
            async with s.post(url, params=params, json=payload) as r:
                if r.status == 201:
                    return await r.json()
                else:
                    logging.error(f"Error Jumpseller ({r.status}): {await r.text()}")
                    return None
        except Exception as e:
            logging.error(f"Excepción de red Jumpseller: {str(e)}")
            return None

async def procesar_canje_atomico(email: str, monto: int, db: Session):
    if settings.MAINTENANCE_MODE_CANJE:
        return {"status": "error", "detail": "Modo mantenimiento activado"}
    
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
    if not user or user.saldo < monto:
        return {"status": "error", "detail": "Saldo insuficiente"}
    
    # Transacción atómica
    try:
        # Código único con manejo de error para email inválido
        try:
            codigo = f"GQ-{email.split('@')[0]}-{monto}"
        except IndexError:
            logging.error(f"Email inválido para generación de código: {email}")
            raise ValueError("Email inválido")
        
        cupon = await crear_cupon_jumpseller(codigo, monto, email, db)
        if not cupon:
            raise ValueError("Fallo en creación de cupón")
        
        user.saldo -= monto
        user.historico_canjeado += monto
        db.commit()
        return {"status": "ok", "cupon_codigo": codigo}
    except Exception as e:
        db.rollback()
        logging.error(f"Error en transacción de canje: {str(e)}")
        return {"status": "error", "detail": str(e)}

# ... imports previos ...

async def fetch_scryfall_prices(scryfall_id: str):
    """Fetch precios de Scryfall API usando ID."""
    url = f"https://api.scryfall.com/cards/{scryfall_id}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    prices = data.get('prices', {})
                    return {
                        'price_normal': float(prices.get('usd', 0)) or 0.0,
                        'price_foil': float(prices.get('usd_foil', 0)) or 0.0
                    }
                else:
                    logging.error(f"Error Scryfall API ({response.status}) para ID {scryfall_id}")
                    return {'price_normal': 0.0, 'price_foil': 0.0}
        except Exception as e:
            logging.error(f"Excepción al fetch Scryfall: {str(e)}")
            return {'price_normal': 0.0, 'price_foil': 0.0}


# ¡Aquí está el fix! Cambia def → async def
async def analizar_csv_estacas(file_content: bytes):
    import pandas as pd
    from io import BytesIO
    import os
    try:
        df = pd.read_csv(BytesIO(file_content))
        df.columns = [str(c).lower().strip() for c in df.columns]
        
        required_cols = ['name', 'scryfall id']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Columnas faltantes: {', '.join(missing_cols)}")
        
        # Límites de .env
        stock_default = int(os.getenv('STOCK_LIMIT_DEFAULT', 8))
        stock_high = int(os.getenv('STOCK_LIMIT_HIGH_DEMAND', 20))
        min_spread = float(os.getenv('MIN_STAKE_SPREAD', 10.0))
        stake_min_spread = float(os.getenv('STAKE_MIN_SPREAD', 25.0))
        ratio_threshold = float(os.getenv('STAKE_RATIO_THRESHOLD', 2.5))
        
        resultados = []
        for _, row in df.iterrows():
            nombre = str(row.get('name', 'Desconocido'))
            scryfall_id = str(row.get('scryfall id', ''))
            purchase_price = float(row.get('purchase price', 0))
            
            # Fetch o fallback
            if 'price_normal' in row and 'price_foil' in row:
                try:
                    pn = float(row['price_normal'])
                    pf = float(row['price_foil'])
                except ValueError:
                    pn = pf = 0.0
            else:
                if scryfall_id:
                    prices = await fetch_scryfall_prices(scryfall_id)   # ← await OK ahora
                    pn = prices['price_normal']
                    pf = prices['price_foil']
                else:
                    pn = pf = purchase_price
            
            current_stock = int(row.get('quantity', 0))
            
            status, razon = "APROBADO", "OK"
            stock_limit = stock_high if pn >= 20.0 else stock_default
            
            spread = abs(pf - pn)
            if spread < min_spread:
                status, razon = "RECHAZADO (SPREAD BAJO)", f"Spread {spread:.1f} < {min_spread}"
            elif spread < stake_min_spread and pn < 20.0:
                status, razon = "RECHAZADO (STAKE SPREAD)", f"Spread {spread:.1f} < {stake_min_spread}"
            
            if pf >= 20.0 and pn < 20.0:
                ratio = pf / pn if pn > 0 else 999
                if ratio > ratio_threshold and spread > min_spread:
                    status, razon = "RECHAZADO (ESTACA)", f"Ratio {ratio:.1f}x peligroso"
            
            elif pn >= 20.0:
                status, razon = "HIGH END", "Staple Seguro"
            
            if current_stock >= stock_limit:
                status, razon = "RECHAZADO (STOCK FULL)", f"Stock {current_stock} >= {stock_limit}"
            
            resultados.append({
                "name": nombre,
                "price_normal": pn,
                "price_foil": pf,
                "current_stock": current_stock,
                "stock_limit": stock_limit,
                "status": status,
                "razon": razon
            })
        return pd.DataFrame(resultados)
    except Exception as e:
        logging.error(f"Error procesando CSV: {str(e)}")
        return {"error": str(e)}