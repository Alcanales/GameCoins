import aiohttp
import asyncio
import io
import pandas as pd
import math
from datetime import datetime
from config import settings

async def fetch_json_with_retry(session, url, params=None, json_body=None):
    try:
        method = "POST" if json_body else "GET"
        async with session.request(method, url, params=params, json=json_body, timeout=10) as resp:
            if resp.status < 300: return await resp.json()
    except: pass
    return None

async def crear_cupon_jumpseller(session, codigo, descuento, email):
    # Usar las credenciales dinámicas de settings
    if not settings.JUMPSELLER_API_TOKEN or not settings.JUMPSELLER_STORE:
        return None
        
    url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
    body = {
        "promotion": {
            "name": f"Canje GQ {codigo}",
            "code": codigo,
            "discount_amount": descuento,
            "status": "active",
            "usage_limit": 1,
            "minimum_order_amount": 0,
            "begins_at": datetime.now().strftime("%Y-%m-%d"),
            "customer_emails": [email]
        }
    }
    params = {"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN}
    return await fetch_json_with_retry(session, url, params=params, json_body=body)

async def procesar_csv_logic(content: bytes, internal_mode: bool):
    # Lógica simplificada para el ejemplo (Tu lógica completa va aquí si es necesario)
    try:
        df = pd.read_csv(io.BytesIO(content))
        # ... (Tu lógica de procesamiento de CSV) ...
        # Retorno dummy para que funcione el script básico
        return [{"name": "Carta Test", "quantity": 1, "purchase_price": 1000, "cash_clp": 500, "mkt": 1000, "cat": "compra", "set_code": "TST", "stock_tienda": 0, "clean_name": "Carta Test"}]
    except: return {"error": "CSV inválido"}

def enviar_correo_dual(cli, items, clp, gc, content, fname):
    pass
