import aiohttp
import asyncio
from datetime import datetime
from sqlalchemy.orm import Session
from models import SystemConfig
from config import settings

def get_db_config(db: Session, key: str) -> str:
    """Obtiene configuración persistente de la DB"""
    item = db.query(SystemConfig).filter(SystemConfig.key == key).first()
    return item.value if item else ""

async def crear_cupon_jumpseller(session, codigo, descuento, email, db: Session):
    token = get_db_config(db, "JUMPSELLER_API_TOKEN")
    store = get_db_config(db, "JUMPSELLER_STORE")
    
    if not token or not store:
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
    params = {"login": store, "authtoken": token}
    
    try:
        async with session.post(url, params=params, json=body, timeout=10) as resp:
            if resp.status < 300: return await resp.json()
            else: print(f"Error Jumpseller: {await resp.text()}")
    except Exception as e:
        print(f"Excepción Jumpseller: {e}")
    return None
