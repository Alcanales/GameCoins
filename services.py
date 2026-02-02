import aiohttp
from datetime import datetime
from .models import SystemConfig

async def crear_cupon_jumpseller(codigo, monto, email, db):
    token = db.query(SystemConfig).filter(SystemConfig.key == "JUMPSELLER_API_TOKEN").first()
    store = db.query(SystemConfig).filter(SystemConfig.key == "JUMPSELLER_STORE").first()
    if not token or not store: return None
    url = f"https://api.jumpseller.com/v1/promotions.json"
    payload = {"promotion": {"name": f"GQ {codigo}", "code": codigo, "discount_amount": monto, "status": "active", "usage_limit": 1, "customer_emails": [email]}}
    async with aiohttp.ClientSession() as s:
        async with s.post(url, params={"login": store.value, "authtoken": token.value}, json=payload) as r:
            return await r.json() if r.status == 201 else None
