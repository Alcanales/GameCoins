import aiohttp
from datetime import datetime
from sqlalchemy.orm import Session
from models import SystemConfig
from config import settings

async def crear_cupon_jumpseller(codigo: str, monto: int, email: str, db: Session):
    # 1. Recuperar credenciales de La Bóveda
    token = db.query(SystemConfig).filter(SystemConfig.key == "JUMPSELLER_API_TOKEN").first()
    store = db.query(SystemConfig).filter(SystemConfig.key == "JUMPSELLER_STORE").first()
    
    if not token or not store:
        print("ERROR CRÍTICO: Credenciales Jumpseller no configuradas en Bóveda.")
        return None
    
    url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
    
    # 2. Payload del Cupón (Reglas de Negocio)
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
    
    # 3. Llamada HTTP Asíncrona
    async with aiohttp.ClientSession() as s:
        try:
            params = {"login": store.value, "authtoken": token.value}
            async with s.post(url, params=params, json=payload) as r:
                if r.status == 201:
                    return await r.json()
                else:
                    print(f"Error Jumpseller ({r.status}): {await r.text()}")
                    return None
        except Exception as e:
            print(f"Excepción de red Jumpseller: {str(e)}")
            return None