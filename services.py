import aiohttp
from datetime import datetime
from sqlalchemy.orm import Session
from models import SystemConfig # Importación absoluta corregida
from config import settings

async def crear_cupon_jumpseller(codigo: str, monto: int, email: str, db: Session):
    # 1. Recuperar credenciales desde La Bóveda (DB)
    token = db.query(SystemConfig).filter(SystemConfig.key == "JUMPSELLER_API_TOKEN").first()
    store = db.query(SystemConfig).filter(SystemConfig.key == "JUMPSELLER_STORE").first()
    
    if not token or not store:
        print("ERROR CRÍTICO: Credenciales de Jumpseller no configuradas en Bóveda.")
        return None
    
    url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
    
    # 2. Configuración del Cupón (Blindaje: 1 uso, 1 cliente)
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
    
    # 3. Envío Asíncrono
    async with aiohttp.ClientSession() as s:
        try:
            params = {"login": store.value, "authtoken": token.value}
            async with s.post(url, params=params, json=payload) as r:
                if r.status == 201:
                    return await r.json()
                else:
                    err_text = await r.text()
                    print(f"Jumpseller Error ({r.status}): {err_text}")
                    return None
        except Exception as e:
            print(f"Excepción de conexión Jumpseller: {str(e)}")
            return None