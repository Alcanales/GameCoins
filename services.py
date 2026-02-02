import aiohttp
from datetime import datetime
from sqlalchemy.orm import Session
from models import SystemConfig 

async def crear_cupon_jumpseller(codigo: str, monto: int, email: str, db: Session):
    token = db.query(SystemConfig).filter(SystemConfig.key == "JUMPSELLER_API_TOKEN").first()
    store = db.query(SystemConfig).filter(SystemConfig.key == "JUMPSELLER_STORE").first()
    
    if not token or not store: 
        print("ERROR: Credenciales de Jumpseller no encontradas en la DB.")
        return None
    
    url = "https://api.jumpseller.com/v1/promotions.json"
    
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
            params = {"login": store.value, "authtoken": token.value}
            async with s.post(url, params=params, json=payload) as r:
                if r.status == 201:
                    return await r.json()
                else:
                    print(f"Fallo Jumpseller ({r.status}): {await r.text()}")
                    return None
        except Exception as e:
            print(f"Error de conexión con Jumpseller: {str(e)}")
            return None