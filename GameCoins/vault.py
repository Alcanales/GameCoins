import uuid
import aiohttp
import logging
from datetime import datetime
from decimal import Decimal
from sqlalchemy.orm import Session
from fastapi import HTTPException

from .models import Gampoint
from .config import settings

logger = logging.getLogger(__name__)

class VaultController:
    @staticmethod
    async def create_js_coupon(email: str, amount: int):
        code = f"QP-{uuid.uuid4().hex[:6].upper()}"
        url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
        params = {"login": settings.JS_LOGIN_CODE, "authtoken": settings.JS_AUTH_TOKEN}
        
        # Jumpseller es estricto: mandamos el monto en todos los campos posibles
        monto_valor = int(amount)
        payload = {
            "promotion": {
                "name": f"Canje QuestPoints: {email}",
                "code": code,
                "enabled": True,
                "type": "fixed",
                "discount_target": "order",
                "amount": monto_valor,
                "discount": monto_valor,
                "value": monto_valor,
                "discount_amount": monto_valor,
                "usage_limit": 1
            }
        }
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, params=params, json=payload) as resp:
                    if resp.status in [200, 201]:
                        data = await resp.json()
                        return data.get("promotion", {}).get("code")
                    else:
                        # Si Jumpseller falla, imprimimos el error exacto en Render
                        err = await resp.text()
                        logger.error(f"Jumpseller rechazó el cupón. Status {resp.status}: {err}")
                        return None
            except Exception as e:
                logger.error(f"Error de conexión con Jumpseller: {e}")
                return None

    @staticmethod
    def sync_user(db: Session, customer_data: dict):
        email = customer_data.get('email', '').lower().strip()
        user = db.query(Gampoint).filter(Gampoint.email == email).first()
        if not user:
            user = Gampoint(
                email=email,
                jumpseller_id=customer_data.get('id'),
                name=customer_data.get('name'),
                surname=customer_data.get('surname')
            )
            db.add(user)
        else:
            user.jumpseller_id = customer_data.get('id')
            user.name = customer_data.get('name')
            user.surname = customer_data.get('surname')
        db.commit()
        return user

    @staticmethod
    async def process_canje(db: Session, email: str, amount: int):
        user = db.query(Gampoint).filter(Gampoint.email == email.lower()).with_for_update().first()
        
        if not user or user.saldo < amount:
            raise HTTPException(status_code=400, detail="Saldo insuficiente")

        coupon_code = await VaultController.create_js_coupon(email, amount)
        if not coupon_code:
            raise HTTPException(status_code=500, detail="Error creando cupón en Jumpseller")

        user.saldo -= Decimal(amount)
        user.historico_canjeado += Decimal(amount)
        
        db.commit()
        return {"status": "ok", "cupon_codigo": coupon_code}