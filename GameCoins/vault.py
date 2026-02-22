import uuid
import aiohttp
import logging
from datetime import datetime
from decimal import Decimal
from sqlalchemy.orm import Session
from fastapi import HTTPException
from .models import Gampoint

logger = logging.getLogger(__name__)

class VaultController:
    @staticmethod
    async def create_js_coupon(email: str, amount: int):
        """Genera un cupón real en Jumpseller."""
        code = f"GQ-{uuid.uuid4().hex[:6].upper()}"
        url = "https://api.jumpseller.com/v1/promotions.json"
        params = {
            "login": "032aa60af252c7f3eb99c65191799bdb", # JS_LOGIN_CODE
            "authtoken": "c168d3283e923c35215b3467357fe5d6" # JS_AUTH_TOKEN
        }
        payload = {
            "promotion": {
                "name": f"Canje GameCoins: {email}",
                "code": code,
                "enabled": True,
                "type": "fixed",
                "discount_target": "order",
                "amount": amount,
                "usage_limit": 1
            }
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, params=params, json=payload) as resp:
                if resp.status in [200, 201]:
                    data = await resp.json()
                    return data.get("promotion", {}).get("code")
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

        user.saldo -= amount
        user.historico_canjeado += amount
        db.commit()
        return {"status": "ok", "cupon_codigo": coupon_code}