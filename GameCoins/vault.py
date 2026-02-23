import uuid
import json
import aiohttp
import logging
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

        val_int   = int(amount)
        val_float = float(amount)

        logger.info(f"[CANJE] email={email} | amount recibido={amount} | tipo={type(amount)} | val_int={val_int} | val_float={val_float}")

        payload = {
            "promotion": {
                "name":             f"Canje QuestPoints - {email}",
                "code":             code,
                "enabled":          True,
                "type":             "fixed",
                "discount_target":  "order",
                "discount":         val_float, 
                "usage_limit":      1,
                "cumulative":       False
            }
        }

        logger.info(f"[CANJE] Payload → {json.dumps(payload)}")

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, params=params, json=payload) as resp:
                    response_text = await resp.text()
                    logger.info(f"[CANJE] Jumpseller [{resp.status}] → {response_text}")

                    if resp.status in [200, 201]:
                        data = json.loads(response_text)
                        return data.get("promotion", {}).get("code")
                    else:
                        logger.error(f"[CANJE] Rechazado ({resp.status}): {response_text}")
                        return None
            except Exception as e:
                logger.error(f"[CANJE] Error de conexión: {e}")
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
        user = db.query(Gampoint).filter(
            Gampoint.email == email.lower()
        ).with_for_update().first()

        if not user:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")

        logger.info(f"[CANJE] Usuario {email} — saldo en DB: {user.saldo} | solicitado: {amount}")

        if user.saldo < amount:
            raise HTTPException(
                status_code=400,
                detail=f"Saldo insuficiente. Tienes ${float(user.saldo):,.0f} QP, intentas canjear ${amount:,} QP"
            )

        coupon_code = await VaultController.create_js_coupon(email, amount)
        if not coupon_code:
            raise HTTPException(
                status_code=502,
                detail="Error creando cupón en Jumpseller. Revisa los logs."
            )

        user.saldo -= Decimal(amount)
        user.historico_canjeado += Decimal(amount)
        db.commit()

        return {"status": "ok", "cupon_codigo": coupon_code}