import uuid
import json
import aiohttp
import logging
import datetime
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

        val = int(amount)
        
        # --- 1 DÍA DE VIGENCIA MÁXIMA ---
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        expires = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        payload = {
            "promotion": {
                "name":                f"Canje QuestPoints - {email}",
                "code":                code,
                "enabled":             True,
                "discount_target":     "order",
                "type":                "fix",
                "discount_amount_fix": val,
                "begins_at":           today,
                "expires_at":          expires,
                "cumulative":          False
            }
        }

        logger.info(f"[JS_COUPON] email={email} | monto={val} | payload={json.dumps(payload)}")

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, params=params, json=payload) as resp:
                    response_text = await resp.text()
                    logger.info(f"[JS_COUPON] Jumpseller [{resp.status}] → {response_text}")

                    if resp.status in [200, 201]:
                        data = json.loads(response_text)
                        created_code = data.get("promotion", {}).get("code")
                        logger.info(f"[JS_COUPON] ✅ Cupón creado: {created_code}")
                        return created_code
                    else:
                        logger.error(f"[JS_COUPON] ❌ Rechazado [{resp.status}]: {response_text}")
                        return None
            except Exception as e:
                logger.error(f"[JS_COUPON] ❌ Error de conexión: {e}")
                return None

    @staticmethod
    async def disable_js_coupon(code_to_disable: str):
        url_get = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
        params = {"login": settings.JS_LOGIN_CODE, "authtoken": settings.JS_AUTH_TOKEN}
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url_get, params=params) as resp:
                    if resp.status == 200:
                        promotions = await resp.json()
                        promo_id = None
                        for p in promotions:
                            promo = p.get("promotion", {})
                            if promo.get("code") == code_to_disable:
                                promo_id = promo.get("id")
                                break
                        
                        if promo_id:
                            url_put = f"{settings.JUMPSELLER_API_BASE}/promotions/{promo_id}.json"
                            payload = {"promotion": {"enabled": False}}
                            await session.put(url_put, params=params, json=payload)
                            logger.info(f"[WEBHOOK] ✅ Cupón {code_to_disable} apagado y bloqueado exitosamente.")
                        else:
                            logger.warning(f"[WEBHOOK] ⚠️ Cupón {code_to_disable} no encontrado para apagar.")
            except Exception as e:
                logger.error(f"[WEBHOOK] ❌ Error apagando cupón: {e}")

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

        logger.info(f"[CANJE] Usuario={email} | saldo_db={user.saldo} | monto={amount}")

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

        logger.info(f"[CANJE] ✅ OK | cupon={coupon_code} | nuevo_saldo={float(user.saldo)}")
        return {"status": "ok", "cupon_codigo": coupon_code}