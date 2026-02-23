import uuid
import aiohttp
import logging
import datetime
import re
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
        now = datetime.datetime.now()
        today = now.strftime('%Y-%m-%d')
        expires_at = today
        payload = {
            "promotion": {
                "name":                f"Canje QuestPoints - {email}",
                "code":                code,
                "enabled":             True,
                "discount_target":     "order",
                "type":                "fix",
                "discount_amount_fix": val,
                "cumulative":          False,

                "begins_at":           today,
                "expires_at":          expires_at,  

                "lasts":               "max_times_used",  
                "max_times_used":      1,                
            }
        }

        logger.info(f"[JS_COUPON] Creando cupón 1-uso para {email}: {code} (expira: {expires_at})")

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, params=params, json=payload) as resp:
                    if resp.status in [200, 201]:
                        data = await resp.json()
                        created_code = data.get("promotion", {}).get("code")
                        logger.info(f"[JS_COUPON] ✅ Cupón creado: {created_code}")
                        return created_code
                    else:
                        err = await resp.text()
                        logger.error(f"[JS_COUPON] ❌ Error JS [{resp.status}]: {err}")
                        return None
            except Exception as e:
                logger.error(f"[JS_COUPON] ❌ Error de conexión: {e}")
                return None

    @staticmethod
    async def burn_coupon(code_to_burn: str):
        """
        Busca el cupón en Jumpseller y lo elimina activamente.
        Esta es la capa de seguridad que implementa el límite de ~2 horas:
        al dispararse el webhook de orden, el cupón se destruye inmediatamente,
        imposibilitando cualquier reutilización sin importar el tiempo.
        """
        if not re.fullmatch(r"QP-[A-F0-9]{6}", code_to_burn):
            logger.warning(f"[SEGURIDAD] Intento de borrar cupón ajeno abortado: {code_to_burn}")
            return False

        url_get = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
        params_base = {"login": settings.JS_LOGIN_CODE, "authtoken": settings.JS_AUTH_TOKEN}

        async with aiohttp.ClientSession() as session:
            try:
                page = 1
                while True:
                    params_get = {**params_base, "limit": 100, "page": page}
                    async with session.get(url_get, params=params_get) as resp:
                        if resp.status != 200:
                            break
                        promotions = await resp.json()
                        if not promotions:
                            break
                        for p in promotions:
                            promo = p.get("promotion", {})
                            if promo.get("code") == code_to_burn:
                                promo_id = promo.get("id")
                                url_del = f"{settings.JUMPSELLER_API_BASE}/promotions/{promo_id}.json"
                                await session.delete(url_del, params=params_base)
                                logger.info(f"[WEBHOOK] 🔥 Cupón {code_to_burn} DESTRUIDO.")
                                return True
                        page += 1

                logger.warning(f"[WEBHOOK] ⚠️ Cupón {code_to_burn} no encontrado (ya fue borrado).")
                return False
            except Exception as e:
                logger.error(f"[WEBHOOK] ❌ Error quemando cupón: {e}")
                return False

    @staticmethod
    async def sweep_used_coupons():
        """
        Barredora de emergencia: revisa órdenes recientes y destruye
        cualquier cupón QP- que haya sido usado.
        """
        url_orders = f"{settings.JUMPSELLER_API_BASE}/orders.json"
        params = {"login": settings.JS_LOGIN_CODE, "authtoken": settings.JS_AUTH_TOKEN, "limit": 50}

        burned_codes = []
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url_orders, params=params) as resp:
                    if resp.status == 200:
                        orders = await resp.json()
                        for o in orders:
                            order = o.get("order", {})
                            coupons = order.get("coupons", [])
                            for c in coupons:
                                code = c.get("code", "")
                                if re.fullmatch(r"QP-[A-F0-9]{6}", code):
                                    success = await VaultController.burn_coupon(code)
                                    if success:
                                        burned_codes.append(code)
            except Exception as e:
                logger.error(f"Error en barredora de cupones: {e}")
        return list(set(burned_codes))

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

        if user.saldo < amount:
            raise HTTPException(status_code=400, detail=f"Saldo insuficiente. Tienes ${float(user.saldo):,.0f} QP.")

        coupon_code = await VaultController.create_js_coupon(email, amount)
        if not coupon_code:
            raise HTTPException(status_code=502, detail="Error creando cupón en Jumpseller.")

        user.saldo -= Decimal(amount)
        user.historico_canjeado += Decimal(amount)
        db.commit()

        return {"status": "ok", "cupon_codigo": coupon_code}