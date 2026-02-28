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

        promo_name = f"Canje QuestPoints - {email}"

        payload = {
            "promotion": {
                "name":                promo_name,
                "code":                code,
                "enabled":             True,
                "discount_target":     "order",
                "type":                "fix",
                "discount_amount_fix": val,
                "cumulative":          False,
                "begins_at":           today,
                "expires_at":          expires_at,

                # ✅ CAMPO CONFIRMADO: límite total de uso = 1 (UI: "Límite total de consumo")
                "lasts":               "max_times_used",
                "max_times_used":      1,
            }
        }

        logger.info(f"[JS_COUPON] Creando cupón para {email}: {code} (expira: {expires_at})")

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, params=params, json=payload) as resp:
                    if resp.status in [200, 201]:
                        data = await resp.json()
                        created_code = data.get("promotion", {}).get("code")
                        logger.info(f"[JS_COUPON] ✅ Cupón 1/1 creado: {created_code}")
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
                            logger.error(f"[WEBHOOK] Error al listar promotions: {resp.status}")
                            break
                        promotions = await resp.json()
                        if not promotions:
                            break

                        for p in promotions:
                            promo = p.get("promotion", {})
                            promo_name = promo.get("name", "")
                            promo_id = promo.get("id")

                            if not promo_name.startswith("Canje QuestPoints -"):
                                continue

                            coupons_in_promo = promo.get("coupons", []) or []
                            coupon_match = any(
                                c.get("coupon", {}).get("name") == code_to_burn or
                                c.get("name") == code_to_burn
                                for c in coupons_in_promo
                                if isinstance(c, dict)
                            )

                            direct_match = promo.get("code") == code_to_burn

                            if coupon_match or direct_match:
                                url_del = f"{settings.JUMPSELLER_API_BASE}/promotions/{promo_id}.json"
                                async with session.delete(url_del, params=params_base) as del_resp:
                                    if del_resp.status in [200, 204]:
                                        logger.info(
                                            f"[WEBHOOK] 🔥 Promotion '{promo_name}' "
                                            f"(id={promo_id}) DESTRUIDA. Cupón: {code_to_burn}"
                                        )
                                        return True
                                    else:
                                        err = await del_resp.text()
                                        logger.error(
                                            f"[WEBHOOK] Error eliminando promotion "
                                            f"{promo_id}: {del_resp.status} - {err}"
                                        )
                                        return False

                        page += 1

                logger.warning(
                    f"[WEBHOOK] ⚠️ Promotion con cupón {code_to_burn} no encontrada "
                    f"(ya fue eliminada o no existe)."
                )
                return False

            except Exception as e:
                logger.error(f"[WEBHOOK] ❌ Error quemando cupón {code_to_burn}: {e}")
                return False

    @staticmethod
    async def burn_coupon_by_order(order_data: dict) -> list[str]:
        order = order_data.get("order", order_data)  
        coupons = order.get("coupons") or []
        burned = []

        for c in coupons:
            if isinstance(c, dict):
                code = c.get("code", "") or c.get("name", "")
            else:
                code = str(c)

            if re.fullmatch(r"QP-[A-F0-9]{6}", code):
                success = await VaultController.burn_coupon(code)
                if success:
                    burned.append(code)

        return burned

    @staticmethod
    async def sweep_used_coupons():
        """
        Barredora de emergencia: revisa órdenes recientes y destruye
        cualquier cupón QP- que haya sido aplicado.
        
        Ejecutar periódicamente (ej: cada 5 minutos via cron/scheduler)
        como red de seguridad adicional.
        """
        url_orders = f"{settings.JUMPSELLER_API_BASE}/orders.json"
        params = {
            "login": settings.JS_LOGIN_CODE,
            "authtoken": settings.JS_AUTH_TOKEN,
            "limit": 50
        }

        burned_codes = []
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url_orders, params=params) as resp:
                    if resp.status == 200:
                        orders = await resp.json()
                        for o in orders:
                            order = o.get("order", {})
                            coupons = order.get("coupons") or []
                            for c in coupons:
                                if isinstance(c, dict):
                                    code = c.get("code", "") or c.get("name", "")
                                else:
                                    code = str(c)
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
            raise HTTPException(
                status_code=400,
                detail=f"Saldo insuficiente. Tienes ${float(user.saldo):,.0f} QP."
            )

        coupon_code = await VaultController.create_js_coupon(email, amount)
        if not coupon_code:
            raise HTTPException(status_code=502, detail="Error creando cupón en Jumpseller.")

        user.saldo -= Decimal(amount)
        user.historico_canjeado += Decimal(amount)
        db.commit()

        return {"status": "ok", "cupon_codigo": coupon_code}
