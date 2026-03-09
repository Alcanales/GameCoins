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
        """
        Crea un código de descuento en Jumpseller que se comporta como una Gift Card:

        MODELO GIFT CARD (sin fecha de expiración):
        ─────────────────────────────────────────────
        • Sin `expires_at` → el código NUNCA expira por fecha.
        • `lasts: max_times_used` + `max_times_used: 1` → se invalida
          automáticamente la primera vez que se aplica a una orden.
        • `cumulative: False` → no se puede combinar con otras promociones.
        • El cliente puede usar el código en cualquier momento futuro,
          exactamente igual al comportamiento de una gift card real.

        Por qué NO usar el sistema nativo de Gift Cards de Jumpseller:
        ─────────────────────────────────────────────────────────────
        La API pública de Jumpseller v1 no expone POST /gift_cards.json.
        Los gift card products tienen precio fijo (no dinámico por canje).
        El código generado se envía directamente por email y nunca retorna
        en el response de la orden — imposible almacenarlo en BD.
        La API Promotions es el único mecanismo programático disponible
        y cumple 100% del comportamiento requerido.
        """
        code = f"QP-{uuid.uuid4().hex[:6].upper()}"
        url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
        params = {"login": settings.JS_LOGIN_CODE, "authtoken": settings.JS_AUTH_TOKEN}

        val = int(amount)
        # begins_at = hoy: el código es válido desde ahora mismo.
        # expires_at = OMITIDO intencionalmente: sin fecha límite (modelo gift card).
        today = datetime.datetime.now().strftime('%Y-%m-%d')

        promo_name = f"Canje QuestPoints - {email}"

        payload = {
            "promotion": {
                "name":                promo_name,
                "code":                code,
                "enabled":             True,
                "discount_target":     "order",
                "type":                "fix",
                "discount_amount_fix": val,
                "cumulative":          True,
                "begins_at":           today,
                # expires_at: NO incluido → sin expiración por fecha.
                # El único trigger de invalidación es max_times_used = 1
                # (se quema al aplicarse a una orden, igual que una gift card).
                # cumulative: True → el código QP se apila con otras promociones
                # activas en la tienda. Orden de aplicación Jumpseller:
                #   1. Descuentos de producto → 2. Subtotal (este cupón) → 3. Envío
                # El cliente puede tener un 20% OFF en la tienda Y aplicar su QP.
                "lasts":               "max_times_used",
                "max_times_used":      1,
            }
        }

        logger.info(
            f"[JS_COUPON] Creando gift card QP para {email}: {code} "
            f"(valor: {val} CLP, sin expiración por fecha — expira al usarse)"
        )

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
    async def burn_coupon(code_to_burn: str, max_pages: int = 10):
        """
        Quema un cupón QP buscándolo en las promotions de Jumpseller.

        max_pages=10 evita O(n) sin límite sobre stores con miles de
        promotions. Con limit=100 y max_pages=10 se cubren 1.000 promotions —
        más que suficiente dado que las promotions QP se eliminan al usarse.
        Si el cupón no se encuentra en max_pages páginas, se asume que ya fue
        eliminado previamente (idempotencia del webhook).
        """
        if not re.fullmatch(r"QP-[A-F0-9]{6}", code_to_burn):
            logger.warning(f"[SEGURIDAD] Intento de borrar cupón ajeno abortado: {code_to_burn}")
            return False

        url_get = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
        params_base = {"login": settings.JS_LOGIN_CODE, "authtoken": settings.JS_AUTH_TOKEN}

        async with aiohttp.ClientSession() as session:
            try:
                page = 1
                while page <= max_pages:
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

                if page > max_pages:
                    logger.warning(
                        f"[WEBHOOK] ⚠️ Cupón {code_to_burn} no encontrado en "
                        f"las primeras {max_pages} páginas de promotions "
                        f"({max_pages * 100} promotions revisadas). "
                        f"Asumiendo que ya fue eliminado o no existe."
                    )
                else:
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
    async def process_canje(db: Session, email: str, amount: int, cart_total: int):
        """
        Canjea QuestPoints del usuario por un cupón de descuento en Jumpseller.
        (Versión Responsabilidad del Usuario: permite múltiples cupones simultáneos)
        """
        # ── Paso 1: Bloqueo de fila (Seguridad Concurrente) ──────────────────
        user = db.query(Gampoint).filter(
            Gampoint.email == email.lower()
        ).with_for_update().first()

        if not user:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")

        # ── Paso 2: Cap del monto al total del carrito ───────────────────────
        effective_amount = min(amount, cart_total)
        adjusted = (effective_amount < amount)

        if adjusted:
            logger.info(
                f"[QA02] Monto ajustado para {email}: solicitó {amount} QP, "
                f"carrito = {cart_total} CLP → cupón emitido por {effective_amount} QP."
            )

        # ── Paso 3: Verificar saldo ──────────────────────────────────────────
        if user.saldo < effective_amount:
            raise HTTPException(
                status_code=400,
                detail=f"Saldo insuficiente. Tienes ${float(user.saldo):,.0f} QP."
            )

        # ── Paso 4: Descontar saldo primero y commitear ──────────────────────
        amount_dec = Decimal(str(effective_amount))
        user.saldo -= amount_dec
        user.historico_canjeado += amount_dec
        
        try:
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(f"[CANJE] Error en BD al debitar saldo para {email}: {e}")
            raise HTTPException(status_code=500, detail="Error interno de base de datos.")

        # ── Paso 5: Crear cupón en Jumpseller ────────────────────────────────
        coupon_code = await VaultController.create_js_coupon(email, effective_amount)

        if not coupon_code:
            # ── Paso 6: Compensating transaction — revertir el débito ───────
            from .database import SessionLocal
            comp_db = SessionLocal()
            try:
                comp_user = comp_db.query(Gampoint).filter(
                    Gampoint.email == email.lower()
                ).with_for_update().first()
                if comp_user:
                    comp_user.saldo += amount_dec
                    comp_user.historico_canjeado -= amount_dec
                    comp_db.commit()
                    logger.warning(f"[CANJE] Cupón JS falló para {email} — saldo revertido")
                else:
                    logger.error(f"[CANJE] CRÍTICO: Usuario {email} no encontrado en compensación.")
            except Exception as comp_e:
                comp_db.rollback()
                logger.error(f"[CANJE] CRÍTICO: Compensación falló para {email}: {comp_e}")
            finally:
                comp_db.close()

            raise HTTPException(
                status_code=502,
                detail="Error creando cupón en Jumpseller. Tu saldo ha sido restaurado."
            )

        # ── Paso 7: Éxito (Fix Estricto Anti-Tuplas) ─────────────────────────
        # Al usar dict() evitamos cualquier bug de sintaxis por comas residuales
        response = dict()
        response["status"] = "ok"
        response["cupon_codigo"] = str(coupon_code)
        
        if adjusted:
            response["monto_ajustado"] = True
            response["monto_original"] = int(amount)
            response["monto_efectivo"] = int(effective_amount)
            response["motivo_ajuste"] = (
                f"Cupón emitido por {effective_amount} QP (= total de tu carrito). "
                f"Los {amount - effective_amount} QP restantes siguen en tu saldo."
            )
            
        return response