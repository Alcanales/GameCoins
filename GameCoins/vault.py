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

# ─────────────────────────────────────────────────────────────────────────────
# ID raíz de la categoría "Singles" en game-quest.jumpseller.com
# Verificado en: /admin/cl/products/categories#/category/2491665
# ─────────────────────────────────────────────────────────────────────────────
SINGLES_ROOT_ID = 2491665

# Cache en memoria: { "ids": [...], "cached_at": datetime }
# Se refresca automáticamente cada CACHE_TTL_MINUTES minutos.
# Así no se llama a la API de categorías en cada canje, pero sí
# se actualiza si se agregan nuevas subcategorías de Singles.
_singles_cache: dict = {"ids": None, "cached_at": None}
CACHE_TTL_MINUTES = 60


class VaultController:

    # ─────────────────────────────────────────────────────────────────────────
    # HELPER: árbol de IDs de Singles con cache TTL
    # ─────────────────────────────────────────────────────────────────────────
    @staticmethod
    async def _get_singles_category_ids(session: aiohttp.ClientSession) -> list[int]:
        """
        Devuelve SINGLES_ROOT_ID + todos los IDs de sus subcategorías (BFS).

        • Cache en memoria con TTL de 60 min → una sola llamada a la API
          por hora aunque se generen muchos cupones.
        • Si la llamada a la API falla, usa el cache anterior (si existe)
          o cae de vuelta al ID raíz solo (fail-safe).
        • Si se agrega una subcategoría nueva en Jumpseller, el cache se
          invalida automáticamente al expirar el TTL.
        """
        global _singles_cache

        now = datetime.datetime.utcnow()
        cached_at = _singles_cache.get("cached_at")
        cached_ids = _singles_cache.get("ids")

        # Devolver cache si está vigente
        if cached_ids and cached_at:
            age_minutes = (now - cached_at).total_seconds() / 60
            if age_minutes < CACHE_TTL_MINUTES:
                logger.debug(
                    f"[SINGLES_IDS] Cache vigente ({age_minutes:.1f} min). "
                    f"IDs: {cached_ids}"
                )
                return cached_ids

        # Intentar refrescar desde la API
        params = {
            "login":     settings.JS_LOGIN_CODE,
            "authtoken": settings.JS_AUTH_TOKEN,
            "limit":     200,
        }
        url = f"{settings.JUMPSELLER_API_BASE}/categories.json"

        all_ids: set[int] = {SINGLES_ROOT_ID}

        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")

                categories = await resp.json()

                # Construir mapa parent_id → [child_ids]
                children: dict[int, list[int]] = {}
                for c in categories:
                    cat    = c.get("category", c)
                    cid    = cat.get("id")
                    parent = cat.get("parent_id")
                    if cid and parent:
                        children.setdefault(int(parent), []).append(int(cid))

                # BFS desde la raíz para cubrir toda la jerarquía
                queue = [SINGLES_ROOT_ID]
                while queue:
                    current = queue.pop()
                    for child_id in children.get(current, []):
                        if child_id not in all_ids:
                            all_ids.add(child_id)
                            queue.append(child_id)

            result = sorted(all_ids)
            _singles_cache = {"ids": result, "cached_at": now}
            logger.info(
                f"[SINGLES_IDS] 🔄 Cache refrescado. "
                f"{len(result)} IDs en árbol Singles: {result}"
            )
            return result

        except Exception as e:
            # Fail-safe: usar cache anterior si existe, sino solo la raíz
            if cached_ids:
                logger.warning(
                    f"[SINGLES_IDS] ⚠️ Error refrescando ({e}). "
                    f"Usando cache anterior: {cached_ids}"
                )
                return cached_ids
            else:
                logger.warning(
                    f"[SINGLES_IDS] ⚠️ Error y sin cache ({e}). "
                    f"Usando solo ID raíz: [{SINGLES_ROOT_ID}]"
                )
                return [SINGLES_ROOT_ID]

    # ─────────────────────────────────────────────────────────────────────────
    # CREAR CUPÓN
    # ─────────────────────────────────────────────────────────────────────────
    @staticmethod
    async def create_js_coupon(email: str, amount: int):
        """
        Crea un cupón QP- de uso único restringido a Singles (raíz + subcategorías).

        ══════════════════════════════════════════════════════════════════════
        TODOS LOS MECANISMOS DE LÍMITE 1/1 APLICADOS SIMULTÁNEAMENTE
        ══════════════════════════════════════════════════════════════════════

        NIVEL API (lo que enviamos a Jumpseller):
        ┌───┬──────────────────────────────────────────────────────────────┐
        │ A │ lasts="max_times_used" + max_times_used=1                   │
        │   │   → Jumpseller rechaza cualquier 2do intento en checkout    │
        │   │   → Campo VERIFICADO en OpenAPI oficial                     │
        ├───┼──────────────────────────────────────────────────────────────┤
        │ B │ once_per_customer=True                                      │
        │   │   → Activa "Límite por cliente" en la UI de Jumpseller      │
        │   │   → Puede ser ignorado silenciosamente por la API           │
        ├───┼──────────────────────────────────────────────────────────────┤
        │ C │ customer_restriction="logged_in"                            │
        │   │   → Solo clientes con cuenta logueada pueden usar el cupón  │
        │   │   → Documentado en Jumpseller Support (promotions page)     │
        │   │   → Bloquea uso anónimo/guest completamente                 │
        ├───┼──────────────────────────────────────────────────────────────┤
        │ D │ cumulative=False                                            │
        │   │   → No acumulable con otras promociones                    │
        ├───┼──────────────────────────────────────────────────────────────┤
        │ E │ expires_at=today                                            │
        │   │   → Cupón inválido al finalizar el día (ventana 0-24 hrs)  │
        └───┴──────────────────────────────────────────────────────────────┘

        NIVEL APLICACIÓN (nuestro código):
        ┌───┬──────────────────────────────────────────────────────────────┐
        │ F │ burn_coupon() vía webhook                                   │
        │   │   → Destrucción física de la promotion segundos después     │
        │   │   │  del primer uso real (order_created / order_paid)       │
        │   │   → Esta es la capa MÁS ROBUSTA                            │
        ├───┼──────────────────────────────────────────────────────────────┤
        │ G │ sweep_used_coupons() periódico                              │
        │   │   → Red de seguridad: barre órdenes recientes y destruye   │
        │   │   │  cualquier QP- que haya pasado el webhook               │
        └───┴──────────────────────────────────────────────────────────────┘

        RESTRICCIÓN DE CATEGORÍA:
        ┌───┬──────────────────────────────────────────────────────────────┐
        │ H │ discount_target="categories" + categories_ids=[árbol]       │
        │   │   → Solo aplica en Singles + TODAS sus subcategorías        │
        │   │   → IDs calculados dinámicamente con cache TTL 60 min      │
        │   │   → Si se agrega una subcategoría nueva, se detecta sola   │
        └───┴──────────────────────────────────────────────────────────────┘
        ══════════════════════════════════════════════════════════════════════
        """
        code   = f"QP-{uuid.uuid4().hex[:6].upper()}"
        url    = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
        params = {"login": settings.JS_LOGIN_CODE, "authtoken": settings.JS_AUTH_TOKEN}

        val   = int(amount)
        today = datetime.datetime.now().strftime('%Y-%m-%d')

        promo_name = f"Canje QuestPoints - {email}"

        async with aiohttp.ClientSession() as session:

            # Obtener árbol completo de IDs de Singles (cache TTL 60 min)
            singles_ids = await VaultController._get_singles_category_ids(session)

            payload = {
                "promotion": {
                    "name":    promo_name,
                    "code":    code,
                    "enabled": True,

                    # ── H: Restricción por categoría (Singles + subcategorías) ──
                    "discount_target": "categories",
                    "categories_ids":  singles_ids,

                    "type":                "fix",
                    "discount_amount_fix": val,
                    "cumulative":          False,   # D
                    "begins_at":           today,
                    "expires_at":          today,   # E

                    # ── A: 1 uso global ────────────────────────────────────────
                    "lasts":          "max_times_used",
                    "max_times_used": 1,

                    # ── B: 1 uso por cliente ───────────────────────────────────
                    "once_per_customer": True,

                    # ── C: solo clientes logueados ─────────────────────────────
                    "customer_restriction": "logged_in",
                }
            }

            logger.info(
                f"[JS_COUPON] 🎟️  Creando cupón | "
                f"Cliente: {email} | "
                f"Monto: ${val:,} CLP | "
                f"Código: {code} | "
                f"Vence: {today} | "
                f"Singles ({len(singles_ids)} IDs): {singles_ids}"
            )

            try:
                async with session.post(url, params=params, json=payload) as resp:
                    if resp.status in [200, 201]:
                        data         = await resp.json()
                        created_code = data.get("promotion", {}).get("code")
                        promo_id     = data.get("promotion", {}).get("id")
                        logger.info(
                            f"[JS_COUPON] ✅ CUPÓN CREADO | "
                            f"Código: {created_code} | "
                            f"Cliente: {email} | "
                            f"Monto canjeado: ${val:,} CLP | "
                            f"Promotion ID: {promo_id} | "
                            f"Vence: {today} | "
                            f"Categorías: {len(singles_ids)} IDs Singles"
                        )
                        return created_code
                    else:
                        err = await resp.text()
                        logger.error(
                            f"[JS_COUPON] ❌ Error JS [{resp.status}] "
                            f"al crear cupón para {email}: {err}"
                        )
                        return None
            except Exception as e:
                logger.error(f"[JS_COUPON] ❌ Error de conexión: {e}")
                return None




    @staticmethod
    async def burn_coupon(code_to_burn: str) -> bool:
        """
        Destruye la PROMOTION completa de Jumpseller tras su primer uso.
        Llamar desde webhook en: order_created, order_paid, order_pending_payment.
        """
        if not re.fullmatch(r"QP-[A-F0-9]{6}", code_to_burn):
            logger.warning(
                f"[SEGURIDAD] Intento de borrar cupón ajeno abortado: {code_to_burn}"
            )
            return False

        url_get     = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
        params_base = {"login": settings.JS_LOGIN_CODE, "authtoken": settings.JS_AUTH_TOKEN}

        async with aiohttp.ClientSession() as session:
            try:
                page = 1
                while True:
                    params_get = {**params_base, "limit": 100, "page": page}
                    async with session.get(url_get, params=params_get) as resp:
                        if resp.status != 200:
                            logger.error(f"[BURN] Error listando promotions: {resp.status}")
                            break
                        promotions = await resp.json()
                        if not promotions:
                            break

                        for p in promotions:
                            promo      = p.get("promotion", {})
                            promo_name = promo.get("name", "")
                            promo_id   = promo.get("id")

                            if not promo_name.startswith("Canje QuestPoints -"):
                                continue


                            coupons_in_promo = promo.get("coupons", []) or []
                            coupon_match = any(
                                c.get("coupon", {}).get("name") == code_to_burn
                                or c.get("name") == code_to_burn
                                for c in coupons_in_promo
                                if isinstance(c, dict)
                            )
                            direct_match = promo.get("code") == code_to_burn

                            if coupon_match or direct_match:
                                url_del = (
                                    f"{settings.JUMPSELLER_API_BASE}"
                                    f"/promotions/{promo_id}.json"
                                )
                                async with session.delete(
                                    url_del, params=params_base
                                ) as del_resp:
                                    if del_resp.status in [200, 204]:
                                        logger.info(
                                            f"[BURN] 🔥 Promotion '{promo_name}' "
                                            f"(id={promo_id}) DESTRUIDA. "
                                            f"Cupón: {code_to_burn}"
                                        )
                                        return True
                                    else:
                                        err = await del_resp.text()
                                        logger.error(
                                            f"[BURN] Error eliminando promotion "
                                            f"{promo_id}: {del_resp.status} - {err}"
                                        )
                                        return False

                        page += 1

                logger.warning(
                    f"[BURN] ⚠️ Promotion con cupón {code_to_burn} no encontrada "
                    f"(ya fue eliminada o no existe)."
                )
                return False

            except Exception as e:
                logger.error(f"[BURN] ❌ Error quemando cupón {code_to_burn}: {e}")
                return False

    @staticmethod
    async def burn_coupon_by_order(order_data: dict) -> list[str]:
        """
        Extrae todos los cupones QP- de una orden y los destruye.
        Llamar desde el webhook handler con el payload de la orden.
        """
        order   = order_data.get("order", order_data)
        coupons = order.get("coupons") or []
        burned  = []

        for c in coupons:
            code = c.get("code", "") or c.get("name", "") if isinstance(c, dict) else str(c)
            if re.fullmatch(r"QP-[A-F0-9]{6}", code):
                if await VaultController.burn_coupon(code):
                    burned.append(code)

        return burned


    @staticmethod
    async def sweep_used_coupons() -> list[str]:
        """
        Revisa órdenes recientes y destruye cualquier cupón QP- aplicado.
        Ejecutar periódicamente (ej: cada 5 min via scheduler) como red de seguridad.
        """
        url_orders = f"{settings.JUMPSELLER_API_BASE}/orders.json"
        params = {
            "login":     settings.JS_LOGIN_CODE,
            "authtoken": settings.JS_AUTH_TOKEN,
            "limit":     50,
        }
        burned_codes: list[str] = []
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url_orders, params=params) as resp:
                    if resp.status == 200:
                        orders = await resp.json()
                        for o in orders:
                            order   = o.get("order", {})
                            coupons = order.get("coupons") or []
                            for c in coupons:
                                code = (
                                    c.get("code", "") or c.get("name", "")
                                    if isinstance(c, dict) else str(c)
                                )
                                if re.fullmatch(r"QP-[A-F0-9]{6}", code):
                                    if await VaultController.burn_coupon(code):
                                        burned_codes.append(code)
            except Exception as e:
                logger.error(f"[SWEEP] Error en barredora de cupones: {e}")
        return list(set(burned_codes))



    @staticmethod
    def sync_user(db: Session, customer_data: dict):
        email = customer_data.get('email', '').lower().strip()
        user  = db.query(Gampoint).filter(Gampoint.email == email).first()
        if not user:
            user = Gampoint(
                email=email,
                jumpseller_id=customer_data.get('id'),
                name=customer_data.get('name'),
                surname=customer_data.get('surname'),
            )
            db.add(user)
        else:
            user.jumpseller_id = customer_data.get('id')
            user.name          = customer_data.get('name')
            user.surname       = customer_data.get('surname')
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
                detail=f"Saldo insuficiente. Tienes ${float(user.saldo):,.0f} QP.",
            )

        coupon_code = await VaultController.create_js_coupon(email, amount)
        if not coupon_code:
            raise HTTPException(
                status_code=502, detail="Error creando cupón en Jumpseller."
            )

        user.saldo              -= Decimal(amount)
        user.historico_canjeado += Decimal(amount)
        db.commit()

        return {"status": "ok", "cupon_codigo": coupon_code}
