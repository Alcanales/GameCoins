import aiohttp
import asyncio
import logging
from sqlalchemy.orm import Session
from .config import settings
from .models import Gampoint

logger = logging.getLogger(__name__)

# Tamaño del batch de páginas que se fetchen en paralelo.
# Jumpseller API tiene rate-limit generoso pero no documentado.
# 5 páginas simultáneas × 50 customers = 250 customers/batch.
# Valor conservador que evita triggear rate-limiting.
_SYNC_BATCH_SIZE = 5


async def _fetch_customers_page(session: aiohttp.ClientSession, page: int) -> list[dict]:
    """
    Fetcha una página de customers de Jumpseller y retorna la lista raw.
    Retorna lista vacía en caso de error o respuesta vacía.
    """
    url = f"{settings.JUMPSELLER_API_BASE}/customers.json"
    params = {
        "login":     settings.JS_LOGIN_CODE,
        "authtoken": settings.JS_AUTH_TOKEN,
        "limit":     50,
        "page":      page,
    }
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                logger.warning(f"[SYNC] Página {page}: status {resp.status}")
                return []
            data = await resp.json()
            return data if isinstance(data, list) else []
    except Exception as e:
        logger.error(f"[SYNC] Error fetching página {page}: {e}")
        return []


async def sync_users_to_db(db: Session) -> dict:
    """
    Sincroniza customers de Jumpseller a la tabla gampoints.

    FIX M-06 ext (services.py): paginación paralela con asyncio.gather en batches
    de _SYNC_BATCH_SIZE páginas simultáneas.

    Antes: fetch secuencial con await por página + sleep(0.2) entre páginas.
    Para 1.000 customers (20 páginas × 50): ~20 × (RTT + 0.2s) ≈ 20–40 segundos.

    Ahora: 5 páginas en paralelo → 4 batches para 20 páginas ≈ 5–8 segundos.
    El sleep entre batches (0.3s) da un pequeño respiro a la API de Jumpseller.

    Thread-safety: db.query/add/commit son síncronos y se ejecutan en el event loop
    principal (1 worker uvicorn). No hay concurrencia real en la BD — seguro.
    """
    added, updated, errors = 0, 0, 0
    page = 1

    async with aiohttp.ClientSession() as session:
        while True:
            # Fetch un batch de páginas en paralelo
            batch_pages = list(range(page, page + _SYNC_BATCH_SIZE))
            results = await asyncio.gather(
                *[_fetch_customers_page(session, p) for p in batch_pages],
                return_exceptions=False,
            )

            total_in_batch = 0
            for customers in results:
                if not customers:
                    continue
                total_in_batch += len(customers)

                for c in customers:
                    cust  = c.get("customer", {})
                    email = cust.get("email", "").lower().strip()
                    if not email:
                        continue

                    try:
                        user = db.query(Gampoint).filter(Gampoint.email == email).first()
                        if not user:
                            db.add(Gampoint(
                                email         = email,
                                name          = cust.get("name"),
                                surname       = cust.get("surname"),
                                jumpseller_id = cust.get("id"),
                            ))
                            added += 1
                        else:
                            user.name          = cust.get("name")
                            user.surname       = cust.get("surname")
                            user.jumpseller_id = cust.get("id")
                            updated += 1
                    except Exception as e:
                        logger.error(f"[SYNC] Error procesando customer {email}: {e}")
                        errors += 1

            # Commit del batch completo
            try:
                db.commit()
            except Exception as e:
                logger.error(f"[SYNC] Error en commit batch página {page}-{page+_SYNC_BATCH_SIZE-1}: {e}")
                db.rollback()

            # Si todos los resultados del batch vinieron vacíos o cortos → terminamos
            all_empty = all(len(r) == 0 for r in results)
            any_short = any(0 < len(r) < 50 for r in results)

            if all_empty or any_short:
                break

            page += _SYNC_BATCH_SIZE
            await asyncio.sleep(0.3)   # respiro entre batches (cortesía hacia la API)

    logger.info(f"[SYNC] Completado: +{added} nuevos, ~{updated} actualizados, {errors} errores")
    return {"added": added, "updated": updated, "errors": errors}
