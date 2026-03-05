import aiohttp
import asyncio
import logging
from sqlalchemy.orm import Session
from .config import settings
from .models import Gampoint

logger = logging.getLogger(__name__)


async def sync_users_to_db(db: Session):
    url    = f"{settings.JUMPSELLER_API_BASE}/customers.json"
    params = {
        "login":     settings.JS_LOGIN_CODE,
        "authtoken": settings.JS_AUTH_TOKEN,
        "limit":     50,
        "page":      1,
    }
    added, updated = 0, 0

    async with aiohttp.ClientSession() as session:
        while True:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    break
                customers = await resp.json()
                if not customers:
                    break

                for c in customers:
                    cust  = c.get("customer", {})
                    email = cust.get("email", "").lower().strip()
                    if not email:
                        continue

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

                if len(customers) < 50:
                    break
                params["page"] += 1
                await asyncio.sleep(0.2)

    db.commit()
    return {"added": added, "updated": updated}
