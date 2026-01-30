import pandas as pd
import aiohttp
import asyncio
import io
import smtplib
import re
import unicodedata
import logging
import json
import math
import time
from datetime import datetime
from typing import List, Dict, Any
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from config import settings

# Observabilidad: Logs con timestamp
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("GameQuestCore")

STOCK_CACHE: Dict[str, tuple] = {}
SCRYFALL_CACHE: Dict[str, tuple] = {}
CACHE_TTL = 300

# Normalización Regex (Compilada una vez para velocidad)
REGEX_BRACKETS = re.compile(r"[\(\[].*?[\)\]]")
REGEX_KEYWORDS = re.compile(r"(?i)\b(foil|retro|etched|borderless|extended art|showcase|surge|textured|serialized|schematic|thick|frame|art series|gold border|oversized|promo|prerelease)\b")
REGEX_CLEANUP = re.compile(r"[^a-z0-9\s]")

def clean_card_name(name: str) -> str:
    if not isinstance(name, str) or not name: return ""
    name = REGEX_BRACKETS.sub("", name)
    name = REGEX_KEYWORDS.sub("", name)
    name = unicodedata.normalize('NFD', name)
    name = "".join(c for c in name if unicodedata.category(c) != 'Mn')
    name = name.lower().replace("&", "and")
    name = REGEX_CLEANUP.sub(" ", name)
    return " ".join(name.split())

def round_clp(val: float) -> int:
    if pd.isna(val) or math.isinf(val): return 0
    return int(round(val / 100.0) * 100)

async def fetch_json_with_retry(session, url, method="GET", params=None, json_body=None, retries=3):
    for attempt in range(retries):
        try:
            async with session.request(method, url, params=params, json=json_body, timeout=15) as resp:
                if resp.status in [200, 201]: return await resp.json()
                if resp.status == 429:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                if resp.status >= 500:
                    await asyncio.sleep(1)
                    continue
                return None
        except Exception as e:
            logger.error(f"Error Red {url}: {e}")
            await asyncio.sleep(1)
    return None

async def crear_cupon_jumpseller(session, codigo: str, descuento: int, email: str):
    return await fetch_json_with_retry(session, f"{settings.JUMPSELLER_API_BASE}/promotions.json", method="POST", 
        params={"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN},
        json_body={"promotion": {"name": f"Canje GQ {codigo}", "code": codigo, "discount_amount": descuento, "status": "active", "usage_limit": 1, "minimum_order_amount": 0, "begins_at": datetime.now().strftime("%Y-%m-%d")}})

async def fetch_scryfall_metadata(ids: List[str]) -> Dict[str, Any]:
    # (Lógica optimizada de batching mantenida del análisis anterior)
    unique_ids = list(set(i for i in ids if isinstance(i, str) and i))
    result = {}
    missing = [uid for uid in unique_ids if uid not in SCRYFALL_CACHE or (datetime.now().timestamp() - SCRYFALL_CACHE[uid][1] >= CACHE_TTL)]
    # ... Recuperar caché válido ...
    for uid in unique_ids:
        if uid not in missing: result[uid] = SCRYFALL_CACHE[uid][0]
        
    if missing:
        batches = [missing[i:i + 75] for i in range(0, len(missing), 75)]
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_json_with_retry(session, "https://api.scryfall.com/cards/collection", method="POST", json_body={"identifiers": [{"id": sid} for sid in batch]}) for batch in batches]
            responses = await asyncio.gather(*tasks)
            for resp in responses:
                if not resp or "data" not in resp: continue
                for card in resp["data"]:
                    info = {
                        "canonical_name": card.get("name", "").split(" // ")[0],
                        "banned": card.get("legalities", {}).get("commander") == "banned",
                        "edhrec": card.get("edhrec_rank") or 999999,
                        "usd": float(card.get("prices", {}).get("usd") or 0.0)
                    }
                    result[card.get("id")] = info
                    SCRYFALL_CACHE[card.get("id")] = (info, datetime.now().timestamp())
    return result

async def get_jumpseller_stock(session, name: str) -> int:
    target = clean_card_name(name)
    if not target: return 0
    if target in STOCK_CACHE and (datetime.now().timestamp() - STOCK_CACHE[target][1] < CACHE_TTL): return STOCK_CACHE[target][0]
    
    data = await fetch_json_with_retry(session, f"{settings.JUMPSELLER_API_BASE}/products.json", params={"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN, "query": target, "limit": 50, "fields": "stock,name,variants"})
    total = 0
    if data:
        for p in data:
            if target in clean_card_name(p.get("product", {}).get("name", "")):
                base = p.get("product", {}).get("stock", 0)
                vars_stock = sum(v.get("stock", 0) for v in p.get("product", {}).get("variants", []))
                total += max(base, vars_stock)
    STOCK_CACHE[target] = (total, datetime.now().timestamp())
    return total

async def procesar_csv_logic(content: bytes, internal_mode: bool) -> List[Dict]:
    start_time = time.time()
    logger.info("Iniciando procesamiento CSV...")

    def parse_csv():
        try:
            df = pd.read_csv(io.BytesIO(content))
            # Normalizar columnas: quitar espacios y minúsculas para comparar
            df.columns = [c.strip() for c in df.columns]
            
            # MAPEO INTELIGENTE (Soporta ManaBox y Estándar)
            # ManaBox usa: "Name", "Set code", "Foil", "Quantity", "Purchase Price", "Scryfall ID"
            column_map = {
                "Name": "name",
                "Set code": "set_code",
                "Foil": "foil",
                "Quantity": "quantity",
                "Purchase price": "purchase_price",
                "Purchase Price": "purchase_price", # Variación ManaBox
                "Scryfall ID": "scryfall_id",
                "Meta Name": "meta_name"
            }
            
            df.rename(columns=column_map, inplace=True)
            
            if "name" not in df.columns:
                logger.error("CSV inválido: Falta columna Name")
                return None

            # Rellenar columnas faltantes (ej: meta_name en ManaBox no existe)
            required = ["name", "set_code", "foil", "scryfall_id", "meta_name"]
            for col in required:
                if col not in df.columns:
                    df[col] = "" # Default vacío para evitar KeyErrors

            # Limpieza de datos
            df["quantity"] = pd.to_numeric(df["quantity"], errors='coerce').fillna(0).astype(int)
            df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors='coerce').fillna(0.0)
            df.fillna("", inplace=True)

            # Agrupación
            keys = ["name", "set_code", "foil", "scryfall_id", "meta_name"]
            df["_v"] = df["quantity"] * df["purchase_price"]
            df = df.groupby(keys, as_index=False).agg({"quantity":"sum", "_v":"sum"})
            df["purchase_price"] = df.apply(lambda x: x["_v"]/x["quantity"] if x["quantity"]>0 else 0, axis=1)
            
            return df
        except Exception as e:
            logger.error(f"Error Parser: {e}")
            return None

    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, parse_csv)
    if df is None: return {"error": "Formato de CSV no reconocido (Use ManaBox o Estándar)"}

    # ... (Resto de la lógica de Scryfall y Jumpseller se mantiene IDÉNTICA) ...
    # [Asegúrate de copiar el resto de la función tal como estaba en la versión anterior]
    # ...
    
    # IMPORTANTE: Al final de la lógica, asegúrate de devolver los datos procesados.
    # Aquí te resumo el bloque final para que no haya dudas:
    
    sf_ids = df["scryfall_id"].unique().tolist()
    sf_meta = await fetch_scryfall_metadata(sf_ids)

    df["clean_name"] = df.apply(lambda row: clean_card_name(row.get("meta_name") or sf_meta.get(row.get("scryfall_id"), {}).get("canonical_name") or row.get("name")), axis=1)
    
    unique_names = df["clean_name"].unique().tolist()
    stock_map = {}
    
    async with aiohttp.ClientSession() as sess:
        sem = asyncio.Semaphore(12)
        async def task(n):
            try:
                async with sem: return n, await get_jumpseller_stock(sess, n)
            except: return n, 0
        stock_res = await asyncio.gather(*[task(n) for n in unique_names])
        stock_map = dict(stock_res)

    def logic(df):
        results = []
        for idx, row in df.iterrows():
            try:
                item = row.to_dict()
                m = sf_meta.get(item.get("scryfall_id"), {})
                
                item["name"] = item["clean_name"]
                item["banned"] = m.get("banned", False)
                item["edhrec"] = m.get("edhrec", 999999)
                item["mkt"] = m.get("usd", 0.0)
                
                if item["purchase_price"] <= 0 and item["mkt"] > 0: item["purchase_price"] = item["mkt"]
                
                item["cash_clp"] = round_clp(item["purchase_price"] * settings.USD_TO_CLP * settings.CASH_MULTIPLIER)
                item["gc_price"] = round_clp(item["purchase_price"] * settings.USD_TO_CLP * settings.GAMECOIN_MULTIPLIER)
                item["stock_tienda"] = int(stock_map.get(item["name"], 0))

                cat = "compra"
                if item["banned"]: cat = "no_compra"
                else:
                    is_staple = any(clean_card_name(s) == item["name"] for s in settings.high_demand_cards) or item["edhrec"] < 500
                    limit = settings.STOCK_LIMIT_HIGH_DEMAND if is_staple else settings.STOCK_LIMIT_DEFAULT
                    qty_sug = max(0, min(item["quantity"], limit - item["stock_tienda"]))
                    item["qty_sug"] = qty_sug
                    
                    if qty_sug == 0 or item["purchase_price"] < settings.MIN_PURCHASE_USD: cat = "no_compra"
                    elif str(item.get("foil","")).lower() == "foil" and item["purchase_price"] >= settings.STAKE_MIN_PRICE_FOR_STAKE:
                        if item["mkt"] > 0 and (item["purchase_price"]/item["mkt"] >= settings.STAKE_RATIO_THRESHOLD):
                            cat = "estaca"
                
                item["cat"] = cat
                item["rank"] = {"estaca":0, "compra":1, "no_compra":2}[cat]
                results.append(item)
            except Exception as e:
                logger.warning(f"Error Fila {idx}: {e}")
        return sorted(results, key=lambda x: (x['rank'], -x['purchase_price']))

    final_data = await loop.run_in_executor(None, logic, df)
    logger.info(f"Finalizado. Items: {len(final_data)}. Tiempo: {time.time()-start_time:.2f}s")
    return final_data

# ... (Función sync_jumpseller_customers_logic que agregaste antes) ...
async def sync_jumpseller_customers_logic():
    url = f"{settings.JUMPSELLER_API_BASE}/customers.json"
    params = {"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN, "limit": 50}
    all_customers = []
    page = 1
    async with aiohttp.ClientSession() as session:
        while True:
            params['page'] = page
            data = await fetch_json_with_retry(session, url, params=params)
            if not data or len(data) == 0: break
            for entry in data:
                c = entry.get('customer', {})
                if c.get('email'):
                    all_customers.append({"email": c.get('email'), "name": f"{c.get('name', '')} {c.get('surname', '')}".strip()})
            if len(data) < 50: break
            page += 1
    return all_customers
def enviar_correo_dual(cli, items, clp, gc, csv_content, fname):
    if not settings.SMTP_EMAIL: return

async def sync_jumpseller_customers_logic():
    """Descarga todos los clientes de Jumpseller para actualizar nombres locales."""
    url = f"{settings.JUMPSELLER_API_BASE}/customers.json"
    params = {"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN, "limit": 50}
    
    all_customers = []
    page = 1
    
    async with aiohttp.ClientSession() as session:
        while True:
            params['page'] = page
            # Usamos la función retry existente
            data = await fetch_json_with_retry(session, url, params=params)
            
            if not data or len(data) == 0:
                break
                
            for entry in data:
                c = entry.get('customer', {})
                if c.get('email'):
                    all_customers.append({
                        "email": c.get('email'),
                        "name": f"{c.get('name', '')} {c.get('surname', '')}".strip(),
                        "phone": c.get('phone')
                    })
            
            if len(data) < 50: break # Fin de páginas
            page += 1
            
    return all_customers    

async def sync_jumpseller_customers_logic():
    """Descarga clientes de Jumpseller para llenar la Bóveda."""
    url = f"{settings.JUMPSELLER_API_BASE}/customers.json"
    params = {"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN, "limit": 50}
    
    all_customers = []
    page = 1
    
    async with aiohttp.ClientSession() as session:
        while True:
            params['page'] = page
            data = await fetch_json_with_retry(session, url, params=params)
            
            if not data or len(data) == 0: break
                
            for entry in data:
                c = entry.get('customer', {})
                if c.get('email'):
                    all_customers.append({
                        "email": c.get('email'),
                        "name": f"{c.get('name', '')} {c.get('surname', '')}".strip(),
                    })
            if len(data) < 50: break
            page += 1
    return all_customers