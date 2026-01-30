import pandas as pd
import aiohttp
import asyncio
import io
import re
import unicodedata
import logging
import json
import math
import time
from datetime import datetime
from typing import List, Dict, Any
from config import settings

# Configuración de Logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("GameQuestServices")

# Cachés en Memoria
STOCK_CACHE = {}
SCRYFALL_CACHE = {}
CACHE_TTL = 300  # 5 minutos

# Regex
REGEX_BRACKETS = re.compile(r"[\(\[].*?[\)\]]")
REGEX_KEYWORDS = re.compile(r"(?i)\b(foil|retro|etched|borderless|extended art|showcase|surge|textured|serialized|schematic|thick|frame|art series|gold border|oversized|promo|prerelease)\b")
REGEX_CLEANUP = re.compile(r"[^a-z0-9\s]")

# --- ÚTILES ---
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

async def fetch_json_with_retry(session, url, params=None, json_body=None, retries=3):
    for _ in range(retries):
        try:
            method = "POST" if json_body else "GET"
            async with session.request(method, url, params=params, json=json_body, timeout=10) as resp:
                if resp.status < 300: return await resp.json()
        except: await asyncio.sleep(1)
    return None

# --- INTEGRACIONES EXTERNAS ---
async def crear_cupon_jumpseller(session, codigo, descuento, email):
    url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
    body = {
        "promotion": {
            "name": f"Canje GQ {codigo}",
            "code": codigo,
            "discount_amount": descuento,
            "status": "active",
            "usage_limit": 1,
            "minimum_order_amount": 0,
            "begins_at": datetime.now().strftime("%Y-%m-%d"),
            "customer_emails": [email]
        }
    }
    return await fetch_json_with_retry(session, url, params={"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN}, json_body=body)

async def get_jumpseller_stock(session, name: str) -> int:
    target = clean_card_name(name)
    if not target: return 0
    
    now = datetime.now().timestamp()
    if target in STOCK_CACHE and (now - STOCK_CACHE[target][1] < CACHE_TTL):
        return STOCK_CACHE[target][0]

    # Busca producto
    params = {"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN, "query": target, "limit": 10, "fields": "stock,name,variants"}
    data = await fetch_json_with_retry(session, f"{settings.JUMPSELLER_API_BASE}/products.json", params=params)
    
    total = 0
    if data:
        for p in data:
            # Filtro estricto: El nombre del producto debe contener el nombre buscado
            if target in clean_card_name(p.get("product", {}).get("name", "")):
                prod = p.get("product", {})
                # Suma variantes si existen, sino usa stock raíz
                vars_stock = sum(v.get("stock", 0) for v in prod.get("variants", []))
                total += max(prod.get("stock", 0), vars_stock)
    
    STOCK_CACHE[target] = (total, now)
    return total

async def fetch_scryfall_metadata(ids: List[str]) -> Dict[str, Any]:
    unique = list(set(i for i in ids if i))
    result = {}
    missing = [i for i in unique if i not in SCRYFALL_CACHE]
    
    # Recuperar de caché
    for i in unique:
        if i in SCRYFALL_CACHE: result[i] = SCRYFALL_CACHE[i]

    if missing:
        # Batch requests de 75
        batches = [missing[i:i + 75] for i in range(0, len(missing), 75)]
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_json_with_retry(session, "https://api.scryfall.com/cards/collection", json_body={"identifiers": [{"id": i} for i in b]}) for b in batches]
            responses = await asyncio.gather(*tasks)
            
            for resp in responses:
                if resp and "data" in resp:
                    for card in resp["data"]:
                        info = {
                            "canonical_name": card.get("name", "").split(" // ")[0],
                            "banned": card.get("legalities", {}).get("commander") == "banned",
                            "edhrec": card.get("edhrec_rank") or 999999,
                            "usd": float(card.get("prices", {}).get("usd") or 0.0)
                        }
                        result[card["id"]] = info
                        SCRYFALL_CACHE[card["id"]] = info
    return result

async def sync_jumpseller_customers_logic():
    url = f"{settings.JUMPSELLER_API_BASE}/customers.json"
    params = {"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN, "limit": 50}
    customers = []
    page = 1
    
    async with aiohttp.ClientSession() as session:
        while True:
            params['page'] = page
            data = await fetch_json_with_retry(session, url, params=params)
            if not data: break
            
            for d in data:
                c = d.get('customer', {})
                if c.get('email'):
                    full_name = f"{c.get('name','')} {c.get('surname','')}".strip()
                    customers.append({"email": c.get('email'), "name": full_name})
            
            if len(data) < 50: break
            page += 1
    return customers

# --- PROCESAMIENTO CSV (SOPORTE MANABOX) ---
async def procesar_csv_logic(content: bytes, internal_mode: bool) -> List[Dict]:
    start_time = time.time()
    
    def parse():
        try:
            df = pd.read_csv(io.BytesIO(content))
            df.columns = [c.strip() for c in df.columns] # Limpiar espacios
            
            # Mapeo Flexible (ManaBox vs Estándar)
            map_cols = {
                "Name": "name", "Set code": "set_code", "Foil": "foil",
                "Quantity": "quantity", "Purchase price": "purchase_price", "Purchase Price": "purchase_price",
                "Scryfall ID": "scryfall_id", "Meta Name": "meta_name"
            }
            df.rename(columns=map_cols, inplace=True)
            
            if "name" not in df.columns: return None
            
            # Columnas opcionales
            for c in ["set_code", "foil", "scryfall_id", "meta_name"]:
                if c not in df.columns: df[c] = ""
            
            # Conversiones
            df["quantity"] = pd.to_numeric(df["quantity"], errors='coerce').fillna(0).astype(int)
            df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors='coerce').fillna(0.0)
            df.fillna("", inplace=True)
            
            # Unificar filas
            return df.groupby(["name", "set_code", "foil", "scryfall_id", "meta_name"], as_index=False).agg({"quantity":"sum", "purchase_price":"max"})
        except: return None

    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, parse)
    if df is None: return {"error": "Formato inválido. Use ManaBox CSV."}

    # Enriquecer datos
    sf_meta = await fetch_scryfall_metadata(df["scryfall_id"].unique().tolist())
    df["clean_name"] = df.apply(lambda r: clean_card_name(r.get("meta_name") or sf_meta.get(r.get("scryfall_id"), {}).get("canonical_name") or r.get("name")), axis=1)
    
    # Stock Check
    stock_map = {}
    async with aiohttp.ClientSession() as sess:
        tasks = [get_jumpseller_stock(sess, n) for n in df["clean_name"].unique()]
        stocks = await asyncio.gather(*tasks)
        stock_map = dict(zip(df["clean_name"].unique(), stocks))

    results = []
    for _, row in df.iterrows():
        item = row.to_dict()
        m = sf_meta.get(item["scryfall_id"], {})
        
        item["name"] = item["clean_name"]
        item["mkt"] = m.get("usd", 0.0)
        item["stock_tienda"] = stock_map.get(item["name"], 0)
        
        # Ajuste Precio
        if item["purchase_price"] <= 0 and item["mkt"] > 0: item["purchase_price"] = item["mkt"]
        item["cash_clp"] = round_clp(item["purchase_price"] * settings.USD_TO_CLP * settings.CASH_MULTIPLIER)
        
        # Clasificación
        cat = "compra"
        if m.get("banned"): cat = "no_compra"
        elif item["stock_tienda"] > settings.STOCK_LIMIT_DEFAULT: cat = "no_compra"
        elif str(item.get("foil")).lower() == "foil" and item["purchase_price"] >= settings.STAKE_MIN_PRICE_FOR_STAKE:
             if item["mkt"] > 0 and (item["purchase_price"]/item["mkt"] >= settings.STAKE_RATIO_THRESHOLD): cat = "estaca"
        
        item["cat"] = cat
        results.append(item)

    return sorted(results, key=lambda x: x['purchase_price'], reverse=True)

# Placeholder para envío de correo (Implementar lógica SMTP aquí si se requiere)
def enviar_correo_dual(cli, items, clp, gc, content, fname):
    pass