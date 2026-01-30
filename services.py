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
from typing import List, Dict, Any, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("Services")

STOCK_CACHE: Dict[str, tuple] = {}
SCRYFALL_CACHE: Dict[str, tuple] = {}
CACHE_TTL = 300

# Regex Globales
REGEX_BRACKETS = re.compile(r"[\(\[].*?[\)\]]")
FORBIDDEN_TERMS = ["foil", "retro", "etched", "borderless", "extended art", "showcase", "surge", "textured", "serialized", "schematic", "thick", "frame", "art series", "gold border", "oversized", "promo", "prerelease"]
REGEX_KEYWORDS = re.compile(r"(?i)\b(" + "|".join(FORBIDDEN_TERMS) + r")\b")
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
                elif resp.status == 429:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                elif resp.status >= 500:
                    await asyncio.sleep(1)
                    continue
                return None
        except Exception as e:
            logger.error(f"Error Red ({attempt+1}): {e}")
            await asyncio.sleep(1)
    return None

async def crear_cupon_jumpseller(session, codigo: str, descuento: int, email: str):
    url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
    params = {"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN}
    payload = {
        "promotion": {
            "name": f"Canje GQ {codigo}",
            "code": codigo,
            "discount_amount": descuento,
            "status": "active",
            "usage_limit": 1,
            "minimum_order_amount": 0,
            "begins_at": datetime.now().strftime("%Y-%m-%d")
        }
    }
    return await fetch_json_with_retry(session, url, method="POST", params=params, json_body=payload)

async def fetch_scryfall_metadata(ids: List[str]) -> Dict[str, Any]:
    # (Mismo código de fetch_scryfall_metadata existente, se omite por brevedad si no cambió lógica)
    # ... [Mantener implementación original] ...
    unique_ids = list(set(i for i in ids if isinstance(i, str) and i))
    result = {}
    missing = []
    now = datetime.now().timestamp()
    for uid in unique_ids:
        if uid in SCRYFALL_CACHE and (now - SCRYFALL_CACHE[uid][1] < CACHE_TTL):
            result[uid] = SCRYFALL_CACHE[uid][0]
        else: missing.append(uid)
    if not missing: return result
    
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
                SCRYFALL_CACHE[card.get("id")] = (info, now)
    return result

async def get_jumpseller_stock(session, name: str) -> int:
    target = clean_card_name(name)
    if not target: return 0
    now = datetime.now().timestamp()
    if target in STOCK_CACHE and (now - STOCK_CACHE[target][1] < CACHE_TTL): return STOCK_CACHE[target][0]
    
    params = {"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN, "query": target, "limit": 50, "fields": "stock,name,variants"}
    data = await fetch_json_with_retry(session, f"{settings.JUMPSELLER_API_BASE}/products.json", params=params)
    total = 0
    if data:
        for p in data:
            if target in clean_card_name(p.get("product", {}).get("name", "")):
                base = p.get("product", {}).get("stock", 0)
                vars_stock = sum(v.get("stock", 0) for v in p.get("product", {}).get("variants", []))
                total += max(base, vars_stock)
    STOCK_CACHE[target] = (total, now)
    return total

async def procesar_csv_logic(content: bytes, internal_mode: bool) -> List[Dict]:
    start_time = time.time()
    logger.info("Iniciando procesamiento CSV...")
    
    def parse_csv():
        try:
            df = pd.read_csv(io.BytesIO(content))
            cols = {"Name": "name", "Set code": "set_code", "Foil": "foil", "Quantity": "quantity", "Purchase price": "purchase_price", "Scryfall ID": "scryfall_id", "Meta Name": "meta_name"}
            df.columns = [c.strip() for c in df.columns]
            df.rename(columns=cols, inplace=True)
            if "name" not in df.columns: return None
            
            df["quantity"] = pd.to_numeric(df["quantity"], errors='coerce').fillna(0).astype(int)
            df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors='coerce').fillna(0.0)
            for k in ["name", "set_code", "foil", "scryfall_id", "meta_name"]:
                if k not in df.columns: df[k] = ""
                else: df[k] = df[k].fillna("")
            
            keys = ["name", "set_code", "foil", "scryfall_id", "meta_name"]
            df["_v"] = df["quantity"] * df["purchase_price"]
            df = df.groupby(keys, as_index=False).agg({"quantity":"sum", "_v":"sum"})
            df["purchase_price"] = df.apply(lambda x: x["_v"]/x["quantity"] if x["quantity"]>0 else 0, axis=1)
            return df
        except Exception as e:
            logger.error(f"Error Parsing: {e}")
            return None

    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, parse_csv)
    if df is None: return {"error": "CSV inválido"}

    sf_ids = df["scryfall_id"].unique().tolist()
    sf_meta = await fetch_scryfall_metadata(sf_ids) # Ya tiene manejo de error interno

    # Pre-fetch Stock
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

    # Lógica Fila a Fila (Observabilidad)
    def logic(df):
        results = []
        errors = 0
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

                # Clasificación
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
                errors += 1
                logger.warning(f"Error fila {idx}: {e}")
        
        return sorted(results, key=lambda x: (x['rank'], -x['purchase_price']))

    final_data = await loop.run_in_executor(None, logic, df)
    logger.info(f"Finalizado en {time.time()-start_time:.2f}s. Items: {len(final_data)}. Errores: {len(df)-len(final_data)}")
    return final_data

def enviar_correo_dual(cli, items, clp, gc, csv_content, fname):
    if not settings.SMTP_EMAIL: return
    rows = "".join([f"<tr><td style='padding:8px;border-bottom:1px solid #ddd;text-align:center'>{i['quantity']}</td><td style='padding:8px;border-bottom:1px solid #ddd'>{i['name']} <small>({i['set_code']})</small></td><td style='text-align:right'>${i.get('price_total',0):,}</td></tr>" for i in items])
    terms = "<div style='background:#f9fafb;padding:15px;margin-top:20px;font-size:0.85em;color:#374151;border-left:4px solid #D32F2F;'><strong>Términos y Condiciones:</strong><ul style='margin:5px 0 0 20px;'><li>Precios sujetos a validación.</li><li>Validez: 24 horas.</li></ul></div>"
    html = f"""<div style="font-family:sans-serif;max-width:600px;margin:auto;border:1px solid #ddd;border-radius:8px;"><div style="background:#D32F2F;color:white;padding:15px;text-align:center"><h3>Buylist: {cli.get('nombre')}</h3></div><div style="padding:20px;"><p><strong>RUT:</strong> {cli.get('rut')} | <strong>Pago:</strong> {cli.get('metodo_pago')}</p><div style="text-align:right;margin-bottom:15px;"><span style="font-size:1.2em;color:#166534;font-weight:bold">Cash: {clp}</span><br><span style="font-size:1.1em;color:#854d0e;font-weight:bold">QP: {gc}</span></div><table width="100%" cellspacing="0">{rows}</table>{terms}</div></div>"""
    
    try:
        s = smtplib.SMTP('smtp.gmail.com', 587); s.starttls(); s.login(settings.SMTP_EMAIL, settings.SMTP_PASSWORD)
        msg = MIMEMultipart(); msg['Subject'] = f"🔔 Buylist: {cli.get('nombre')}"; msg['From'] = settings.SMTP_EMAIL; msg['To'] = settings.TARGET_EMAIL
        msg.attach(MIMEText(html, 'html'))
        if csv_content: msg.attach(MIMEApplication(csv_content, Name=fname))
        s.sendmail(settings.SMTP_EMAIL, settings.TARGET_EMAIL, msg.as_string())
        
        msg2 = MIMEMultipart(); msg2['Subject'] = "✅ Recepción GameQuest"; msg2['From'] = settings.SMTP_EMAIL; msg2['To'] = cli.get('email')
        msg2.attach(MIMEText(html.replace("Buylist:", "Recepción:"), 'html'))
        if csv_content: msg2.attach(MIMEApplication(csv_content, Name=fname))
        s.sendmail(settings.SMTP_EMAIL, cli.get('email'), msg2.as_string())
        s.quit()
    except Exception as e: logger.error(f"SMTP Error: {e}")