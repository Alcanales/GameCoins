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
from datetime import datetime
from typing import List, Dict, Any, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Services")

STOCK_CACHE: Dict[str, tuple] = {}
SCRYFALL_CACHE: Dict[str, tuple] = {}
CACHE_TTL = 300

def clean_card_name(name: str) -> str:
    if not isinstance(name, str): return ""
    name = re.sub(r"[\(\[].*?[\)\]]", "", name)
    name = unicodedata.normalize('NFD', name)
    name = "".join(c for c in name if unicodedata.category(c) != 'Mn')
    name = name.lower().replace("&", "and")
    name = re.sub(r"[^a-z0-9\s]", " ", name)
    return " ".join(name.split())

def round_clp(val: float) -> int:
    if pd.isna(val) or math.isinf(val): return 0
    return int(round(val / 100.0) * 100)

async def fetch_json(session, url, method="GET", params=None, json_body=None):
    try:
        async with session.request(method, url, params=params, json=json_body, timeout=15) as resp:
            if resp.status == 200: return await resp.json()
            elif resp.status == 429:
                await asyncio.sleep(1.5)
                return await fetch_json(session, url, method, params, json_body)
            return None
    except: return None

async def fetch_scryfall_metadata(ids: List[str]) -> Dict[str, Any]:
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
        tasks = []
        for batch in batches:
            payload = {"identifiers": [{"id": sid} for sid in batch]}
            tasks.append(fetch_json(session, "https://api.scryfall.com/cards/collection", method="POST", json_body=payload))
        responses = await asyncio.gather(*tasks)
        for resp in responses:
            if not resp or "data" not in resp: continue
            for card in resp["data"]:
                cid = card.get("id")
                prices = card.get("prices", {})
                info = {
                    "canonical_name": card.get("name", "").split(" // ")[0],
                    "banned": card.get("legalities", {}).get("commander") == "banned",
                    "edhrec": card.get("edhrec_rank") or 999999,
                    "usd": float(prices.get("usd") or 0.0),
                    "usd_foil": float(prices.get("usd_foil") or 0.0)
                }
                result[cid] = info
                SCRYFALL_CACHE[cid] = (info, now)
    return result

async def get_jumpseller_stock(session, name: str) -> int:
    if not name: return 0
    target = clean_card_name(name)
    now = datetime.now().timestamp()
    if target in STOCK_CACHE and (now - STOCK_CACHE[target][1] < CACHE_TTL):
        return STOCK_CACHE[target][0]
    
    params = {"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN, "query": target, "limit": 50, "fields": "stock,name,variants"}
    data = await fetch_json(session, f"{settings.JUMPSELLER_API_BASE}/products.json", params=params)
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
    def parse_csv():
        try:
            df = pd.read_csv(io.BytesIO(content))
            cols = {"Name": "name", "Set code": "set_code", "Foil": "foil", "Quantity": "quantity", "Purchase price": "purchase_price", "Scryfall ID": "scryfall_id"}
            df.columns = [c.strip() for c in df.columns]
            df.rename(columns=cols, inplace=True)
            if "name" not in df.columns: return None
            df["quantity"] = pd.to_numeric(df["quantity"], errors='coerce').fillna(0).astype(int)
            df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors='coerce').fillna(0.0)
            for k in ["name", "set_code", "foil", "scryfall_id"]:
                if k in df.columns: df[k] = df[k].fillna("")
            return df
        except: return None

    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, parse_csv)
    if df is None: return {"error": "CSV Inválido"}
    
    sf_ids = df["scryfall_id"].unique().tolist() if "scryfall_id" in df.columns else []
    sf_meta = await fetch_scryfall_metadata(sf_ids)
    names = df["name"].unique().tolist()
    stock_map = {}
    async with aiohttp.ClientSession() as sess:
        sem = asyncio.Semaphore(12)
        async def task(n):
            async with sem: return n, await get_jumpseller_stock(sess, n)
        stock_res = await asyncio.gather(*[task(n) for n in names])
        stock_map = dict(stock_res)

    def logic(df):
        def enrich(row):
            m = sf_meta.get(row.get("scryfall_id"), {})
            if m.get("canonical_name"): row["name"] = m["canonical_name"]
            row["banned"] = m.get("banned", False)
            row["edhrec"] = m.get("edhrec", 999999)
            row["mkt"] = m.get("usd", 0.0)
            if row["purchase_price"] <= 0 and row["mkt"] > 0: row["purchase_price"] = row["mkt"]
            return row
        df = df.apply(enrich, axis=1)
        df["cash_clp"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.CASH_MULTIPLIER).apply(round_clp)
        df["gc_price"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.GAMECOIN_MULTIPLIER).apply(round_clp)
        df["stock_tienda"] = df["name"].map(stock_map).fillna(0).astype(int)
        
        def classify(row):
            if row["banned"]: return "no_compra"
            clean = clean_card_name(row["name"])
            is_staple = any(clean_card_name(s) == clean for s in settings.high_demand_cards)
            if row["edhrec"] < 500: is_staple = True
            limit = settings.STOCK_LIMIT_HIGH_DEMAND if is_staple else settings.STOCK_LIMIT_DEFAULT
            qty_sug = max(0, min(row["quantity"], limit - row["stock_tienda"]))
            row["qty_sug"] = qty_sug
            if qty_sug == 0: return "no_compra"
            if row["purchase_price"] < settings.MIN_PURCHASE_USD: return "no_compra"
            if str(row.get("foil","")).lower() == "foil" and row["purchase_price"] >= settings.STAKE_MIN_PRICE_FOR_STAKE and row["mkt"] > 0:
                if (row["purchase_price"]/row["mkt"]) >= settings.STAKE_RATIO_THRESHOLD: return "estaca"
            return "compra"
            
        df["cat"] = df.apply(classify, axis=1)
        df["rank"] = df["cat"].map({"estaca":0, "compra":1, "no_compra":2})
        return df.sort_values(["rank", "purchase_price"], ascending=[True, False]).fillna("").to_dict(orient="records")
    return await loop.run_in_executor(None, logic, df)

def enviar_correo_dual(cli, items, clp, gc, csv, fname):
    if not settings.SMTP_EMAIL: return
    rows = "".join([f"<tr><td>{i['quantity']}</td><td>{i['name']}</td><td align='right'>${i.get('price_total',0):,}</td></tr>" for i in items])
    html = f"<h2>Orden Buylist</h2><p>Cliente: {cli.get('nombre')}</p><p>Total: {clp} / {gc}</p><table>{rows}</table>"
    try:
        s = smtplib.SMTP('smtp.gmail.com', 587); s.starttls(); s.login(settings.SMTP_EMAIL, settings.SMTP_PASSWORD)
        msg = MIMEMultipart(); msg['Subject'] = f"Buylist: {cli.get('nombre')}"; msg['From'] = settings.SMTP_EMAIL; msg['To'] = settings.TARGET_EMAIL
        msg.attach(MIMEText(html, 'html'))
        if csv: msg.attach(MIMEApplication(csv, Name=fname))
        s.sendmail(settings.SMTP_EMAIL, settings.TARGET_EMAIL, msg.as_string())
        s.quit()
    except Exception as e: logger.error(f"SMTP Error: {e}")