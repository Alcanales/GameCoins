import pandas as pd
import aiohttp
import asyncio
import io
import smtplib
import re
import unicodedata
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Services")

STOCK_CACHE = {}
SCRYFALL_CACHE = {}
CACHE_TTL = 300

def clean_card_name(name):
    if not isinstance(name, str): return ""
    name = re.sub(r"[\(\[].*?[\)\]]", "", name)
    name = unicodedata.normalize('NFD', name)
    name = "".join(c for c in name if unicodedata.category(c) != 'Mn')
    name = name.lower().replace("&", "and")
    name = re.sub(r"[^a-z0-9\s]", " ", name)
    return " ".join(name.split())

def round_clp(val):
    return int(round(val / 100.0) * 100)

async def fetch_json(session, url, params=None, method="GET", json_body=None):
    try:
        async with session.request(method, url, params=params, json=json_body, timeout=15) as resp:
            if resp.status == 200:
                return await resp.json()
            elif resp.status == 429:
                await asyncio.sleep(1)
                return await fetch_json(session, url, method, params, json_body)
            return None
    except Exception:
        return None

async def fetch_scryfall_metadata(ids):
    unique = list(set(i for i in ids if isinstance(i, str) and i))
    result = {}
    missing = []
    now = datetime.now().timestamp()

    for i in unique:
        if i in SCRYFALL_CACHE and (now - SCRYFALL_CACHE[i][1] < CACHE_TTL):
            result[i] = SCRYFALL_CACHE[i][0]
        else:
            missing.append(i)

    if not missing: return result

    async with aiohttp.ClientSession() as session:
        batches = [missing[i:i+75] for i in range(0, len(missing), 75)]
        tasks = []
        for b in batches:
            tasks.append(fetch_json(session, "https://api.scryfall.com/cards/collection", method="POST", json_body={"identifiers": [{"id": x} for x in b]}))
        
        resps = await asyncio.gather(*tasks)
        for r in resps:
            if not r or "data" not in r: continue
            for c in r["data"]:
                info = {
                    "canonical_name": c.get("name", "").split(" // ")[0],
                    "banned": c.get("legalities", {}).get("commander") == "banned",
                    "edhrec": c.get("edhrec_rank") or 999999,
                    "usd": float(c.get("prices", {}).get("usd") or 0),
                    "usd_foil": float(c.get("prices", {}).get("usd_foil") or 0)
                }
                result[c["id"]] = info
                SCRYFALL_CACHE[c["id"]] = (info, now)
    return result

async def get_jumpseller_stock(session, name):
    if not name: return 0
    target = clean_card_name(name)
    if not target: return 0
    
    now = datetime.now().timestamp()
    if target in STOCK_CACHE and (now - STOCK_CACHE[target][1] < CACHE_TTL):
        return STOCK_CACHE[target][0]

    data = await fetch_json(session, f"{settings.JUMPSELLER_API_BASE}/products.json", params={
        "login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN,
        "query": target, "fields": "stock,name,variants", "limit": 50
    })
    
    total = 0
    if data:
        for p in data:
            p_name = clean_card_name(p.get("product", {}).get("name", ""))
            if target in p_name:
                base = p.get("product", {}).get("stock", 0)
                vars_stock = sum(v.get("stock", 0) for v in p.get("product", {}).get("variants", []))
                total += max(base, vars_stock)

    STOCK_CACHE[target] = (total, now)
    return total

async def procesar_csv_logic(content, internal_mode):
    def parse():
        try:
            df = pd.read_csv(io.BytesIO(content))
            cols = {"Name":"name","Set code":"set_code","Foil":"foil","Quantity":"quantity","Purchase price":"purchase_price","Scryfall ID":"scryfall_id"}
            df.columns = [c.strip() for c in df.columns]
            df.rename(columns=cols, inplace=True)
            if "name" not in df.columns: return None
            
            df["quantity"] = pd.to_numeric(df["quantity"], errors='coerce').fillna(0).astype(int)
            df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors='coerce').fillna(0.0)
            
            keys = [k for k in ["name", "set_code", "foil", "scryfall_id"] if k in df.columns]
            for k in keys: df[k] = df[k].fillna("")
            
            if keys:
                df["_v"] = df["quantity"] * df["purchase_price"]
                df = df.groupby(keys, as_index=False).agg({"quantity":"sum", "_v":"sum"})
                df["purchase_price"] = df.apply(lambda x: x["_v"]/x["quantity"] if x["quantity"]>0 else 0, axis=1)
                df.drop(columns=["_v"], inplace=True)
            return df
        except: return None

    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, parse)
    if df is None: return {"error": "CSV Inválido"}

    sf_data = await fetch_scryfall_metadata(df["scryfall_id"].unique().tolist() if "scryfall_id" in df.columns else [])
    
    unique_names = df["name"].unique().tolist()
    stock_map = {}
    async with aiohttp.ClientSession() as sess:
        sem = asyncio.Semaphore(12)
        async def task(n):
            async with sem: return n, await get_jumpseller_stock(sess, n)
        stock_res = await asyncio.gather(*[task(n) for n in unique_names])
        stock_map = dict(stock_res)

    def logic(df):
        def enrich(row):
            m = sf_data.get(row.get("scryfall_id"), {})
            if m.get("canonical_name"): row["name"] = m["canonical_name"]
            row["banned"] = m.get("banned", False)
            row["edhrec"] = m.get("edhrec", 999999)
            row["mkt"] = m.get("usd", 0.0)
            return row
        df = df.apply(enrich, axis=1)

        df["cash_clp"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.CASH_MULTIPLIER).apply(round_clp)
        df["gc_price"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.GAMECOIN_MULTIPLIER).apply(round_clp)
        df["stock_tienda"] = df["name"].map(stock_map).fillna(0).astype(int)

        def classify(row):
            if row["banned"]: return "no_compra", "BANEADA"
            
            clean = clean_card_name(row["name"])
            is_staple = any(clean_card_name(s) == clean for s in settings.HIGH_DEMAND_CARDS)
            if row["edhrec"] < 500: is_staple = True
            
            limit = settings.STOCK_LIMIT_HIGH_DEMAND if is_staple else settings.STOCK_LIMIT_DEFAULT
            qty_sug = max(0, min(row["quantity"], limit - row["stock_tienda"]))
            row["qty_sug"] = qty_sug
            
            if qty_sug == 0: return "no_compra", "STOCK LLENO"
            if row["purchase_price"] < settings.MIN_PURCHASE_USD: return "no_compra", "BULK"
            
            if str(row.get("foil","")).lower() == "foil" and row["purchase_price"] >= settings.STAKE_MIN_PRICE_FOR_STAKE:
                if row["mkt"] > 0:
                    ratio = row["purchase_price"] / row["mkt"]
                    if ratio >= settings.STAKE_RATIO_THRESHOLD: return "estaca", f"ESTACA ({ratio:.1f}x)"
            
            return "compra", "COMPRAR"

        r = df.apply(classify, axis=1, result_type="expand")
        df["cat"], df["razon"] = r[0], r[1]
        df["rank"] = df["cat"].map({"compra":1, "no_compra":2, "estaca":0})
        return df.sort_values(["rank", "purchase_price"], ascending=[True, False]).fillna("").to_dict(orient="records")

    return await loop.run_in_executor(None, logic, df)

def enviar_correo_dual(cli, items, clp, gc, csv, fname):
    if not settings.SMTP_EMAIL: return
    rows = "".join([f"<tr><td style='padding:5px;border-bottom:1px solid #ddd;text-align:center'>{i['quantity']}</td><td style='padding:5px;border-bottom:1px solid #ddd'>{i['name']} <small>({i['set_code']})</small></td><td style='padding:5px;border-bottom:1px solid #ddd;text-align:right'>${i.get('price_unit',0):,}</td><td style='padding:5px;border-bottom:1px solid #ddd;text-align:right'><b>${i.get('price_total',0):,}</b></td></tr>" for i in items])
    
    html = f"""<div style="font-family:sans-serif;max-width:600px;margin:auto;border:1px solid #eee;border-radius:8px;overflow:hidden">
        <div style="background:#D32F2F;color:white;padding:15px;text-align:center"><h2>Cotización Recibida</h2></div>
        <div style="padding:20px;background:#f9f9f9">
            <p><strong>Cliente:</strong> {cli.get('nombre')} ({cli.get('rut')})</p>
            <p><strong>Pago:</strong> {cli.get('metodo_pago')}</p>
            <h3 style="text-align:right;color:#10B981">{clp} <small style="color:#F59E0B">({gc})</small></h3>
            <table width="100%" cellspacing="0" style="background:white">{rows}</table>
            <p style="margin-top:20px;color:#777;font-size:0.9em">{cli.get('notas')}</p>
        </div>
    </div>"""

    try:
        s = smtplib.SMTP('smtp.gmail.com', 587); s.starttls(); s.login(settings.SMTP_EMAIL, settings.SMTP_PASSWORD)
        
        m1 = MIMEMultipart(); m1['Subject'] = f"🔔 Buylist: {cli.get('nombre')}"; m1['From'] = settings.SMTP_EMAIL; m1['To'] = settings.TARGET_EMAIL
        m1.attach(MIMEText(html, 'html'))
        if csv: m1.attach(MIMEApplication(csv, Name=fname))
        s.sendmail(settings.SMTP_EMAIL, settings.TARGET_EMAIL, m1.as_string())

        m2 = MIMEMultipart(); m2['Subject'] = "✅ Recibido - GameQuest"; m2['From'] = settings.SMTP_EMAIL; m2['To'] = cli.get('email')
        m2.attach(MIMEText(html, 'html'))
        if csv: m2.attach(MIMEApplication(csv, Name=fname))
        s.sendmail(settings.SMTP_EMAIL, cli.get('email'), m2.as_string())
        
        s.quit()
    except Exception: pass