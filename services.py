import pandas as pd
import aiohttp
import asyncio
import io
import smtplib
import re
import unicodedata
import logging
import json
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
    return int(round(val / 100.0) * 100)

async def fetch_json(session: aiohttp.ClientSession, url: str, method: str = "GET", params: Optional[dict] = None, json_body: Optional[dict] = None) -> Any:
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

async def fetch_scryfall_metadata(ids: List[str]) -> Dict[str, Any]:
    unique_ids = list(set(i for i in ids if isinstance(i, str) and i))
    result = {}
    missing = []
    now = datetime.now().timestamp()

    for uid in unique_ids:
        if uid in SCRYFALL_CACHE and (now - SCRYFALL_CACHE[uid][1] < CACHE_TTL):
            result[uid] = SCRYFALL_CACHE[uid][0]
        else:
            missing.append(uid)

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

async def get_jumpseller_stock(session: aiohttp.ClientSession, name: str) -> int:
    if not name: return 0
    target_clean = clean_card_name(name)
    if not target_clean: return 0
    
    now = datetime.now().timestamp()
    if target_clean in STOCK_CACHE and (now - STOCK_CACHE[target_clean][1] < CACHE_TTL):
        return STOCK_CACHE[target_clean][0]

    params = {
        "login": settings.JUMPSELLER_STORE,
        "authtoken": settings.JUMPSELLER_API_TOKEN,
        "query": target_clean,
        "limit": 50,
        "fields": "stock,name,variants"
    }
    
    data = await fetch_json(session, f"{settings.JUMPSELLER_API_BASE}/products.json", params=params)
    total_stock = 0
    
    if data:
        for p in data:
            prod_name_clean = clean_card_name(p.get("product", {}).get("name", ""))
            if target_clean in prod_name_clean:
                base_stock = p.get("product", {}).get("stock", 0)
                variants = p.get("product", {}).get("variants", [])
                variant_stock = sum(v.get("stock", 0) for v in variants)
                total_stock += max(base_stock, variant_stock)

    STOCK_CACHE[target_clean] = (total_stock, now)
    return total_stock

async def procesar_csv_logic(content: bytes, internal_mode: bool) -> List[Dict]:
    def parse_csv():
        try:
            df = pd.read_csv(io.BytesIO(content))
            cols_map = {
                "Name": "name", "Set code": "set_code", "Foil": "foil",
                "Quantity": "quantity", "Purchase price": "purchase_price",
                "Scryfall ID": "scryfall_id"
            }
            df.columns = [c.strip() for c in df.columns]
            df = df.rename(columns=cols_map)
            
            if "name" not in df.columns: return None
            
            df["quantity"] = pd.to_numeric(df["quantity"], errors='coerce').fillna(0).astype(int)
            # Leemos el precio del CSV tal cual viene
            df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors='coerce').fillna(0.0)
            
            keys = [k for k in ["name", "set_code", "foil", "scryfall_id"] if k in df.columns]
            for k in keys: df[k] = df[k].fillna("")
            
            # Agrupar si hay duplicados
            if keys:
                # Calculamos valor total temporal para preservar el precio ponderado si varía
                df["_val"] = df["quantity"] * df["purchase_price"]
                df = df.groupby(keys, as_index=False).agg({"quantity": "sum", "_val": "sum"})
                df["purchase_price"] = df.apply(lambda x: x["_val"]/x["quantity"] if x["quantity"]>0 else 0, axis=1)
                df = df.drop(columns=["_val"])
            
            return df
        except Exception: return None

    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, parse_csv)
    if df is None: return {"error": "El archivo CSV no tiene el formato correcto."}

    sf_ids = df["scryfall_id"].unique().tolist() if "scryfall_id" in df.columns else []
    sf_meta = await fetch_scryfall_metadata(sf_ids)

    unique_names = df["name"].unique().tolist()
    stock_map = {}
    
    async with aiohttp.ClientSession() as session:
        sem = asyncio.Semaphore(12) 
        async def bounded_stock_check(name):
            async with sem:
                return name, await get_jumpseller_stock(session, name)
        
        stock_results = await asyncio.gather(*[bounded_stock_check(n) for n in unique_names])
        stock_map = dict(stock_results)

    def apply_rules(df):
        def enrich(row):
            meta = sf_meta.get(row.get("scryfall_id"), {})
            if meta.get("canonical_name"): row["name"] = meta.get("canonical_name")
            row["banned"] = meta.get("banned", False)
            row["edhrec"] = meta.get("edhrec", 999999)
            row["mkt"] = meta.get("usd", 0.0)
            
            # --- LÓGICA DE PRECIO CORREGIDA ---
            # 1. Si el CSV trae precio (>0), lo respetamos absolutamente.
            # 2. Solo si el CSV trae 0 o error, usamos el precio de mercado (Scryfall) como rescate.
            if row["purchase_price"] <= 0 and row["mkt"] > 0:
                row["purchase_price"] = row["mkt"]
                
            return row
        
        df = df.apply(enrich, axis=1)
        
        df["cash_clp"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.CASH_MULTIPLIER).apply(round_clp)
        df["gc_price"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.GAMECOIN_MULTIPLIER).apply(round_clp)
        df["stock_tienda"] = df["name"].map(stock_map).fillna(0).astype(int)

        def classify(row):
            if row["banned"]: return "no_compra", "BANEADA"
            
            clean_name = clean_card_name(row["name"])
            is_staple = any(clean_card_name(s) == clean_name for s in settings.high_demand_cards)
            if row["edhrec"] < 500: is_staple = True
            
            limit = settings.STOCK_LIMIT_HIGH_DEMAND if is_staple else settings.STOCK_LIMIT_DEFAULT
            qty_sug = max(0, min(row["quantity"], limit - row["stock_tienda"]))
            
            row["qty_sug"] = qty_sug
            
            if qty_sug == 0: return "no_compra", "STOCK LLENO"
            if row["purchase_price"] < settings.MIN_PURCHASE_USD: return "no_compra", "BULK"
            
            is_foil = str(row.get("foil", "")).lower() == "foil"
            if is_foil and row["purchase_price"] >= settings.STAKE_MIN_PRICE_FOR_STAKE:
                if row["mkt"] > 0:
                    ratio = row["purchase_price"] / row["mkt"]
                    if ratio >= settings.STAKE_RATIO_THRESHOLD:
                        return "estaca", f"ESTACA ({ratio:.1f}x)"
            
            return "compra", "COMPRAR"

        res = df.apply(classify, axis=1, result_type="expand")
        df["cat"], df["razon"] = res[0], res[1]
        
        df["rank"] = df["cat"].map({"compra": 1, "no_compra": 2, "estaca": 0})
        return df.sort_values(["rank", "purchase_price"], ascending=[True, False]).fillna("").to_dict(orient="records")

    return await loop.run_in_executor(None, apply_rules, df)

def enviar_correo_dual(cli, items, clp, gc, csv_bytes, filename):
    if not settings.SMTP_EMAIL: return
    
    rows = ""
    for i in items:
        rows += f"""<tr>
            <td style='padding:8px;border-bottom:1px solid #ddd;text-align:center'>{i['quantity']}</td>
            <td style='padding:8px;border-bottom:1px solid #ddd'>{i['name']} <small style='color:#777'>({i['set_code']})</small></td>
            <td style='padding:8px;border-bottom:1px solid #ddd;text-align:right'>${i.get('price_unit',0):,}</td>
            <td style='padding:8px;border-bottom:1px solid #ddd;text-align:right'><b>${i.get('price_total',0):,}</b></td>
        </tr>"""

    html_body = f"""
    <div style="font-family:Helvetica,Arial,sans-serif;max-width:600px;margin:auto;border:1px solid #eee;border-radius:8px;overflow:hidden">
        <div style="background:#D32F2F;color:white;padding:15px;text-align:center"><h2 style="margin:0">Solicitud de Venta</h2></div>
        <div style="padding:20px;background:#fafafa">
            <p><strong>Cliente:</strong> {cli.get('nombre')} ({cli.get('rut')})</p>
            <p><strong>Pago:</strong> {cli.get('metodo_pago')}</p>
            <div style="text-align:right;margin:20px 0;font-size:1.2em">
                <span style="color:#10B981;font-weight:bold">{clp}</span> <small style="color:#F59E0B">({gc})</small>
            </div>
            <table width="100%" cellspacing="0" style="background:white;font-size:0.9em">{rows}</table>
            <p style="margin-top:20px;font-size:0.8em;color:#999">Notas: {cli.get('notas')}</p>
        </div>
    </div>
    """

    try:
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.starttls()
        s.login(settings.SMTP_EMAIL, settings.SMTP_PASSWORD)

        msg1 = MIMEMultipart()
        msg1['Subject'] = f"🔔 Buylist: {cli.get('nombre')} ({clp})"
        msg1['From'] = settings.SMTP_EMAIL
        msg1['To'] = settings.TARGET_EMAIL
        msg1.attach(MIMEText(html_body, 'html'))
        if csv_bytes:
            att = MIMEApplication(csv_bytes, Name=filename)
            att['Content-Disposition'] = f'attachment; filename="{filename}"'
            msg1.attach(att)
        s.sendmail(settings.SMTP_EMAIL, settings.TARGET_EMAIL, msg1.as_string())

        msg2 = MIMEMultipart()
        msg2['Subject'] = "✅ Solicitud Recibida - GameQuest"
        msg2['From'] = settings.SMTP_EMAIL
        msg2['To'] = cli.get('email')
        msg2.attach(MIMEText(html_body.replace("Solicitud de Venta", "Confirmación de Recibo"), 'html'))
        if csv_bytes:
            att = MIMEApplication(csv_bytes, Name=filename)
            att['Content-Disposition'] = f'attachment; filename="{filename}"'
            msg2.attach(att)
        s.sendmail(settings.SMTP_EMAIL, cli.get('email'), msg2.as_string())
        
        s.quit()
    except Exception as e:
        logger.error(f"SMTP Error: {e}")