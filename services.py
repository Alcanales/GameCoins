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

# Logging Estructurado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("Services")

# --- CACHÉ EN MEMORIA (Simple) ---
# Nota: Para escalar horizontalmente (varias instancias), usar Redis.
STOCK_CACHE: Dict[str, tuple] = {}
SCRYFALL_CACHE: Dict[str, tuple] = {}
CACHE_TTL = 300  # 5 minutos

# --- UTILS ---
def normalize_text(text: str) -> str:
    if not isinstance(text, str): return ""
    text = unicodedata.normalize('NFD', text)
    text = "".join(c for c in text if unicodedata.category(c) != 'Mn')
    text = text.lower().replace("&", "and")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return " ".join(text.split())

def round_clp(val: float) -> int:
    return int(round(val / 100.0) * 100)

# --- ASYNC CLIENT ---
async def fetch_json(session: aiohttp.ClientSession, url: str, method="GET", **kwargs) -> Any:
    try:
        async with session.request(method, url, **kwargs) as resp:
            if resp.status == 200:
                return await resp.json()
            return None
    except Exception as e:
        logger.warning(f"Request failed to {url}: {e}")
        return None

# --- SCRYFALL ASYNC ---
async def fetch_scryfall_metadata(ids: List[str]) -> Dict[str, Any]:
    """Obtiene metadata de Scryfall en lotes de forma asíncrona."""
    unique_ids = list(set(i for i in ids if isinstance(i, str)))
    result_data = {}
    missing_ids = []
    now = datetime.now().timestamp()

    # Check Cache
    for uid in unique_ids:
        if uid in SCRYFALL_CACHE and (now - SCRYFALL_CACHE[uid][1] < CACHE_TTL):
            result_data[uid] = SCRYFALL_CACHE[uid][0]
        else:
            missing_ids.append(uid)

    if not missing_ids:
        return result_data

    # Batches de 75 (Límite Scryfall)
    batches = [missing_ids[i:i + 75] for i in range(0, len(missing_ids), 75)]
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for batch in batches:
            payload = {"identifiers": [{"id": sid} for sid in batch]}
            tasks.append(fetch_json(session, "https://api.scryfall.com/cards/collection", method="POST", json=payload))
        
        responses = await asyncio.gather(*tasks)

        for resp in responses:
            if not resp or "data" not in resp:
                continue
            for card in resp["data"]:
                cid = card.get("id")
                prices = card.get("prices", {})
                info = {
                    "canonical_name": card.get("name", "").split(" // ")[0],
                    "banned": card.get("legalities", {}).get("commander") == "banned",
                    "edhrec_rank": card.get("edhrec_rank") or 999999,
                    "market_usd": float(prices.get("usd") or 0.0),
                    "market_usd_foil": float(prices.get("usd_foil") or 0.0)
                }
                result_data[cid] = info
                SCRYFALL_CACHE[cid] = (info, now)
    
    return result_data

# --- JUMPSELLER ASYNC ---
async def get_jumpseller_stock(session: aiohttp.ClientSession, name: str) -> int:
    """Busca stock en Jumpseller con caché."""
    if not name: return 0
    clean_name = normalize_text(name)
    now = datetime.now().timestamp()

    if clean_name in STOCK_CACHE and (now - STOCK_CACHE[clean_name][1] < CACHE_TTL):
        return STOCK_CACHE[clean_name][0]

    params = {
        "login": settings.JUMPSELLER_STORE,
        "authtoken": settings.JUMPSELLER_API_TOKEN,
        "query": clean_name,
        "limit": 50,
        "fields": "stock,name,variants"
    }
    
    data = await fetch_json(session, f"{settings.JUMPSELLER_API_BASE}/products.json", params=params)
    total_stock = 0
    
    if data:
        for p in data:
            prod = p.get("product", {})
            p_name = normalize_text(prod.get("name", ""))
            # Lógica de coincidencia flexible
            if f" {clean_name} " in f" {p_name} ":
                vars_stock = sum(v.get("stock", 0) for v in prod.get("variants", []))
                total_stock += max(prod.get("stock", 0), vars_stock)

    STOCK_CACHE[clean_name] = (total_stock, now)
    return total_stock

# --- CSV PROCESSING ENGINE ---
async def procesar_csv_logic(content: bytes, internal_mode: bool) -> List[Dict]:
    """Lógica principal de procesamiento. Combina Pandas (CPU) con Async IO."""
    
    # 1. Pandas Parsing (CPU Bound - Ejecutar en Thread)
    def parse_csv():
        try:
            df = pd.read_csv(io.BytesIO(content))
            # Normalización de columnas
            cols_map = {
                "Name": "name", "Set code": "set_code", "Foil": "foil",
                "Quantity": "quantity", "Purchase price": "purchase_price",
                "Scryfall ID": "scryfall_id"
            }
            df.columns = [c.strip() for c in df.columns]
            df = df.rename(columns=cols_map)
            
            # Validación mínima
            if "name" not in df.columns: raise ValueError("CSV falta columna 'Name'")
            
            # Limpieza tipos
            df["quantity"] = pd.to_numeric(df["quantity"], errors='coerce').fillna(0).astype(int)
            df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors='coerce').fillna(0.0)
            
            # Agrupación (Tu lógica personalizada)
            keys = [k for k in ["name", "set_code", "foil", "scryfall_id"] if k in df.columns]
            for k in keys: df[k] = df[k].fillna("")
            
            if keys:
                df["_val"] = df["quantity"] * df["purchase_price"]
                df = df.groupby(keys, as_index=False).agg({"quantity": "sum", "_val": "sum"})
                df["purchase_price"] = df.apply(lambda x: x["_val"]/x["quantity"] if x["quantity"]>0 else 0, axis=1)
                df = df.drop(columns=["_val"])
            
            return df
        except Exception as e:
            logger.error(f"CSV Parse Error: {e}")
            return None

    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, parse_csv)
    
    if df is None: return {"error": "CSV Inválido o corrupto"}

    # 2. Enrich Metadata (Scryfall) - Async
    sf_ids = df["scryfall_id"].unique().tolist() if "scryfall_id" in df.columns else []
    sf_meta = await fetch_scryfall_metadata(sf_ids)

    # 3. Enrich Stock (Jumpseller) - Async Concurrente
    unique_names = df["name"].unique().tolist()
    stock_map = {}
    
    async with aiohttp.ClientSession() as session:
        # Semáforo para no saturar Jumpseller (Max 10 concurrent reqs)
        sem = asyncio.Semaphore(10)
        
        async def bounded_stock(name):
            async with sem:
                return name, await get_jumpseller_stock(session, name)
        
        stock_results = await asyncio.gather(*[bounded_stock(n) for n in unique_names])
        stock_map = dict(stock_results)

    # 4. Final Processing (CPU Logic)
    def apply_business_rules(df):
        # Enrich Scryfall
        def enrich(row):
            meta = sf_meta.get(row.get("scryfall_id"), {})
            if meta.get("canonical_name"): row["name"] = meta.get("canonical_name")
            row["banned"] = meta.get("banned", False)
            row["edhrec"] = meta.get("edhrec_rank", 999999)
            row["mkt_usd"] = meta.get("market_usd", 0.0)
            return row
        
        df = df.apply(enrich, axis=1)
        
        # Prices
        df["cash_clp"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.CASH_MULTIPLIER).apply(round_clp)
        df["gc_price"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.GAMECOIN_MULTIPLIER).apply(round_clp)
        df["stock_tienda"] = df["name"].map(stock_map).fillna(0).astype(int)

        # Classification Logic
        def classify(row):
            if row["banned"]: return "no_compra", "BANEADA"
            
            # Staple Check
            norm_name = normalize_text(row["name"])
            is_staple = any(normalize_text(s) == norm_name for s in settings.high_demand_cards)
            if row["edhrec"] < 500: is_staple = True
            
            limit = settings.STOCK_LIMIT_HIGH_DEMAND if is_staple else settings.STOCK_LIMIT_DEFAULT
            qty_sug = max(0, min(row["quantity"], limit - row["stock_tienda"]))
            
            # Output mutations for DataFrame
            row["qty_sug"] = qty_sug
            
            if qty_sug == 0: return "no_compra", "STOCK LLENO"
            if row["purchase_price"] < settings.MIN_PURCHASE_USD: return "no_compra", f"BULK (< ${settings.MIN_PURCHASE_USD})"
            
            # Stake Logic
            if str(row.get("foil", "")).lower() == "foil" and row["purchase_price"] >= settings.STAKE_MIN_PRICE_FOR_STAKE:
                if row["mkt_usd"] > 0:
                    ratio = row["purchase_price"] / row["mkt_usd"]
                    if ratio >= settings.STAKE_RATIO_THRESHOLD:
                        return "estaca", f"ESTACA ({ratio:.1f}x)"
            
            return "compra", "COMPRAR"

        res = df.apply(classify, axis=1, result_type="expand")
        df["cat"], df["razon"] = res[0], res[1]
        
        # Sort
        df["rank"] = df["cat"].map({"compra": 1, "no_compra": 2, "estaca": 0})
        return df.sort_values(["rank", "purchase_price"], ascending=[True, False]).fillna("").to_dict(orient="records")

    return await loop.run_in_executor(None, apply_business_rules, df)

# --- EMAIL SERVICE (SYNC wrapper for Background Tasks) ---
def enviar_correo_dual(cliente, cartas, total_clp, total_gc, csv_bytes, filename):
    """Envía correo al Staff y al Cliente."""
    if not settings.SMTP_EMAIL:
        logger.error("SMTP Not Configured")
        return

    # Generar HTML Tabla
    rows_html = ""
    for c in cartas:
        rows_html += f"""
        <tr>
            <td style="padding:5px;border-bottom:1px solid #ddd;text-align:center">{c['quantity']}</td>
            <td style="padding:5px;border-bottom:1px solid #ddd">{c['name']} <small>({c['set_code']})</small></td>
            <td style="padding:5px;border-bottom:1px solid #ddd;text-align:right">${c.get('price_unit',0):,}</td>
            <td style="padding:5px;border-bottom:1px solid #ddd;text-align:right"><b>${c.get('price_total',0):,}</b></td>
        </tr>"""

    # Template Base
    def get_template(titulo, subtitulo):
        return f"""
        <div style="font-family:sans-serif;max-width:600px;margin:auto;border:1px solid #eee;border-radius:8px;">
            <div style="background:#D32F2F;color:white;padding:15px;text-align:center"><h2>{titulo}</h2></div>
            <div style="padding:20px">
                <p>{subtitulo}</p>
                <div style="background:#f9f9f9;padding:10px;border-radius:5px;margin:10px 0;">
                    <strong>Cliente:</strong> {cliente.get('nombre')}<br>
                    <strong>RUT:</strong> {cliente.get('rut')}<br>
                    <strong>Email:</strong> {cliente.get('email')}<br>
                    <strong>Pago:</strong> {cliente.get('metodo_pago')}<br>
                    <small>{cliente.get('notas')}</small>
                </div>
                <h3 style="text-align:right;color:#10B981">Total: {total_clp} <small style="color:#F59E0B">(QP: {total_gc})</small></h3>
                <table width="100%" cellspacing="0">{rows_html}</table>
            </div>
        </div>
        """

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(settings.SMTP_EMAIL, settings.SMTP_PASSWORD)

        # 1. Correo Staff
        msg_staff = MIMEMultipart()
        msg_staff['Subject'] = f"🔔 Buylist: {cliente.get('nombre')} ({total_clp})"
        msg_staff['From'] = settings.SMTP_EMAIL
        msg_staff['To'] = settings.TARGET_EMAIL
        msg_staff.attach(MIMEText(get_template("Nueva Solicitud", "El cliente ha enviado la siguiente lista:"), "html"))
        if csv_bytes:
            part = MIMEApplication(csv_bytes, Name=filename)
            part['Content-Disposition'] = f'attachment; filename="{filename}"'
            msg_staff.attach(part)
        server.sendmail(settings.SMTP_EMAIL, settings.TARGET_EMAIL, msg_staff.as_string())

        # 2. Correo Cliente
        msg_cli = MIMEMultipart()
        msg_cli['Subject'] = "✅ Solicitud Recibida - GameQuest"
        msg_cli['From'] = settings.SMTP_EMAIL
        msg_cli['To'] = cliente.get('email')
        msg_cli.attach(MIMEText(get_template("Solicitud Recibida", "Hemos recibido tu lista. Adjuntamos tu CSV original como respaldo."), "html"))
        if csv_bytes: # También le mandamos su respaldo
            part = MIMEApplication(csv_bytes, Name=filename)
            part['Content-Disposition'] = f'attachment; filename="{filename}"'
            msg_cli.attach(part)
        server.sendmail(settings.SMTP_EMAIL, cliente.get('email'), msg_cli.as_string())

        server.quit()
    except Exception as e:
        logger.error(f"SMTP Error: {e}")