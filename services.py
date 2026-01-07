import pandas as pd
import requests
import io
import smtplib
import datetime
import re
import time
import unicodedata
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.orm import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from models import GameCoinUser
from config import settings

def create_robust_session():
    session = requests.Session()
    retry = Retry(
        total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS"]
    )
    adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "GameQuest-Bot/9.0", "Content-Type": "application/json"})
    return session

session = create_robust_session()

STOCK_CACHE = {}     
SCRYFALL_METADATA_CACHE = {}
CACHE_TTL = 300 

def normalize_text_strict(text):
    if not isinstance(text, str): 
        return ""
    text = unicodedata.normalize('NFD', text)
    text = "".join(c for c in text if unicodedata.category(c) != 'Mn')
    text = text.lower()
    text = text.replace("&", "and")
    text = re.sub(r"[^a-z0-9\s]", "", text)
    text = " ".join(text.split())
    return text

def redondear_a_100(valor):
    return int(round(valor / 100.0) * 100)

def _fetch_scryfall_batch_metadata(batch_ids):
    url = "https://api.scryfall.com/cards/collection"
    try:
        resp = session.post(url, json={"identifiers": [{"id": sid} for sid in batch_ids]}, timeout=10)
        if resp.status_code == 200:
            data = {}
            for c in resp.json().get("data", []):
                canon = c.get("name", "").split(" // ")[0]
                data[c["id"]] = {
                    "canonical_name": canon,
                    "normalized_name": normalize_text_strict(canon),
                    "banned": c.get("legalities", {}).get("commander") == "banned",
                    "edhrec_rank": c.get("edhrec_rank")
                }
            return data
    except Exception: 
        pass
    return {}

def fetch_scryfall_metadata(ids):
    u_ids = [i for i in pd.unique(ids) if isinstance(i, str)]
    data, missing, now = {}, [], time.time()
    
    for i in u_ids:
        if i in SCRYFALL_METADATA_CACHE and (now - SCRYFALL_METADATA_CACHE[i][1] < CACHE_TTL): 
            data[i] = SCRYFALL_METADATA_CACHE[i][0]
        else: 
            missing.append(i)
            
    if missing:
        with ThreadPoolExecutor(max_workers=5) as ex:
            batches = [missing[i:i+75] for i in range(0, len(missing), 75)]
            futures = {ex.submit(_fetch_scryfall_batch_metadata, b): b for b in batches}
            for f in as_completed(futures):
                result = f.result()
                data.update(result)
                for k, v in result.items(): 
                    SCRYFALL_METADATA_CACHE[k] = (v, now)
    return data

def get_jumpseller_stock_for_name(name):
    if not name: return 0
    clean_target = normalize_text_strict(name)
    now = time.time()
    
    if clean_target in STOCK_CACHE and (now - STOCK_CACHE[clean_target][1] < CACHE_TTL): 
        return STOCK_CACHE[clean_target][0]
    
    url = f"{settings.JUMPSELLER_API_BASE}/products.json"
    total = 0
    try:
        params = {"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN, "query": clean_target, "limit": 50, "fields": "stock,name,variants"}
        resp = session.get(url, params=params, timeout=6)
        if resp.status_code == 200:
            for p in resp.json():
                prod = p.get("product", {})
                prod_clean = normalize_text_strict(prod.get("name", ""))
                
                if prod_clean == clean_target or prod_clean.startswith(clean_target):
                    vars_stock = sum(v.get("stock", 0) for v in prod.get("variants", []))
                    total += max(prod.get("stock", 0), vars_stock)
    except: pass
    STOCK_CACHE[clean_target] = (total, now)
    return total

def procesar_csv_manabox(content, internal_mode=False):
    try: 
        df = pd.read_csv(io.BytesIO(content))
    except: 
        return {"error": "CSV Inválido o Corrupto"}
    
    cols = {
        "Name": "name", 
        "Set code": "set_code", 
        "Foil": "foil", 
        "Quantity": "quantity", 
        "Purchase price": "purchase_price",
        "Scryfall ID": "scryfall_id"
    }
    
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns=cols)
    
    if "name" not in df.columns: 
        return {"error": "Falta columna 'Name' en el CSV"}
    
    df["quantity"] = pd.to_numeric(df["quantity"], errors='coerce').fillna(0).astype(int)
    df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors='coerce').fillna(0.0)
    
    if "scryfall_id" in df.columns:
        sf_ids = df["scryfall_id"].dropna().unique()
        sf_data = fetch_scryfall_metadata(sf_ids)
        
        def enrich_row(row):
            sid = row.get("scryfall_id")
            meta = sf_data.get(sid, {})
            row["name"] = meta.get("canonical_name", row["name"])
            row["banned"] = meta.get("banned", False)
            row["edhrec_rank"] = meta.get("edhrec_rank", 999999)
            return row
            
        df = df.apply(enrich_row, axis=1)
    else:
        df["banned"] = False
        df["edhrec_rank"] = 999999

    df["cash_clp"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.CASH_MULTIPLIER).apply(redondear_a_100)
    df["gc_price"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.GAMECOIN_MULTIPLIER).apply(redondear_a_100)
    
    if internal_mode:
        unique_names = df["name"].unique()
        with ThreadPoolExecutor(max_workers=10) as ex:
            stocks = list(ex.map(get_jumpseller_stock_for_name, unique_names))
            stock_map = dict(zip(unique_names, stocks))
        df["stock_tienda"] = df["name"].map(stock_map).fillna(0).astype(int)
        
        def is_staple(row):
            norm_name = normalize_text_strict(row["name"])
            manual_staple = any(normalize_text_strict(s) == norm_name for s in settings.HIGH_DEMAND_CARDS)
            rank_staple = (row.get("edhrec_rank", 999999) or 999999) < 500
            return manual_staple or rank_staple

        def calc_sug(row):
            limit = settings.STOCK_LIMIT_HIGH_DEMAND if is_staple(row) else settings.STOCK_LIMIT_DEFAULT
            return max(0, min(row["quantity"], limit - row["stock_tienda"]))
            
        df["qty_sug"] = df.apply(calc_sug, axis=1)
    else:
        df["stock_tienda"] = 0
        df["qty_sug"] = df["quantity"]

    def clasificar(row):
        if row["banned"]: 
            return "no_compra", "BANEADA (Commander)"
            
        p_curr = row["purchase_price"]
        if p_curr < settings.MIN_PURCHASE_USD: 
            return "no_compra", "BULK (< Min USD)"
            
        if internal_mode and row["qty_sug"] == 0: 
            return "no_compra", "STOCK LLENO"
            
        return "compra", "COMPRAR"

    res = df.apply(clasificar, axis=1, result_type="expand")
    df["cat"], df["razon"] = res[0], res[1]
    
    df["sort_rank"] = df["cat"].map({"compra": 1, "estaca": 2, "no_compra": 3})
    final_df = df.sort_values(by=["sort_rank", "purchase_price"], ascending=[True, False])
    
    return final_df.fillna("").to_dict(orient="records")

def enviar_correo_buylist(cli, items, clp, gc):
    if not settings.SMTP_EMAIL: return {"error": "No SMTP Configurado"}
    msg = MIMEMultipart()
    msg['Subject'] = f"Buylist: {cli.get('nombre')}"
    msg['From'] = settings.SMTP_EMAIL; msg['To'] = settings.TARGET_EMAIL
    
    rows = ""
    for i in items:
        rows += f"<tr><td>{i['quantity']}</td><td>{i['name']}</td><td>${i.get('cash_clp',0)}</td><td>${i.get('gc_price',0)}</td></tr>"
        
    html = f"""
    <h3>Cliente: {cli.get('nombre')}</h3>
    <p>{cli.get('email')} | {cli.get('telefono')}</p>
    <table border='1' cellpadding='5' style='border-collapse:collapse;'>
    <tr><th>Cant</th><th>Carta</th><th>Cash (CLP)</th><th>GameCoins</th></tr>
    {rows}
    </table>
    <br><b>Total Cash: {clp} | Total GC: {gc}</b>
    """
    msg.attach(MIMEText(html, 'html'))
    
    try:
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.starttls()
        s.login(settings.SMTP_EMAIL, settings.SMTP_PASSWORD)
        s.sendmail(settings.SMTP_EMAIL, settings.TARGET_EMAIL, msg.as_string())
        s.quit()
        return {"status": "ok"}
    except Exception as e: 
        return {"error": str(e)}

def crear_cupon_jumpseller(codigo, monto):
    if monto <= 0: return False
    url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
    params = {"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN}
    payload = {
        "promotion": {
            "name": f"Canje GC {codigo}", "code": codigo, "enabled": True, 
            "discount_target": "order", "type": "fix", "discount_amount_fix": monto,
            "begins_at": datetime.datetime.now().strftime('%Y-%m-%d'),
            "expires_at": (datetime.datetime.now() + datetime.timedelta(days=365)).strftime('%Y-%m-%d')
        }
    }
    try: return session.post(url, params=params, json=payload, timeout=10).status_code in [200, 201]
    except: return False

def actualizar_orden_jumpseller(oid, st, msg=""):
    try: 
        session.put(f"{settings.JUMPSELLER_API_BASE}/orders/{oid}.json", 
        params={"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN}, 
        json={"order": {"status": st, "additional_information": msg}}, timeout=10)
    except: pass

def sincronizar_clientes_jumpseller(db_session: Session, GameCoinUser_Model):
    page = 1; nuevos = 0; actualizados = 0
    while True:
        url = f"{settings.JUMPSELLER_API_BASE}/customers.json"
        try:
            resp = session.get(url, params={"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN, "limit": 50, "page": page}, timeout=20)
            if resp.status_code != 200 or not resp.json(): break
            clientes_api = resp.json()
            
            clientes_map = {}
            for c in clientes_api:
                raw_email = c.get("customer", {}).get("email", "")
                if raw_email:
                    clean_email = normalize_text_strict(raw_email).replace(" ", "")
                    clientes_map[clean_email] = c.get("customer", {})

            emails_lote = list(clientes_map.keys())
            if not emails_lote: page += 1; continue
            
            usuarios_db = db_session.query(GameCoinUser_Model).filter(GameCoinUser_Model.email.in_(emails_lote)).all()
            usuarios_db_map = {u.email: u for u in usuarios_db}
            
            for email, data in clientes_map.items():
                nom = ""; ape = ""
                billing = data.get("billing_address", {})
                shipping = data.get("shipping_address", {})
                
                if billing.get("name"):
                    nom = billing.get("name", ""); ape = billing.get("surname", "")
                elif shipping.get("name"):
                    nom = shipping.get("name", ""); ape = shipping.get("surname", "")
                
                if not nom:
                    fullname = data.get("fullname", "").strip()
                    if fullname:
                        parts = fullname.split(" ", 1)
                        nom = parts[0]
                        ape = parts[1] if len(parts) > 1 else ""
                
                nom = normalize_text_strict(nom or "Cliente").title()
                ape = normalize_text_strict(ape or "").title()
                rut = (data.get("tax_id") or "")
                
                if not rut:
                    for f in data.get