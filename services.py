import pandas as pd
import requests
import io
import json
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
        total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS"]
    )
    adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "GameQuest-Bot/4.0", "Content-Type": "application/json"})
    return session

session = create_robust_session()
STOCK_CACHE = {}     
SCRYFALL_CACHE = {}
BASE_PRICE_CACHE = {}
CACHE_TTL = 300 

def normalize_card_name(name):
    if not isinstance(name, str): return ""
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('utf-8')
    name = name.split("|")[0].split("(")[0].split("[")[0]
    name = name.split(" // ")[0].split(" / ")[0]
    name = re.split(r'\s+[-–—]\s+', name)[0]
    name = re.sub(r"[^\w\s]", "", name)
    name = name.strip()
    name = re.sub(r'\s+(?:Promo|Foil|Prerelease|List|Art|Showcase|Extended|Borderless|Etched)\s*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+', ' ', name)
    return name.strip().lower()

def _fetch_scryfall_batch(batch_ids):
    url = "https://api.scryfall.com/cards/collection"
    try:
        resp = session.post(url, json={"identifiers": [{"id": sid} for sid in batch_ids]}, timeout=8)
        if resp.status_code == 200:
            data = {}
            for c in resp.json().get("data", []):
                data[c["id"]] = {
                    "canonical_name": c.get("name", "").split(" // ")[0],
                    "usd": float(c["prices"].get("usd") or 0),
                    "usd_foil": float(c["prices"].get("usd_foil") or 0),
                    "banned": c["legalities"].get("commander") == "banned"
                }
            return data
    except: pass
    return {}

def fetch_scryfall_data(ids):
    u_ids = [i for i in pd.unique(ids) if isinstance(i, str)]
    data, missing, now = {}, [], time.time()
    for i in u_ids:
        if i in SCRYFALL_CACHE and (now - SCRYFALL_CACHE[i][1] < CACHE_TTL): data[i] = SCRYFALL_CACHE[i][0]
        else: missing.append(i)
    if missing:
        with ThreadPoolExecutor(max_workers=5) as ex:
            for res in ex.map(_fetch_scryfall_batch, [missing[i:i+75] for i in range(0, len(missing), 75)]):
                data.update(res)
                for k, v in res.items(): SCRYFALL_CACHE[k] = (v, now)
    return data

def get_cheapest_print_price(name):
    clean = normalize_card_name(name)
    now = time.time()
    if clean in BASE_PRICE_CACHE:
        if now - BASE_PRICE_CACHE[clean][1] < CACHE_TTL: return BASE_PRICE_CACHE[clean][0]
    
    url = "https://api.scryfall.com/cards/search"
    params = {"q": f'!"{clean}" game:paper -is:digital', "order": "usd", "dir": "asc", "unique": "prints"}
    try:
        time.sleep(0.05)
        resp = session.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("data"):
                c = data["data"][0]
                p = float(c.get("prices", {}).get("usd") or c.get("prices", {}).get("usd_foil") or 0)
                BASE_PRICE_CACHE[clean] = (p, now)
                return p
    except: pass
    BASE_PRICE_CACHE[clean] = (0.0, now)
    return 0.0

def get_jumpseller_stock_for_name(name):
    if not name: return 0
    clean = normalize_card_name(name); now = time.time()
    
    if clean in STOCK_CACHE and (now - STOCK_CACHE[clean][1] < CACHE_TTL): 
        return STOCK_CACHE[clean][0]
    
    url = f"{settings.JUMPSELLER_API_BASE}/products.json"
    total = 0
    try:
        resp = session.get(url, params={"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN, "query": clean, "limit": 50, "fields": "stock,name,variants"}, timeout=6)
        if resp.status_code == 200:
            for p in resp.json():
                prod = p.get("product", {})
                if normalize_card_name(prod.get("name", "")) == clean:
                    vars_stock = sum(v.get("stock", 0) for v in prod.get("variants", []))
                    total += max(prod.get("stock", 0), vars_stock)
    except: pass
        
    STOCK_CACHE[clean] = (total, now)
    return total

def crear_cupon_jumpseller(codigo, monto):
    if monto <= 0: return False
    payload = {"promotion": {"name": f"Canje GC {codigo}", "code": codigo, "enabled": True, "discount_target": "order", "type": "fix", "discount_amount_fix": monto, "begins_at": datetime.datetime.now().strftime('%Y-%m-%d'), "expires_at": (datetime.datetime.now() + datetime.timedelta(days=365)).strftime('%Y-%m-%d')}}
    try: return session.post(f"{settings.JUMPSELLER_API_BASE}/promotions.json", params={"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN}, json=payload, timeout=10).status_code in [200, 201]
    except: return False

def actualizar_orden_jumpseller(oid, st, msg=""):
    try: session.put(f"{settings.JUMPSELLER_API_BASE}/orders/{oid}.json", params={"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN}, json={"order": {"status": st, "additional_information": msg}}, timeout=10)
    except: pass

def procesar_csv_manabox(content, internal_mode=False):
    try: df = pd.read_csv(io.BytesIO(content))
    except: return {"error": "CSV Inválido"}
    
    cols = {"Name": "name", "Set code": "set_code", "Foil": "foil", "Quantity": "quantity", "Purchase price": "purchase_price", "Scryfall ID": "scryfall_id"}
    df = df.rename(columns=cols)
    if "name" not in df.columns: return {"error": "Falta columna Name"}
    
    df["quantity"] = pd.to_numeric(df["quantity"], errors='coerce').fillna(0).astype(int)
    df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors='coerce')
    
    if "scryfall_id" in df.columns:
        sf_data = fetch_scryfall_data(df["scryfall_id"])
        df["name"] = df.apply(lambda row: sf_data.get(row["scryfall_id"], {}).get("canonical_name", row["name"]), axis=1)
        df["banned"] = df["scryfall_id"].map(lambda x: sf_data.get(x, {}).get("banned", False))
    else:
        df["banned"] = False

    df["cash_clp"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.CASH_MULTIPLIER).fillna(0).apply(lambda x: int(round(x/100)*100))
    df["gc_price"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.GAMECOIN_MULTIPLIER).fillna(0).apply(lambda x: int(round(x/100)*100))
    
    if internal_mode:
        unique_names = df["name"].unique()
        with ThreadPoolExecutor(max_workers=10) as ex:
            stocks = list(ex.map(get_jumpseller_stock_for_name, unique_names))
            stock_map = dict(zip(unique_names, stocks))
        df["stock_tienda"] = df["name"].map(stock_map).fillna(0).astype(int)
        
        def calc_sug(row):
            norm = normalize_card_name(row["name"])
            is_staple = any(normalize_card_name(s) == norm for s in settings.HIGH_DEMAND_CARDS)
            lim = settings.STOCK_LIMIT_HIGH_DEMAND if is_staple else settings.STOCK_LIMIT_DEFAULT
            return max(0, min(row["quantity"], lim - row["stock_tienda"]))
        df["qty_sug"] = df.apply(calc_sug, axis=1)
    else:
        df["stock_tienda"] = 0
        df["qty_sug"] = df["quantity"]

    df["is_foil"] = df["foil"].astype(str).str.lower() == "foil"
    variants = ["prerelease", "promo", "showcase", "extended", "borderless", "etched"]
    
    def check_candidate(row):
        if row["purchase_price"] >= settings.STAKE_PRICE_THRESHOLD: return True
        if row["is_foil"]: return True
        if any(k in str(row["name"]).lower() for k in variants): return True
        return False

    cands = df[df.apply(check_candidate, axis=1)]["name"].unique()
    base_ps = {}
    if len(cands) > 0:
        with ThreadPoolExecutor(max_workers=5) as ex:
            ps = list(ex.map(get_cheapest_print_price, cands))
            base_ps = dict(zip(cands, ps))

    def clasificar(row):
        if row["banned"]: return "no_compra", "BANEADA"
        
        p = row["purchase_price"]
        bp = base_ps.get(row["name"], 0)
        nm = normalize_card_name(row["name"])
        staple = any(normalize_card_name(s) == nm for s in settings.HIGH_DEMAND_CARDS)
        
        if row["is_foil"] or p >= settings.STAKE_PRICE_THRESHOLD or any(k in str(row["name"]).lower() for k in variants):
            if any(k in str(row["name"]).lower() for k in variants) and p > 5.0 and bp > 0 and p > (bp * 3):
                return "estaca", "ESTACA (Variante)"
            
            if bp > 0 and (p - bp) > 15.0:
                if not staple: return "estaca", "ESTACA (Gap)"
                elif (p - bp) > 30.0: return "estaca", "ESTACA (Staple Cara)"
            
            ratio = 2.5 if staple else 1.8
            if bp > 0 and p > (bp * ratio) and p > 5.0:
                return "estaca", f"ESTACA (x{ratio})"
            
            if p >= 20.0 and not staple:
                return "estaca", "ESTACA (Alto Valor)"

        if not p or p < settings.MIN_PURCHASE_USD: return "no_compra", "BULK"
        if internal_mode and row["qty_sug"] == 0: return "no_compra", "STOCK LLENO"
        return "compra", "COMPRAR"

    res = df.apply(clasificar, axis=1, result_type="expand")
    df["cat"], df["razon"] = res[0], res[1]
    
    df["sort_rank"] = df["cat"].map({"compra": 1, "estaca": 2, "no_compra": 3})
    final_df = df.sort_values(by=["sort_rank", "purchase_price"], ascending=[True, False])
    
    return final_df.fillna("").to_dict(orient="records")

def enviar_correo_buylist(cli, items, clp, gc):
    if not settings.SMTP_EMAIL: return {"error": "No SMTP"}
    msg = MIMEMultipart()
    msg['Subject'] = f"Buylist: {cli.get('nombre')}"
    msg['From'] = settings.SMTP_EMAIL; msg['To'] = settings.TARGET_EMAIL
    rows = "".join([f"<tr><td>{i['quantity']}</td><td>{i['name']}</td><td>${i['price_unit']}</td><td>${i['price_total']}</td></tr>" for i in items])
    msg.attach(MIMEText(f"<h3>Cliente: {cli.get('nombre')}</h3><p>{cli.get('email')} | {cli.get('telefono')}</p><table border='1'>{rows}</table><br><b>Total Cash: {clp} | Total GC: {gc}</b>", 'html'))
    try:
        s = smtplib.SMTP('smtp.gmail.com', 587); s.starttls(); s.login(settings.SMTP_EMAIL, settings.SMTP_PASSWORD)
        s.sendmail(settings.SMTP_EMAIL, settings.TARGET_EMAIL, msg.as_string()); s.quit()
        return {"status": "ok"}
    except Exception as e: return {"error": str(e)}

def sincronizar_clientes_jumpseller(db_session: Session, GameCoinUser_Model):
    return {"status": "ok"}