import pandas as pd
import requests
import io
import smtplib
import datetime
import re
import time
import unicodedata
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy.orm import Session
from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GameCoinsServices")

def create_robust_session():
    session = requests.Session()
    retry = Retry(
        total=3, 
        backoff_factor=0.3, 
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS"]
    )
    adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "GameQuest-BuylistBot/2.1", 
        "Content-Type": "application/json"
    })
    return session

session = create_robust_session()

STOCK_CACHE = {}     
SCRYFALL_METADATA_CACHE = {}
CACHE_TTL = 300 

def normalize_text_strict(text):
    if not isinstance(text, str): return ""
    text = unicodedata.normalize('NFD', text)
    text = "".join(c for c in text if unicodedata.category(c) != 'Mn') 
    text = text.lower().replace("&", "and") 
    text = re.sub(r"[^a-z0-9\s]", " ", text) 
    return " ".join(text.split())

def redondear_a_100(valor):
    return int(round(valor / 100.0) * 100)

def _fetch_scryfall_batch_metadata(batch_ids):
    url = "https://api.scryfall.com/cards/collection"
    try:
        payload = {"identifiers": [{"id": sid} for sid in batch_ids]}
        resp = session.post(url, json=payload, timeout=10)
        
        if resp.status_code == 200:
            data = {}
            for c in resp.json().get("data", []):
                canon = c.get("name", "").split(" // ")[0] 
                prices = c.get("prices", {})
                
                data[c["id"]] = {
                    "canonical_name": canon,
                    "banned": c.get("legalities", {}).get("commander") == "banned",
                    "edhrec_rank": c.get("edhrec_rank") or 999999,
                    "market_usd": float(prices.get("usd") or 0.0),
                    "market_usd_foil": float(prices.get("usd_foil") or 0.0)
                }
            return data
    except Exception as e:
        logger.error(f"Error Scryfall Batch: {e}")
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
        with ThreadPoolExecutor(max_workers=8) as ex:
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
    if not clean_target: return 0
    
    now = time.time()
    
    if clean_target in STOCK_CACHE and (now - STOCK_CACHE[clean_target][1] < CACHE_TTL): 
        return STOCK_CACHE[clean_target][0]
    
    url = f"{settings.JUMPSELLER_API_BASE}/products.json"
    total = 0
    try:
        params = {
            "login": settings.JUMPSELLER_STORE, 
            "authtoken": settings.JUMPSELLER_API_TOKEN, 
            "query": clean_target, 
            "limit": 100, 
            "fields": "stock,name,variants"
        }
        resp = session.get(url, params=params, timeout=5)
        
        if resp.status_code == 200:
            for p in resp.json():
                prod = p.get("product", {})
                prod_raw_name = prod.get("name", "")
                prod_clean = normalize_text_strict(prod_raw_name)
                
                # Coincidencia flexible: "krosan grip" in "krosan grip español"
                match = f" {clean_target} " in f" {prod_clean} "
                
                if match:
                    vars_stock = sum(v.get("stock", 0) for v in prod.get("variants", []))
                    total += max(prod.get("stock", 0), vars_stock)
                    
    except Exception as e: 
        logger.warning(f"Error Jumpseller Stock '{name}': {e}")
        pass
        
    STOCK_CACHE[clean_target] = (total, now)
    return total

def procesar_csv_manabox(content, internal_mode=True):
    try: 
        df = pd.read_csv(io.BytesIO(content))
    except: 
        return {"error": "CSV Inválido. Asegúrate de exportar desde ManaBox correctamente."}
    
    cols_map = {
        "Name": "name", "Set code": "set_code", "Foil": "foil", 
        "Quantity": "quantity", "Purchase price": "purchase_price", 
        "Scryfall ID": "scryfall_id"
    }
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns=cols_map)
    
    if "name" not in df.columns: return {"error": "Falta columna 'Name' en el CSV."}
    
    df["quantity"] = pd.to_numeric(df["quantity"], errors='coerce').fillna(0).astype(int)
    df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors='coerce').fillna(0.0)

    group_keys = [c for c in ["name", "set_code", "foil", "scryfall_id"] if c in df.columns]
    for col in group_keys: df[col] = df[col].fillna("")

    if group_keys:
        df["_total_value"] = df["quantity"] * df["purchase_price"]
        df = df.groupby(group_keys, as_index=False).agg({"quantity": "sum", "_total_value": "sum"})
        df["purchase_price"] = df.apply(lambda x: x["_total_value"]/x["quantity"] if x["quantity"]>0 else 0.0, axis=1)
        df = df.drop(columns=["_total_value"])
    
    if "scryfall_id" in df.columns:
        sf_ids = df["scryfall_id"].dropna().unique()
        sf_data = fetch_scryfall_metadata(sf_ids)
        
        def enrich_row(row):
            sid = row.get("scryfall_id")
            meta = sf_data.get(sid, {})
            if meta.get("canonical_name"): row["name"] = meta.get("canonical_name")
            row["banned"] = meta.get("banned", False)
            row["edhrec_rank"] = meta.get("edhrec_rank", 999999)
            row["market_usd"] = meta.get("market_usd", 0.0)
            row["market_usd_foil"] = meta.get("market_usd_foil", 0.0)
            return row
        df = df.apply(enrich_row, axis=1)
    else:
        df["banned"] = False; df["edhrec_rank"] = 999999; df["market_usd"] = 0.0

    df["cash_clp"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.CASH_MULTIPLIER).apply(redondear_a_100)
    df["gc_price"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.GAMECOIN_MULTIPLIER).apply(redondear_a_100)
    
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
    # ------------------------------------------------------------

    def clasificar(row):
        if row["banned"]: return "no_compra", "BANEADA"
        
  
        if row["qty_sug"] == 0: return "no_compra", "STOCK LLENO"

        p_curr = float(row["purchase_price"])
        is_foil = str(row.get("foil", "")).lower() == "foil"

        if p_curr < settings.MIN_PURCHASE_USD: return "no_compra", f"BULK (< ${settings.MIN_PURCHASE_USD})"
        
        if is_foil and p_curr >= settings.STAKE_MIN_PRICE_FOR_STAKE:
            market_norm = row.get("market_usd", 0.0)
            if market_norm == 0: return "estaca", "ESTACA 🔥 (Sin ref)"
            
            ratio = p_curr / market_norm
            spread = p_curr - market_norm
            if ratio >= settings.STAKE_RATIO_THRESHOLD and spread >= settings.STAKE_MIN_SPREAD:
                return "estaca", f"ESTACA 🔥 (Ratio {ratio:.1f}x)"

        return "compra", "COMPRAR"

    res = df.apply(clasificar, axis=1, result_type="expand")
    df["cat"], df["razon"] = res[0], res[1]
    
    df["sort_rank"] = df["cat"].map({"compra": 1, "no_compra": 2})
    df.loc[df["cat"] == "estaca", "sort_rank"] = 0 
    
    final_df = df.sort_values(by=["sort_rank", "purchase_price"], ascending=[True, False])
    return final_df.fillna("").to_dict(orient="records")

def enviar_correo_buylist(cli, items, clp, gc):
    if not settings.SMTP_EMAIL: return {"error": "SMTP no configurado"}
    msg = MIMEMultipart(); msg['Subject'] = f"Buylist: {cli.get('nombre')} ({clp})"; msg['From'] = settings.SMTP_EMAIL; msg['To'] = settings.TARGET_EMAIL
    
    rows = ""
    for i in items:
        color = "#dcfce7" if i['cat'] == 'compra' else ("#fee2e2" if i['cat'] == 'no_compra' else "#fff7ed")
        rows += f"<tr style='background-color:{color}'><td style='padding:5px;text-align:center'>{i['quantity']}</td><td style='padding:5px'><b>{i['name']}</b><br><small>{i['set_code'] or ''}</small></td><td style='padding:5px'>{i['razon']}</td><td style='padding:5px;text-align:right'>${i.get('cash_clp',0):,}</td><td style='padding:5px;text-align:right'>${i.get('gc_price',0):,}</td></tr>"
    
    html = f"""
    <h2>Buylist de {cli.get('nombre')}</h2>
    <p>RUT: {cli.get('rut')} | Pago: {cli.get('metodo_pago')}</p>
    <p>Notas: {cli.get('notas')}</p>
    <hr>
    <h3>Total: <span style='color:green'>{clp}</span> | GC: <span style='color:orange'>{gc}</span></h3>
    <table border='1' cellspacing='0' cellpadding='5' width='100%'>
        <tr><th>Cant</th><th>Carta</th><th>Estado</th><th>Cash</th><th>GC</th></tr>
        {rows}
    </table>
    """
    msg.attach(MIMEText(html, 'html'))
    try:
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.starttls()
        s.login(settings.SMTP_EMAIL, settings.SMTP_PASSWORD)
        s.sendmail(settings.SMTP_EMAIL, settings.TARGET_EMAIL, msg.as_string())
        s.quit()
        return {"status": "ok"}
    except Exception as e: return {"error": str(e)}

def crear_cupon_jumpseller(codigo, monto):
    if monto <= 0: return False
    url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
    params = {"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN}
    payload = {
        "promotion": {
            "name": f"Canje GC {codigo}", 
            "code": codigo, 
            "enabled": True, 
            "discount_target": "order", 
            "type": "fix", 
            "discount_amount_fix": monto, 
            "begins_at": datetime.datetime.now().strftime('%Y-%m-%d'), 
            "expires_at": (datetime.datetime.now() + datetime.timedelta(days=365)).strftime('%Y-%m-%d')
        }
    }
    try: 
        resp = session.post(url, params=params, json=payload, timeout=10)
        return resp.status_code in [200, 201]
    except: return False

def sincronizar_clientes_jumpseller(db_session, GameCoinUser_Model):
    page = 1; nuevos = 0; actualizados = 0
    while True:
        url = f"{settings.JUMPSELLER_API_BASE}/customers.json"
        try:
            resp = session.get(url, params={"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN, "page": page}, timeout=20)
            
            if resp.status_code != 200 or not resp.json(): break
            
            clientes_api = resp.json()
            clientes_map = {}
            for c in clientes_api:
                raw_email = c.get("customer", {}).get("email", "")
                if raw_email:
                    clean_email = raw_email.strip().lower()
                    clientes_map[clean_email] = c.get("customer", {})
            
            if not clientes_map: page += 1; continue
            
            emails_lote = list(clientes_map.keys())
            usuarios_db = db_session.query(GameCoinUser_Model).filter(GameCoinUser_Model.email.in_(emails_lote)).all()
            usuarios_db_map = {u.email: u for u in usuarios_db}
            
            for email, data in clientes_map.items():
                nom = ""; ape = ""
                billing = data.get("billing_address", {})
                shipping = data.get("shipping_address", {})
                
                if billing.get("name"): nom = billing.get("name"); ape = billing.get("surname")
                elif shipping.get("name"): nom = shipping.get("name"); ape = shipping.get("surname")
                
                if not nom:
                    parts = data.get("fullname", "Cliente").split(" ", 1)
                    nom = parts[0]; ape = parts[1] if len(parts) > 1 else ""
                
                nom = normalize_text_strict(nom or "Cliente").title()
                ape = normalize_text_strict(ape or "").title()
                
                rut = data.get("tax_id") or ""
                if not rut:
                    for f in data.get("fields", []):
                        if "rut" in str(f.get("label", "")).lower(): rut = str(f.get("value", "")).strip(); break
                
                user = usuarios_db_map.get(email)
                if user:
                    if user.name != nom: user.name = nom
                    if user.surname != ape: user.surname = ape
                    actualizados += 1
                else:
                    rf = rut if rut else f"PENDIENTE-{email}"
                    db_session.add(GameCoinUser_Model(email=email, name=nom, surname=ape, rut=rf, saldo=0))
                    nuevos += 1
            
            db_session.commit()
            page += 1
        except Exception as e:
            db_session.rollback()
            return {"status": "error", "detail": str(e)}
            
    return {"status": "ok", "nuevos": nuevos, "actualizados": actualizados}