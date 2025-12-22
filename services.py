import pandas as pd
import requests
import io
import json
import smtplib
import datetime
import re
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.orm import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from models import GameCoinUser
from config import settings

# --- INFRAESTRUCTURA DE RED ---
def create_robust_session():
    session = requests.Session()
    retry = Retry(
        total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS"]
    )
    adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "GameQuest-Bot/2.5 (Variant Fix)", 
        "Content-Type": "application/json"
    })
    return session

session = create_robust_session()

# --- CACHÉS ---
STOCK_CACHE = {}     
SCRYFALL_CACHE = {}  
CACHE_TTL = 300 

def normalize_card_name(name):
    if not isinstance(name, str): return ""
    name = name.split("|")[0].split("(")[0].split("[")[0]
    name = re.split(r'\s+[-–—]\s+', name)[0]
    return name.strip().lower()

def _fetch_scryfall_batch(batch_ids):
    url = "https://api.scryfall.com/cards/collection"
    identifiers = [{"id": sid} for sid in batch_ids]
    local_map = {}
    try:
        resp = session.post(url, json={"identifiers": identifiers}, timeout=8)
        if resp.status_code == 200:
            for card in resp.json().get("data", []):
                p = card.get("prices", {})
                l = card.get("legalities", {})
                local_map[card.get("id")] = {
                    "usd": float(p["usd"]) if p.get("usd") else None,
                    "usd_foil": float(p["usd_foil"]) if p.get("usd_foil") else None,
                    "banned_commander": l.get("commander") == "banned",
                    "banned_modern": l.get("modern") == "banned"
                }
    except Exception: pass
    return local_map

def fetch_scryfall_data(scryfall_ids):
    unique_ids = [sid for sid in pd.unique(scryfall_ids) if isinstance(sid, str)]
    data_map = {}
    missing = []
    now = time.time()

    for sid in unique_ids:
        if sid in SCRYFALL_CACHE and (now - SCRYFALL_CACHE[sid][1] < CACHE_TTL):
            data_map[sid] = SCRYFALL_CACHE[sid][0]
        else:
            missing.append(sid)

    if missing:
        batch_size = 75
        batches = [missing[i:i+batch_size] for i in range(0, len(missing), batch_size)]
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(_fetch_scryfall_batch, b) for b in batches]
            for f in as_completed(futures):
                res = f.result()
                data_map.update(res)
                for k, v in res.items(): SCRYFALL_CACHE[k] = (v, now)
            
    return data_map

def get_jumpseller_stock_for_name(name):
    if not name: return 0
    clean = normalize_card_name(name)
    now = time.time()
    
    if clean in STOCK_CACHE:
        if now - STOCK_CACHE[clean][1] < CACHE_TTL: return STOCK_CACHE[clean][0]
        else: del STOCK_CACHE[clean]

    # FIX CRÍTICO: Solicitamos 'variants' para sumar stock real
    url = f"{settings.JUMPSELLER_API_BASE}/products.json"
    params = {"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN, "query": clean, "limit": 50, "fields": "stock,name,variants"}
    
    total = 0
    try:
        resp = session.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            for p in resp.json():
                prod = p.get("product", {})
                # Verificamos nombre exacto normalizado
                if normalize_card_name(prod.get("name", "")) == clean:
                    stock_main = prod.get("stock", 0)
                    variants = prod.get("variants", [])
                    
                    # Sumamos variantes si existen (Ej: Sol Ring con ediciones)
                    if variants:
                        stock_vars = sum(v.get("stock", 0) for v in variants)
                        total += max(stock_main, stock_vars)
                    else:
                        total += stock_main
    except Exception: pass
    
    STOCK_CACHE[clean] = (total, now)
    return total

def crear_cupon_jumpseller(codigo, monto):
    if not monto or int(monto) <= 0: return False
    url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
    params = {"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN}
    now = datetime.datetime.now()
    payload = {
        "promotion": {
            "name": f"Canje GameCoins {codigo}", "code": codigo, "enabled": True,
            "discount_target": "order", "type": "fix", "discount_amount_fix": monto,
            "minimum_order_amount": 0, "begins_at": now.strftime('%Y-%m-%d'),
            "expires_at": (now + datetime.timedelta(days=365)).strftime('%Y-%m-%d'), "accumulable": False
        }
    }
    try: return session.post(url, params=params, json=payload, timeout=10).status_code in [200, 201]
    except: return False

def actualizar_orden_jumpseller(order_id, estado, notas=""):
    url = f"{settings.JUMPSELLER_API_BASE}/orders/{order_id}.json"
    params = {"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN}
    try: session.put(url, params=params, json={"order": {"status": estado, "additional_information": notas}}, timeout=10)
    except: pass

def procesar_csv_manabox(file_content: bytes, internal_mode: bool = False):
    try: df = pd.read_csv(io.BytesIO(file_content))
    except: return {"error": "CSV inválido."}

    df = df.rename(columns={"Name": "name", "Set code": "set_code", "Foil": "foil", "Quantity": "quantity", "Purchase price": "purchase_price", "Scryfall ID": "scryfall_id"})
    if "name" not in df.columns: return {"error": "Falta columna 'Name'."}

    df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["has_price"] = df["purchase_price"].notna()
    
    scryfall_map = {}
    if "scryfall_id" in df.columns: 
        scryfall_map = fetch_scryfall_data(df["scryfall_id"])
        
    df["banned_alert"] = df["scryfall_id"].map(lambda x: scryfall_map.get(x, {}).get("banned_commander", False))
    
    df["cash_buy_price_clp"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.CASH_MULTIPLIER).fillna(0).apply(lambda x: int(round(x/100.0))*100)
    df["gamecoin_price"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.GAMECOIN_MULTIPLIER).fillna(0).apply(lambda x: int(round(x/100.0))*100)

    if internal_mode:
        names = df["name"].unique()
        stock_map = {}
        with ThreadPoolExecutor(max_workers=10) as executor: 
            results = executor.map(get_jumpseller_stock_for_name, names)
            for name, stock in zip(names, results): stock_map[name] = stock
        
        df["stock_tienda"] = df["name"].map(stock_map).fillna(0).astype(int)
        
        def calcular_sugerido(row):
            nombre = normalize_card_name(row["name"])
            stock_actual = row["stock_tienda"]
            limite = settings.STOCK_LIMIT_HIGH_DEMAND if nombre in settings.HIGH_DEMAND_CARDS else settings.STOCK_LIMIT_DEFAULT
            espacio = max(0, limite - stock_actual)
            return min(row["quantity"], espacio)

        df["qty_sugerida"] = df.apply(calcular_sugerido, axis=1)
    else:
        df["stock_tienda"] = 0
        df["qty_sugerida"] = df["quantity"]

    def clasificar(row):
        price = row["purchase_price"]
        is_foil = str(row.get("foil", "")).lower() == "foil"
        
        if row.get("banned_alert"): return "no_compra", "BANEADA (Commander)"
        if is_foil and price >= settings.STAKE_PRICE_THRESHOLD: return "estaca", "Posible Estaca (Foil)"
        if not row["has_price"] or price < settings.MIN_PURCHASE_USD: return "no_compra", "Bulk / Bajo Precio"
        if internal_mode and row["qty_sugerida"] == 0: return "no_compra", "Stock Lleno"
            
        return "compra", "Comprar"

    res = df.apply(clasificar, axis=1, result_type="expand")
    df["categoria"], df["buy_decision"] = res[0], res[1]
    df["sort_rank"] = df["categoria"].map({"compra": 1, "estaca": 2, "no_compra": 3})
    
    final_cols = ["name", "set_code", "foil", "quantity", "purchase_price", "cash_buy_price_clp", "gamecoin_price", "buy_decision", "categoria", "stock_tienda", "qty_sugerida", "banned_alert"]
    cols_existentes = [c for c in final_cols if c in df.columns]
    
    return df.sort_values(by=["sort_rank", "purchase_price"], ascending=[True, False])[cols_existentes].fillna("").to_dict(orient="records")

def enviar_correo_buylist(datos_cliente, lista_cartas, total_clp, total_gc):
    if not settings.SMTP_EMAIL or not settings.SMTP_PASSWORD: return {"error": "SMTP no configurado"}
    msg = MIMEMultipart()
    msg['From'] = settings.SMTP_EMAIL; msg['To'] = settings.TARGET_EMAIL; msg['Subject'] = f"Buylist: {datos_cliente.get('nombre')}"
    rows = "".join([f"<tr><td>{c.get('quantity')}</td><td>{c.get('name')} ({c.get('set_code')})</td><td>${c.get('price_unit')}</td><td>${c.get('price_total')}</td></tr>" for c in lista_cartas])
    html = f"<h2>Solicitud Venta</h2><p>Cliente: {datos_cliente.get('nombre')}<br>Email: {datos_cliente.get('email')}</p><table border='1'>{rows}</table><h3>Total Cash: {total_clp} | Total GC: {total_gc}</h3>"
    msg.attach(MIMEText(html, 'html'))
    try:
        s = smtplib.SMTP('smtp.gmail.com', 587); s.starttls(); s.login(settings.SMTP_EMAIL, settings.SMTP_PASSWORD)
        s.sendmail(settings.SMTP_EMAIL, settings.TARGET_EMAIL, msg.as_string()); s.quit()
        return {"status": "ok"}
    except Exception as e: return {"error": str(e)}

def sincronizar_clientes_jumpseller(db_session: Session, GameCoinUser_Model):
    page = 1; nuevos = 0; actualizados = 0
    while True:
        url = f"{settings.JUMPSELLER_API_BASE}/customers.json"
        try:
            resp = session.get(url, params={"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN, "limit": 50, "page": page}, timeout=20)
            if resp.status_code != 200 or not resp.json(): break
            clientes_api = resp.json(); emails_lote = []
            clientes_map = {c.get("customer", {}).get("email", "").strip().lower(): c.get("customer", {}) for c in clientes_api if c.get("customer", {}).get("email")}
            emails_lote = list(clientes_map.keys())
            if not emails_lote: page += 1; continue
            usuarios_db = db_session.query(GameCoinUser_Model).filter(GameCoinUser_Model.email.in_(emails_lote)).all()
            usuarios_db_map = {u.email: u for u in usuarios_db}
            for email, data in clientes_map.items():
                nom = (data.get('name') or "Cliente").strip()
                ape = (data.get('surname') or "").strip() or "-"
                rut = (data.get("tax_id") or "")
                if not rut:
                    for f in data.get("fields", []):
                        if "rut" in str(f.get("label", "")).lower(): rut = str(f.get("value", "")).strip(); break
                user = usuarios_db_map.get(email)
                if user:
                    chg = False
                    if user.name != nom: user.name = nom; chg = True
                    if user.surname != ape: user.surname = ape; chg = True
                    if rut and ("PENDIENTE" in user.rut or user.rut != rut):
                        if not db_session.query(GameCoinUser_Model).filter(GameCoinUser_Model.rut == rut, GameCoinUser_Model.id != user.id).first():
                            user.rut = rut; chg = True
                    if chg: actualizados += 1
                else:
                    rf = rut if rut else f"PENDIENTE-{email}"
                    if db_session.query(GameCoinUser_Model).filter(GameCoinUser_Model.rut == rf).first(): rf = f"DUP-{rf}-{email}"
                    db_session.add(GameCoinUser_Model(email=email, name=nom, surname=ape, rut=rf)); nuevos += 1
            db_session.commit(); page += 1
        except Exception as e:
            db_session.rollback(); return {"status": "error", "detail": str(e)}
    return {"status": "ok", "nuevos": nuevos, "actualizados": actualizados}
