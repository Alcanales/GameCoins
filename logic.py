import pandas as pd
import requests
import io
import numpy as np
import os
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from concurrent.futures import ThreadPoolExecutor

JUMPSELLER_API_TOKEN = os.environ.get("JUMPSELLER_API_TOKEN", "")
JUMPSELLER_STORE = os.environ.get("JUMPSELLER_STORE", "")
JUMPSELLER_API_BASE = "https://api.jumpseller.com/v1"
SMTP_EMAIL = os.environ.get("SMTP_EMAIL", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
TARGET_EMAIL = "contacto@gamequest.cl"

USD_TO_CLP = 1000
CASH_MULTIPLIER = 0.40
GAMECOIN_MULTIPLIER = 0.50
MIN_PURCHASE_USD = 1.19
STAKE_PRICE_THRESHOLD = 10.0

def normalize_card_name(name):
    if not isinstance(name, str): return ""
    return name.split("|")[0].strip().lower()

def fetch_scryfall_prices(scryfall_ids):
    url = "https://api.scryfall.com/cards/collection"
    unique_ids = [sid for sid in pd.unique(scryfall_ids) if isinstance(sid, str)]
    prices_map = {}
    batch_size = 75
    for i in range(0, len(unique_ids), batch_size):
        batch = unique_ids[i:i+batch_size]
        identifiers = [{"id": sid} for sid in batch]
        try:
            resp = requests.post(url, json={"identifiers": identifiers}, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                for card in data.get("data", []):
                    sid = card.get("id")
                    p = card.get("prices", {})
                    prices_map[sid] = {
                        "usd": float(p["usd"]) if p.get("usd") else None,
                        "usd_foil": float(p["usd_foil"]) if p.get("usd_foil") else None
                    }
        except Exception: continue 
    return prices_map

def get_jumpseller_stock_for_name(name):
    if not name: return 0
    url = f"{JUMPSELLER_API_BASE}/products.json?login={JUMPSELLER_STORE}&authtoken={JUMPSELLER_API_TOKEN}&query={name}&limit=1&fields=stock,name"
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            products = resp.json()
            for p in products:
                if normalize_card_name(p.get("product", {}).get("name", "")) == normalize_card_name(name):
                    return p.get("product", {}).get("stock", 0)
    except: pass
    return 0

def enrich_with_stock(df):
    names = df["name"].unique()
    stock_map = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(get_jumpseller_stock_for_name, names)
        for name, stock in zip(names, results):
            stock_map[name] = stock
    df["stock_tienda"] = df["name"].map(stock_map).fillna(0).astype(int)
    return df

def procesar_csv_manabox(file_content: bytes, internal_mode: bool = False):
    try:
        df = pd.read_csv(io.BytesIO(file_content))
    except Exception: return {"error": "No se pudo leer el CSV."}

    col_map = { 
        "Name": "name", "Set code": "set_code", "Foil": "foil", 
        "Quantity": "quantity", "Purchase price": "purchase_price", 
        "Scryfall ID": "scryfall_id", "ManaBox ID": "manabox_id" 
    }
    df = df.rename(columns=col_map)
    if "name" not in df.columns: return {"error": "Falta columna Name."}

    df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["has_price"] = df["purchase_price"].notna()
    
    if "scryfall_id" in df.columns:
        scryfall_data = fetch_scryfall_prices(df["scryfall_id"])
        
    df["cash_buy_price_clp"] = (df["purchase_price"] * USD_TO_CLP * CASH_MULTIPLIER).fillna(0).apply(lambda x: int(round(x/100.0))*100)
    df["gamecoin_price"] = (df["purchase_price"] * USD_TO_CLP * GAMECOIN_MULTIPLIER).fillna(0).apply(lambda x: int(round(x/100.0))*100)

    if internal_mode:
        df = enrich_with_stock(df)
    else:
        df["stock_tienda"] = 0

    def clasificar(row):
        price = row["purchase_price"]
        is_foil = str(row.get("foil", "")).lower() == "foil"
        if is_foil and price >= STAKE_PRICE_THRESHOLD:
            return "estaca", "Posible Estaca (Foil Caro)"
        if not row["has_price"] or price < MIN_PURCHASE_USD:
            return "no_compra", "Bulk / Bajo Precio"
        return "compra", "Comprar"

    res = df.apply(clasificar, axis=1, result_type="expand")
    df["categoria"], df["buy_decision"] = res[0], res[1]
    df["sort_rank"] = df["categoria"].map({"compra": 1, "estaca": 2, "no_compra": 3})
    
    cols = ["name", "set_code", "foil", "quantity", "purchase_price", "cash_buy_price_clp", "gamecoin_price", "buy_decision", "categoria", "stock_tienda"]
    return df.sort_values(by=["sort_rank", "purchase_price"], ascending=[True, False])[cols].fillna("").to_dict(orient="records")

def enviar_correo_buylist(datos_cliente, lista_cartas, total_clp, total_gc):
    if not SMTP_EMAIL or not SMTP_PASSWORD: return {"error": "Correo no configurado"}

    msg = MIMEMultipart()
    msg['From'] = SMTP_EMAIL
    msg['To'] = TARGET_EMAIL
    msg['Subject'] = f"Buylist Recibida: {datos_cliente.get('nombre')}"

    filas = ""
    for c in lista_cartas:
        filas += f"<tr><td style='border:1px solid #ddd;padding:5px'>{c['quantity']}</td><td style='border:1px solid #ddd;padding:5px'>{c['name']} ({c['set_code']}) {c['foil']}</td><td style='border:1px solid #ddd;padding:5px;text-align:right'>${c['price_unit']}</td><td style='border:1px solid #ddd;padding:5px;text-align:right'>${c['price_total']}</td></tr>"

    html = f"""
    <h2>Solicitud de Venta</h2>
    <p><b>Cliente:</b> {datos_cliente.get('nombre')}<br><b>Email:</b> {datos_cliente.get('email')}<br><b>Fono:</b> {datos_cliente.get('telefono')}<br><b>Mensaje:</b> {datos_cliente.get('comentarios')}</p>
    <hr>
    <table style='width:100%;border-collapse:collapse'>
        <tr style='background:#f0f0f0'><th>Cant</th><th>Carta</th><th>Unit</th><th>Total</th></tr>
        {filas}
    </table>
    <h3>Totales Estimados:<br>Efectivo: {total_clp}<br>GameCoins: {total_gc}</h3>
    """
    msg.attach(MIMEText(html, 'html'))

    try:
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.starttls()
        s.login(SMTP_EMAIL, SMTP_PASSWORD)
        s.sendmail(SMTP_EMAIL, TARGET_EMAIL, msg.as_string())
        s.quit()
        return {"status": "ok"}
    except Exception as e: return {"error": str(e)}

def crear_cupom_jumpseller(codigo, monto):
    url = f"{JUMPSELLER_API_BASE}/promotions.json?login={JUMPSELLER_STORE}&authtoken={JUMPSELLER_API_TOKEN}"
    payload = {
        "promotion": {
            "name": f"Canje GC {codigo}",
            "code": codigo,
            "discount_amount": monto,
            "type": "fix",
            "status": "active",
            "usage_limit": 1,
            "minimum_order_amount": 0
        }
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        return r.status_code in [200, 201]
    except: return False

def sincronizar_clientes_jumpseller(db_session, GameCoinUser_Model):
    url = f"{JUMPSELLER_API_BASE}/customers.json?login={JUMPSELLER_STORE}&authtoken={JUMPSELLER_API_TOKEN}&limit=50"
    nuevos = 0
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            clientes = resp.json()
            for c in clientes:
                email = c.get("customer", {}).get("email", "").strip().lower()
                if email:
                    if not db_session.query(GameCoinUser_Model).filter(GameCoinUser_Model.email == email).first():
                        db_session.add(GameCoinUser_Model(email=email, saldo=0))
                        nuevos += 1
            db_session.commit()
            return {"status": "ok", "nuevos": nuevos}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
    return {"status": "ok", "nuevos": 0}

def actualizar_orden_jumpseller(order_id, estado, notas=""):
    url = f"{JUMPSELLER_API_BASE}/orders/{order_id}.json?login={JUMPSELLER_STORE}&authtoken={JUMPSELLER_API_TOKEN}"
    payload = {"order": {"status": estado, "additional_information": notas}}
    try: requests.put(url, json=payload, timeout=10)
    except: pass
