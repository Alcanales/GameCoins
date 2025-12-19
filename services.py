import pandas as pd
import requests
import io
import json
import smtplib
import datetime
import re 
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.orm import Session
from models import GameCoinUser
from config import settings

def normalize_card_name(name):
    """
    Normaliza el nombre para comparar bases.
    Ej: "Sol Ring (Commander) [Foil]" -> "sol ring"
    Ej: "Sol Ring - Magic 2010" -> "sol ring"
    """
    if not isinstance(name, str): return ""
    name = name.split("|")[0]  
    name = name.split("(")[0]  
    name = name.split("[")[0]  
    name = name.split("-")[0]  # CLAVE: Ignora lo que venga después del guion
    return name.strip().lower()

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
        except Exception as e:
            print(f"Error fetching Scryfall batch: {e}")
            continue 
    return prices_map

def get_jumpseller_stock_for_name(name):
    if not name: return 0
    clean_search = normalize_card_name(name)
    
    url = f"{settings.JUMPSELLER_API_BASE}/products.json"
    params = {
        "login": settings.JUMPSELLER_STORE,
        "authtoken": settings.JUMPSELLER_API_TOKEN,
        "query": clean_search,
        "limit": 50, 
        "fields": "stock,name" 
    }
    
    total_stock = 0
    try:
        resp = requests.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            products = resp.json()
            for p in products:
                prod = p.get("product", {})
                prod_name_raw = prod.get("name", "")
                prod_name_clean = normalize_card_name(prod_name_raw)
                
                if prod_name_clean == clean_search:
                    total_stock += prod.get("stock", 0)
    except Exception as e: 
        print(f"Error checking stock for {name}: {e}")
        pass
        
    return total_stock

def crear_cupon_jumpseller(codigo, monto):
    if not monto or int(monto) <= 0: return False
    url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
    params = {"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN}
    
    now = datetime.datetime.now()
    begins = now.strftime('%Y-%m-%d')
    expires = (now + datetime.timedelta(days=365)).strftime('%Y-%m-%d')

    payload = {
        "promotion": {
            "name": f"Canje GameCoins {codigo}",
            "code": codigo,
            "enabled": True,
            "discount_target": "order",
            "type": "fix",
            "discount_amount_fix": monto,
            "minimum_order_amount": 0,
            "begins_at": begins,
            "expires_at": expires,
            "accumulable": False
        }
    }
    
    try:
        r = requests.post(url, params=params, json=payload, timeout=10)
        return r.status_code in [200, 201]
    except Exception as e:
        print(f"❌ Error creando cupón: {str(e)}")
        return False

def actualizar_orden_jumpseller(order_id, estado, notas=""):
    url = f"{settings.JUMPSELLER_API_BASE}/orders/{order_id}.json"
    params = {"login": settings.JUMPSELLER_STORE, "authtoken": settings.JUMPSELLER_API_TOKEN}
    payload = {"order": {"status": estado, "additional_information": notas}}
    try: 
        requests.put(url, params=params, json=payload, timeout=10)
    except: 
        pass

def procesar_csv_manabox(file_content: bytes, internal_mode: bool = False):
    try:
        df = pd.read_csv(io.BytesIO(file_content))
    except Exception: 
        return {"error": "No se pudo leer el CSV. Asegúrate de que sea válido."}

    col_map = { 
        "Name": "name", "Set code": "set_code", "Foil": "foil", 
        "Quantity": "quantity", "Purchase price": "purchase_price", 
        "Scryfall ID": "scryfall_id", "ManaBox ID": "manabox_id" 
    }
    df = df.rename(columns=col_map)
    if "name" not in df.columns: return {"error": "Falta columna 'Name' en el CSV."}

    df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["has_price"] = df["purchase_price"].notna()
    
    if "scryfall_id" in df.columns: 
        df["scryfall_id_prices"] = fetch_scryfall_prices(df["scryfall_id"])
        
    df["cash_buy_price_clp"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.CASH_MULTIPLIER).fillna(0).apply(lambda x: int(round(x/100.0))*100)
    df["gamecoin_price"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.GAMECOIN_MULTIPLIER).fillna(0).apply(lambda x: int(round(x/100.0))*100)

    # Solo si es modo interno consultamos stock
    if internal_mode:
        names = df["name"].unique()
        stock_map = {}
        with ThreadPoolExecutor(max_workers=5) as executor: 
            results = executor.map(get_jumpseller_stock_for_name, names)
            for name, stock in zip(names, results):
                stock_map[name] = stock
        df["stock_tienda"] = df["name"].map(stock_map).fillna(0).astype(int)
    else:
        df["stock_tienda"] = 0

    def clasificar(row):
        price = row["purchase_price"]
        is_foil = str(row.get("foil", "")).lower() == "foil"
        
        if is_foil and price >= settings.STAKE_PRICE_THRESHOLD: 
            return "estaca", "Posible Estaca (Foil Caro)"
        if not row["has_price"] or price < settings.MIN_PURCHASE_USD: 
            return "no_compra", "Bulk / Bajo Precio"
        return "compra", "Comprar"

    res = df.apply(clasificar, axis=1, result_type="expand")
    df["categoria"], df["buy_decision"] = res[0], res[1]
    df["sort_rank"] = df["categoria"].map({"compra": 1, "estaca": 2, "no_compra": 3})
    
    cols = ["name", "set_code", "foil", "quantity", "purchase_price", "cash_buy_price_clp", "gamecoin_price", "buy_decision", "categoria", "stock_tienda"]
    
    final_cols = [c for c in cols if c in df.columns]
    
    return df.sort_values(by=["sort_rank", "purchase_price"], ascending=[True, False])[final_cols].fillna("").to_dict(orient="records")

def enviar_correo_buylist(datos_cliente, lista_cartas, total_clp, total_gc):
    if not settings.SMTP_EMAIL or not settings.SMTP_PASSWORD: return {"error": "Correo no configurado"}
    
    msg = MIMEMultipart()
    msg['From'] = settings.SMTP_EMAIL
    msg['To'] = settings.TARGET_EMAIL
    msg['Subject'] = f"Buylist Recibida: {datos_cliente.get('nombre')}"
    
    filas = ""
    for c in lista_cartas: 
        filas += f"<tr><td>{c.get('quantity')}</td><td>{c.get('name')} ({c.get('set_code')})</td><td>${c.get('price_unit')}</td><td>${c.get('price_total')}</td></tr>"

    html = f"""
    <h2>Solicitud de Venta</h2>
    <p><strong>Cliente:</strong> {datos_cliente.get('nombre')}<br>
    <strong>Email:</strong> {datos_cliente.get('email')}<br>
    <strong>Método Pago:</strong> {datos_cliente.get('metodo_pago', 'No especificado')}</p>
    <table border="1" style="border-collapse: collapse; width: 100%;">
        <tr style="background-color: #f2f2f2;"><th>Cant</th><th>Carta</th><th>Unit</th><th>Total</th></tr>
        {filas}
    </table>
    <h3>Total Cash: {total_clp} | Total GC: {total_gc}</h3>
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

def sincronizar_clientes_jumpseller(db_session: Session, GameCoinUser_Model):
    page = 1
    nuevos = 0
    actualizados = 0
    
    print("--- 🔄 INICIANDO SYNC OPTIMIZADO ---")
    
    while True:
        url = f"{settings.JUMPSELLER_API_BASE}/customers.json"
        params = {
            "login": settings.JUMPSELLER_STORE,
            "authtoken": settings.JUMPSELLER_API_TOKEN,
            "limit": 50,
            "page": page
        }
        
        try:
            resp = requests.get(url, params=params, timeout=20)
            if resp.status_code != 200: break
            clientes_api = resp.json()
            if not clientes_api: break
            
            emails_lote = []
            clientes_map = {}
            
            for c in clientes_api:
                cust_data = c.get("customer", {})
                email = cust_data.get("email", "").strip().lower()
                if email:
                    emails_lote.append(email)
                    clientes_map[email] = cust_data

            if not emails_lote:
                page += 1
                continue

            usuarios_db = db_session.query(GameCoinUser_Model).filter(GameCoinUser_Model.email.in_(emails_lote)).all()
            usuarios_db_map = {u.email: u for u in usuarios_db}

            for email, data_api in clientes_map.items():
                bill = data_api.get("billing_address") or {}
                ship = data_api.get("shipping_address") or {}
                
                nombre = (data_api.get('name') or bill.get('name') or ship.get('name') or "Cliente").strip()
                apellido = (data_api.get('surname') or bill.get('surname') or ship.get('surname') or "").strip()
                if not apellido: apellido = "-"

                rut = (data_api.get("tax_id") or data_api.get("taxid") or 
                       bill.get("taxid") or bill.get("tax_id") or 
                       ship.get("taxid") or ship.get("tax_id") or "")
                
                if not rut and "fields" in data_api:
                    for field in data_api.get("fields", []) or []:
                        if "rut" in str(field.get("label", "")).lower():
                            rut = str(field.get("value", "")).strip()
                            break

                user = usuarios_db_map.get(email)
                
                if user:
                    cambios = False
                    if nombre and nombre != "Cliente" and user.name != nombre:
                        user.name = nombre; cambios = True
                    if apellido and apellido != "-" and user.surname != apellido:
                        user.surname = apellido; cambios = True
                    
                    if rut:
                        if "PENDIENTE" in user.rut or (user.rut != rut):
                            rut_existe = db_session.query(GameCoinUser_Model).filter(
                                GameCoinUser_Model.rut == rut, 
                                GameCoinUser_Model.id != user.id
                            ).first()
                            if not rut_existe:
                                user.rut = rut; cambios = True
                    
                    if cambios: actualizados += 1
                else:
                    rut_final = rut if rut else f"PENDIENTE-{email}"
                    rut_ocupado = db_session.query(GameCoinUser_Model).filter(GameCoinUser_Model.rut == rut_final).first()
                    if rut_ocupado: rut_final = f"DUP-{rut_final}-{email}"

                    new_user = GameCoinUser_Model(
                        email=email, saldo=0, name=nombre, surname=apellido, rut=rut_final
                    )
                    db_session.add(new_user)
                    nuevos += 1
            
            db_session.commit()
            print(f"✅ Página {page} procesada. Nuevos: {nuevos} | Act: {actualizados}")
            page += 1
            
        except Exception as e:
            print(f"❌ Error Critical Sync Page {page}: {str(e)}")
            db_session.rollback()
            return {"status": "error", "detail": str(e)}
            
    return {"status": "ok", "nuevos": nuevos, "actualizados": actualizados}
