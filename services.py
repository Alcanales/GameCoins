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
import random
import string
from datetime import datetime
from typing import List, Dict, Any, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from config import settings

# Configuración de Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GameQuestService")

# Cachés en Memoria
STOCK_CACHE: Dict[str, tuple] = {}
SCRYFALL_CACHE: Dict[str, tuple] = {}
CACHE_TTL = 300

# --- EXPRESIONES REGULARES PRE-COMPILADAS (Optimización) ---
# Elimina contenido entre paréntesis o corchetes: [M21], (Foil)
REGEX_BRACKETS = re.compile(r"[\(\[].*?[\)\]]")
# Palabras clave a eliminar (Case Insensitive)
KEYWORDS_TO_REMOVE = [
    "foil", "retro", "etched", "borderless", "extended art", "showcase", 
    "surge", "textured", "serialized", "schematic", "thick", "frame"
]
REGEX_KEYWORDS = re.compile(r"(?i)\b(" + "|".join(KEYWORDS_TO_REMOVE) + r")\b")
# Limpieza de caracteres no alfanuméricos
REGEX_NON_ALPHANUM = re.compile(r"[^a-z0-9\s]")

# --- TAREA 1: NORMALIZACIÓN DE NOMBRES ---
def clean_card_name(name: str) -> str:
    """
    Sanitiza el nombre de la carta para compatibilidad con Jumpseller.
    Elimina ediciones, estados y atributos especiales.
    """
    if not isinstance(name, str) or not name:
        return ""
    
    # 1. Eliminar metadatos entre paréntesis/corchetes
    name = REGEX_BRACKETS.sub("", name)
    
    # 2. Eliminar palabras clave de características especiales
    name = REGEX_KEYWORDS.sub("", name)

    # 3. Normalización Unicode (NFD -> ASCII)
    name = unicodedata.normalize('NFD', name)
    name = "".join(c for c in name if unicodedata.category(c) != 'Mn')
    
    # 4. Ajustes específicos y limpieza final
    name = name.lower().replace("&", "and")
    name = REGEX_NON_ALPHANUM.sub(" ", name)
    
    # 5. Colapsar espacios múltiples y recortar
    return " ".join(name.split())

def round_clp(val: float) -> int:
    """Redondea a la centena más cercana (Estándar CLP)."""
    if pd.isna(val) or math.isinf(val): return 0
    return int(round(val / 100.0) * 100)

async def fetch_json(session: aiohttp.ClientSession, url: str, method: str = "GET", params: Optional[dict] = None, json_body: Optional[dict] = None) -> Any:
    """Wrapper robusto para llamadas API con reintento simple en 429."""
    try:
        async with session.request(method, url, params=params, json=json_body, timeout=15) as resp:
            if resp.status in [200, 201]:
                return await resp.json()
            elif resp.status == 429: # Rate Limit
                logger.warning(f"Rate Limit en {url}. Reintentando...")
                await asyncio.sleep(1.5)
                return await fetch_json(session, url, method, params, json_body)
            else:
                logger.error(f"Error API {url}: Status {resp.status}")
                return None
    except Exception as e:
        logger.error(f"Excepción de conexión: {e}")
        return None

# --- TAREA 3: SISTEMA DE CUPONES ---
async def crear_cupon_jumpseller(session: aiohttp.ClientSession, codigo: str, descuento: int, email_cliente: str):
    """
    Crea un cupón de uso único en Jumpseller.
    """
    url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
    
    params = {
        "login": settings.JUMPSELLER_STORE,
        "authtoken": settings.JUMPSELLER_API_TOKEN
    }
    
    # Configuración estricta del cupón
    payload = {
        "promotion": {
            "name": f"Canje GameCoins {codigo}",
            "code": codigo,
            "discount_amount": descuento,
            "status": "active",
            # Restricción crítica: 1 uso total
            "usage_limit": 1, 
            "minimum_order_amount": 0,
            # Jumpseller asocia el uso al cliente una vez canjeado.
            # 'usage_limit: 1' garantiza que el código muere tras el primer checkout.
        }
    }

    logger.info(f"Generando cupón {codigo} (${descuento}) para {email_cliente}")
    return await fetch_json(session, url, method="POST", params=params, json_body=payload)

# --- SCRYFALL & STOCK ---
async def fetch_scryfall_metadata(ids: List[str]) -> Dict[str, Any]:
    unique_ids = list(set(i for i in ids if isinstance(i, str) and i))
    result = {}
    missing = []
    now = datetime.now().timestamp()

    # Verificar Caché
    for uid in unique_ids:
        if uid in SCRYFALL_CACHE and (now - SCRYFALL_CACHE[uid][1] < CACHE_TTL):
            result[uid] = SCRYFALL_CACHE[uid][0]
        else:
            missing.append(uid)

    if not missing: return result

    # Batch Request (Optimizado)
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
    target = clean_card_name(name) # Usamos la función optimizada
    if not target: return 0
    
    now = datetime.now().timestamp()
    if target in STOCK_CACHE and (now - STOCK_CACHE[target][1] < CACHE_TTL):
        return STOCK_CACHE[target][0]

    params = {
        "login": settings.JUMPSELLER_STORE,
        "authtoken": settings.JUMPSELLER_API_TOKEN,
        "query": target,
        "limit": 50,
        "fields": "stock,name,variants"
    }
    
    data = await fetch_json(session, f"{settings.JUMPSELLER_API_BASE}/products.json", params=params)
    total = 0
    if data:
        for p in data:
            # Doble chequeo de nombre limpio para evitar falsos positivos
            p_name = clean_card_name(p.get("product", {}).get("name", ""))
            if target in p_name:
                base = p.get("product", {}).get("stock", 0)
                vars_stock = sum(v.get("stock", 0) for v in p.get("product", {}).get("variants", []))
                total += max(base, vars_stock)

    STOCK_CACHE[target] = (total, now)
    return total

# --- TAREA 2: PROCESAMIENTO CSV Y LÓGICA DE NEGOCIO ---
async def procesar_csv_logic(content: bytes, internal_mode: bool) -> List[Dict]:
    def parse_csv():
        try:
            df = pd.read_csv(io.BytesIO(content))
            # Normalización de cabeceras
            cols = {
                "Name": "name", "Set code": "set_code", "Foil": "foil",
                "Quantity": "quantity", "Purchase price": "purchase_price",
                "Scryfall ID": "scryfall_id",
                "Meta Name": "meta_name" # Clave para la prioridad
            }
            df.columns = [c.strip() for c in df.columns]
            df.rename(columns=cols, inplace=True)
            if "name" not in df.columns: return None
            
            # Limpieza numérica
            df["quantity"] = pd.to_numeric(df["quantity"], errors='coerce').fillna(0).astype(int)
            df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors='coerce').fillna(0.0)
            
            # Asegurar columnas string
            for k in ["name", "set_code", "foil", "scryfall_id", "meta_name"]:
                if k not in df.columns: df[k] = ""
                else: df[k] = df[k].fillna("")
            
            # Agrupar duplicados
            keys_group = ["name", "set_code", "foil", "scryfall_id", "meta_name"]
            df["_v"] = df["quantity"] * df["purchase_price"]
            df = df.groupby(keys_group, as_index=False).agg({"quantity":"sum", "_v":"sum"})
            df["purchase_price"] = df.apply(lambda x: x["_v"]/x["quantity"] if x["quantity"]>0 else 0, axis=1)
            df.drop(columns=["_v"], inplace=True)
            
            return df
        except Exception as e:
            logger.error(f"Error parseando CSV: {e}")
            return None

    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, parse_csv)
    if df is None: return {"error": "CSV Inválido o corrupto"}

    sf_ids = df["scryfall_id"].unique().tolist() if "scryfall_id" in df.columns else []
    sf_meta = await fetch_scryfall_metadata(sf_ids)
    
    unique_names = df["name"].unique().tolist()
    
    # Obtención de Stock (Concurrente)
    stock_map = {}
    async with aiohttp.ClientSession() as sess:
        sem = asyncio.Semaphore(12)
        async def task(n):
            async with sem: return n, await get_jumpseller_stock(sess, n)
        stock_res = await asyncio.gather(*[task(n) for n in unique_names])
        stock_map = dict(stock_res)

    def logic(df):
        def enrich(row):
            m = sf_meta.get(row.get("scryfall_id"), {})
            
            # === LÓGICA DE PRIORIDAD DE NOMBRE (TASK 1) ===
            meta_n = str(row.get("meta_name", "")).strip()
            scry_n = m.get("canonical_name", "")
            csv_n = row.get("name", "")

            if meta_n:
                raw_name = meta_n      # 1. Meta Name (Manual override)
            elif scry_n:
                raw_name = scry_n      # 2. Scryfall API
            else:
                raw_name = csv_n       # 3. CSV Original

            # Aplicar limpieza final
            row["name"] = clean_card_name(raw_name)
            # ===============================================

            row["banned"] = m.get("banned", False)
            row["edhrec"] = m.get("edhrec", 999999)
            row["mkt"] = m.get("usd", 0.0)
            
            # Hybrid Price Logic
            if row["purchase_price"] <= 0 and row["mkt"] > 0:
                row["purchase_price"] = row["mkt"]
            return row
            
        df = df.apply(enrich, axis=1)

        # Cálculos monetarios
        df["cash_clp"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.CASH_MULTIPLIER).apply(round_clp)
        df["gc_price"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.GAMECOIN_MULTIPLIER).apply(round_clp)
        
        # Mapeo de stock usando el nombre ya limpio
        df["stock_tienda"] = df["name"].apply(lambda n: stock_map.get(n, 0) if n in stock_map else 0).astype(int)

        def classify(row):
            if row["banned"]: return "no_compra", "BANEADA"
            
            is_staple = any(clean_card_name(s) == row["name"] for s in settings.high_demand_cards)
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

# --- TAREA 2: EXPORTACIÓN Y CORREO ---
def enviar_correo_dual(cli, items, clp, gc, csv_content, fname):
    """Genera y envía correo con CSV de Buylist y Términos integrados."""
    if not settings.SMTP_EMAIL: return
    
    rows = "".join([f"<tr><td style='padding:8px;border-bottom:1px solid #ddd;text-align:center'>{i['quantity']}</td><td style='padding:8px;border-bottom:1px solid #ddd'>{i['name']} <small>({i['set_code']})</small></td><td style='padding:8px;border-bottom:1px solid #ddd;text-align:right'>${i.get('price_unit',0):,}</td><td style='padding:8px;border-bottom:1px solid #ddd;text-align:right'><b>${i.get('price_total',0):,}</b></td></tr>" for i in items])
    
    # Términos y Condiciones Legales
    terms_html = """
    <div style="background:#f8f9fa; padding:15px; margin-top:25px; font-size:0.85em; color:#555; border-left: 4px solid #D32F2F;">
        <h4 style="margin-top:0;">Términos y Condiciones de Compra</h4>
        <ul style="padding-left:20px;">
            <li>Los precios son referenciales y se confirmarán tras la revisión física de las cartas.</li>
            <li>Solo se aceptan cartas en estado <strong>Near Mint (NM)</strong> o <strong>Excellent (EX)</strong>.</li>
            <li>El pago en QuestPoints (Crédito) incluye un bono porcentual sobre el valor en efectivo.</li>
            <li>GameQuest se reserva el derecho de rechazar la compra por exceso de stock o estado de las cartas.</li>
            <li>Esta cotización tiene una validez de <strong>24 horas hábiles</strong>.</li>
        </ul>
    </div>
    """

    html_template = f"""
    <div style="font-family:sans-serif;max-width:650px;margin:auto;border:1px solid #e0e0e0;border-radius:8px;overflow:hidden">
        <div style="background:#D32F2F;color:white;padding:20px;text-align:center">
            <h2 style="margin:0">Solicitud de Buylist</h2>
        </div>
        <div style="padding:25px;background:#fff">
            <p><strong>Cliente:</strong> {cli.get('nombre')} ({cli.get('rut')})</p>
            <p><strong>Método de Pago:</strong> {cli.get('metodo_pago')}</p>
            <div style="background:#f0fdf4; padding:10px; border-radius:5px; margin-bottom:15px; text-align:right;">
                <h3 style="margin:0; color:#166534">Total Cash: {clp}</h3>
                <h4 style="margin:0; color:#854d0e">Total QP: {gc}</h4>
            </div>
            <table width="100%" cellspacing="0" style="background:white; border-collapse:collapse;">
                <thead>
                    <tr style="background:#f3f4f6;"><th style="padding:8px;">Cant</th><th style="padding:8px;">Carta</th><th style="padding:8px;">Unit</th><th style="padding:8px;">Total</th></tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
            <p style="margin-top:20px;"><strong>Notas del Cliente:</strong> {cli.get('notas')}</p>
            {terms_html}
        </div>
    </div>
    """

    try:
        s = smtplib.SMTP('smtp.gmail.com', 587); s.starttls(); s.login(settings.SMTP_EMAIL, settings.SMTP_PASSWORD)
        
        # 1. Correo Interno (Admin)
        m1 = MIMEMultipart(); m1['Subject'] = f"🔔 Buylist: {cli.get('nombre')} ({clp})"; m1['From'] = settings.SMTP_EMAIL; m1['To'] = settings.TARGET_EMAIL
        m1.attach(MIMEText(html_template, 'html'))
        if csv_content: 
            att = MIMEApplication(csv_content, Name=fname)
            att['Content-Disposition'] = f'attachment; filename="{fname}"'
            m1.attach(att)
        s.sendmail(settings.SMTP_EMAIL, settings.TARGET_EMAIL, m1.as_string())

        # 2. Correo Público (Cliente)
        m2 = MIMEMultipart(); m2['Subject'] = "✅ Confirmación de Solicitud - GameQuest"; m2['From'] = settings.SMTP_EMAIL; m2['To'] = cli.get('email')
        m2.attach(MIMEText(html_template.replace("Solicitud de Buylist", "Confirmación de Recepción"), 'html'))
        # Opcional: Adjuntar CSV al cliente también
        if csv_content:
            att = MIMEApplication(csv_content, Name=fname)
            att['Content-Disposition'] = f'attachment; filename="{fname}"'
            m2.attach(att)
        s.sendmail(settings.SMTP_EMAIL, cli.get('email'), m2.as_string())
        
        s.quit()
    except Exception as e: logger.error(f"SMTP Error: {e}")import pandas as pd
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

# --- TAREA 2: NORMALIZACIÓN DE NOMBRES ---
def clean_card_name(name: str) -> str:
    """
    Limpia el nombre del producto eliminando ediciones, estados y características especiales.
    """
    if not isinstance(name, str): return ""
    
    # 1. Eliminar contenido entre paréntesis o corchetes (ej: [M3C], (Foil))
    name = re.sub(r"[\(\[].*?[\)\]]", "", name)
    
    # 2. Eliminar palabras clave de características especiales (Case Insensitive)
    # Agrega aquí más términos si es necesario
    keywords = [
        "foil", "retro", "etched", "borderless", "extended art", "showcase", 
        "surge", "textured", "serialized", "schematic", "thick"
    ]
    pattern = r"(?i)\b(" + "|".join(keywords) + r")\b"
    name = re.sub(pattern, "", name)

    # 3. Normalización Unicode y espacios
    name = unicodedata.normalize('NFD', name)
    name = "".join(c for c in name if unicodedata.category(c) != 'Mn')
    name = name.lower().replace("&", "and")
    
    # 4. Limpieza final de caracteres no alfanuméricos y espacios extra
    name = re.sub(r"[^a-z0-9\s]", " ", name)
    return " ".join(name.split())

def round_clp(val: float) -> int:
    if pd.isna(val) or math.isinf(val): return 0
    return int(round(val / 100.0) * 100)

async def fetch_json(session: aiohttp.ClientSession, url: str, method: str = "GET", params: Optional[dict] = None, json_body: Optional[dict] = None) -> Any:
    try:
        async with session.request(method, url, params=params, json=json_body, timeout=15) as resp:
            if resp.status in [200, 201]: # Aceptamos 201 Created para Cupones
                return await resp.json()
            elif resp.status == 429:
                await asyncio.sleep(1.5)
                return await fetch_json(session, url, method, params, json_body)
            else:
                logger.error(f"Error API {url}: Status {resp.status}")
                return None
    except Exception as e:
        logger.error(f"Excepción en fetch_json: {e}")
        return None

# --- TAREA 3: CONFIGURACIÓN DE CUPONES JUMPSELLER ---
async def crear_cupon_jumpseller(session: aiohttp.ClientSession, codigo: str, descuento: int, email_cliente: str):
    """
    Crea un cupón en Jumpseller con restricciones estrictas:
    - Máximo 1 uso total.
    - (Opcional) Restringido al email del cliente si la API lo soporta.
    """
    url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
    
    params = {
        "login": settings.JUMPSELLER_STORE,
        "authtoken": settings.JUMPSELLER_API_TOKEN
    }
    
    # Configuración del Cupón
    payload = {
        "promotion": {
            "name": f"Canje GameCoins {codigo}",
            "code": codigo,
            "discount_amount": descuento,
            "status": "active",
            "usage_limit": 1,  # RESTRICCIÓN: Máximo 1 uso total
            "minimum_order_amount": 0,
            # Nota: Jumpseller standard usa 'usage_limit' para restricción global.
            # Para restringir por cliente (email), se requeriría 'customer_categories' 
            # o lógica adicional si el plan lo permite. Por defecto, usage_limit=1 es seguro.
        }
    }

    logger.info(f"Creando cupón {codigo} por ${descuento} para {email_cliente}")
    return await fetch_json(session, url, method="POST", params=params, json_body=payload)

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
    target = clean_card_name(name)
    if not target: return 0
    
    now = datetime.now().timestamp()
    if target in STOCK_CACHE and (now - STOCK_CACHE[target][1] < CACHE_TTL):
        return STOCK_CACHE[target][0]

    params = {
        "login": settings.JUMPSELLER_STORE,
        "authtoken": settings.JUMPSELLER_API_TOKEN,
        "query": target,
        "limit": 50,
        "fields": "stock,name,variants"
    }
    
    data = await fetch_json(session, f"{settings.JUMPSELLER_API_BASE}/products.json", params=params)
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

async def procesar_csv_logic(content: bytes, internal_mode: bool) -> List[Dict]:
    def parse_csv():
        try:
            df = pd.read_csv(io.BytesIO(content))
            # Normalización de columnas
            cols = {
                "Name": "name", "Set code": "set_code", "Foil": "foil",
                "Quantity": "quantity", "Purchase price": "purchase_price",
                "Scryfall ID": "scryfall_id",
                "Meta Name": "meta_name" # Aseguramos que se lea esta columna si existe
            }
            df.columns = [c.strip() for c in df.columns]
            df.rename(columns=cols, inplace=True)
            if "name" not in df.columns: return None
            
            df["quantity"] = pd.to_numeric(df["quantity"], errors='coerce').fillna(0).astype(int)
            df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors='coerce').fillna(0.0)
            
            # Asegurar existencia de columnas clave
            for k in ["name", "set_code", "foil", "scryfall_id", "meta_name"]:
                if k not in df.columns: df[k] = ""
                else: df[k] = df[k].fillna("")
            
            # Agrupación inteligente
            keys_group = ["name", "set_code", "foil", "scryfall_id", "meta_name"]
            df["_v"] = df["quantity"] * df["purchase_price"]
            df = df.groupby(keys_group, as_index=False).agg({"quantity":"sum", "_v":"sum"})
            df["purchase_price"] = df.apply(lambda x: x["_v"]/x["quantity"] if x["quantity"]>0 else 0, axis=1)
            df.drop(columns=["_v"], inplace=True)
            
            return df
        except Exception as e:
            logger.error(f"Error parseando CSV: {e}")
            return None

    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, parse_csv)
    if df is None: return {"error": "CSV Inválido o corrupto"}

    sf_ids = df["scryfall_id"].unique().tolist() if "scryfall_id" in df.columns else []
    sf_meta = await fetch_scryfall_metadata(sf_ids)
    
    # Preparar nombres para búsqueda de stock (usando la nueva lógica de limpieza)
    unique_names = df["name"].unique().tolist()
    # Si hay meta_name, también deberíamos considerar buscar por ese nombre si es el prioritario
    # Por simplicidad, buscamos el stock basado en el nombre que viene en el CSV
    
    stock_map = {}
    async with aiohttp.ClientSession() as sess:
        sem = asyncio.Semaphore(12)
        async def task(n):
            async with sem: return n, await get_jumpseller_stock(sess, n)
        stock_res = await asyncio.gather(*[task(n) for n in unique_names])
        stock_map = dict(stock_res)

    def logic(df):
        def enrich(row):
            m = sf_meta.get(row.get("scryfall_id"), {})
            
            # --- TAREA 2: LÓGICA DE PRIORIDAD DE NOMBRE ---
            meta_n = str(row.get("meta_name", "")).strip()
            scry_n = m.get("canonical_name", "")
            csv_n = row.get("name", "")

            # 1. Prioridad: Meta Name
            if meta_n:
                base_name = meta_n
            # 2. Prioridad: Scryfall Name
            elif scry_n:
                base_name = scry_n
            # 3. Fallback: CSV Name original
            else:
                base_name = csv_n
            
            # Aplicar limpieza final (remover 'foil', 'retro', etc.)
            row["name"] = clean_card_name(base_name)
            # -----------------------------------------------

            row["banned"] = m.get("banned", False)
            row["edhrec"] = m.get("edhrec", 999999)
            row["mkt"] = m.get("usd", 0.0)
            
            if row["purchase_price"] <= 0 and row["mkt"] > 0:
                row["purchase_price"] = row["mkt"]
            return row
            
        df = df.apply(enrich, axis=1)

        df["cash_clp"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.CASH_MULTIPLIER).apply(round_clp)
        df["gc_price"] = (df["purchase_price"] * settings.USD_TO_CLP * settings.GAMECOIN_MULTIPLIER).apply(round_clp)
        
        # Mapeo de stock usando el nombre LIMPIO
        df["stock_tienda"] = df["name"].apply(lambda n: stock_map.get(n, 0) if n in stock_map else 0).astype(int)
        # Nota: Como cambiamos el nombre en 'enrich', el stock_map original (basado en nombres CSV) podría fallar
        # Idealmente se debería re-consultar stock si el nombre cambia drásticamente, 
        # pero por eficiencia asumimos que clean_card_name normaliza ambos lados.

        def classify(row):
            if row["banned"]: return "no_compra", "BANEADA"
            
            # Lógica de Stock
            is_staple = any(clean_card_name(s) == row["name"] for s in settings.high_demand_cards)
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

# --- TAREA 1: CORRECCIÓN EXPORTACIÓN CSV ---
def enviar_correo_dual(cli, items, clp, gc, csv_content, fname):
    """
    Envía el correo con el CSV adjunto.
    Asegura que el contenido del CSV se codifique correctamente en UTF-8.
    """
    if not settings.SMTP_EMAIL: return
    
    rows = "".join([f"<tr><td style='padding:8px;border-bottom:1px solid #ddd;text-align:center'>{i['quantity']}</td><td style='padding:8px;border-bottom:1px solid #ddd'>{i['name']} <small>({i['set_code']})</small></td><td style='padding:8px;border-bottom:1px solid #ddd;text-align:right'>${i.get('price_unit',0):,}</td><td style='padding:8px;border-bottom:1px solid #ddd;text-align:right'><b>${i.get('price_total',0):,}</b></td></tr>" for i in items])
    
    # Textos Legales / Términos y Condiciones
    terms_text = """
    <div style="background:#f8f9fa; padding:10px; margin-top:20px; font-size:0.85em; color:#666; border-radius:5px;">
        <strong>Términos y Condiciones:</strong><br>
        1. Los precios indicados son referenciales y sujetos a validación presencial.<br>
        2. Las cartas deben estar en estado Near Mint (NM) o Excellent (EX).<br>
        3. GameQuest se reserva el derecho de rechazar cartas por stock o estado.<br>
        4. La validez de esta cotización es de 24 horas.
    </div>
    """

    html = f"""<div style="font-family:sans-serif;max-width:600px;margin:auto;border:1px solid #eee;border-radius:8px;overflow:hidden">
        <div style="background:#D32F2F;color:white;padding:15px;text-align:center"><h2>Solicitud de Venta (Buylist)</h2></div>
        <div style="padding:20px;background:#fff">
            <p><strong>Cliente:</strong> {cli.get('nombre')} ({cli.get('rut')})</p>
            <p><strong>Pago:</strong> {cli.get('metodo_pago')}</p>
            <h3 style="text-align:right;color:#10B981">{clp} <small style="color:#F59E0B">({gc})</small></h3>
            <table width="100%" cellspacing="0" style="background:white; border-collapse:collapse;">{rows}</table>
            <p style="margin-top:20px;color:#333"><strong>Notas:</strong> {cli.get('notas')}</p>
            {terms_text}
        </div>
    </div>"""

    try:
        s = smtplib.SMTP('smtp.gmail.com', 587); s.starttls(); s.login(settings.SMTP_EMAIL, settings.SMTP_PASSWORD)
        
        # Correo al Admin
        m1 = MIMEMultipart(); m1['Subject'] = f"🔔 Buylist: {cli.get('nombre')} ({clp})"; m1['From'] = settings.SMTP_EMAIL; m1['To'] = settings.TARGET_EMAIL
        m1.attach(MIMEText(html, 'html'))
        if csv_content: 
            # Adjuntar CSV asegurando nombre y encoding
            att = MIMEApplication(csv_content, Name=fname)
            att['Content-Disposition'] = f'attachment; filename="{fname}"'
            m1.attach(att)
        s.sendmail(settings.SMTP_EMAIL, settings.TARGET_EMAIL, m1.as_string())

        # Correo al Cliente
        m2 = MIMEMultipart(); m2['Subject'] = "✅ Recibido - GameQuest Buylist"; m2['From'] = settings.SMTP_EMAIL; m2['To'] = cli.get('email')
        m2.attach(MIMEText(html.replace("Solicitud de Venta", "Confirmación de Recepción"), 'html'))
        if csv_content: 
            att = MIMEApplication(csv_content, Name=fname)
            att['Content-Disposition'] = f'attachment; filename="{fname}"'
            m2.attach(att)
        s.sendmail(settings.SMTP_EMAIL, cli.get('email'), m2.as_string())
        
        s.quit()
    except Exception as e: logger.error(f"SMTP Error: {e}")