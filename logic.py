import pandas as pd
import requests
import io
import numpy as np
import os
import re

JUMPSELLER_API_TOKEN = os.environ.get("JUMPSELLER_API_TOKEN", "")
JUMPSELLER_STORE = os.environ.get("JUMPSELLER_STORE", "")
JUMPSELLER_API_BASE = "https://api.jumpseller.com/v1"

USD_TO_CLP = 1000
CASH_MULTIPLIER = 0.40
MIN_PURCHASE_USD = 1.19
STAKE_PRICE_THRESHOLD = 10.0

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

def procesar_csv_manabox(file_content: bytes):
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
        def get_current_price(row):
            sid = row.get("scryfall_id")
            is_foil = str(row.get("foil", "")).lower() == "foil"
            prices = scryfall_data.get(sid, {})
            return prices.get("usd_foil") if is_foil and prices.get("usd_foil") else prices.get("usd")
        df["scryfall_market_price"] = df.apply(get_current_price, axis=1)

    df["cash_buy_price_clp"] = (df["purchase_price"] * USD_TO_CLP * CASH_MULTIPLIER).fillna(0).apply(lambda x: int(round(x/100.0))*100)

    def clasificar(row):
        price = row["purchase_price"]
        is_foil = str(row.get("foil", "")).lower() == "foil"
        if is_foil and price >= STAKE_PRICE_THRESHOLD:
            return "estaca", "Posible Estaca (Foil Caro)"
        if not row["has_price"] or price < MIN_PURCHASE_USD:
            return "no_compra", "No comprar (Bulk/Bajo precio)"
        return "compra", f"Comprar {row['quantity']}"

    res = df.apply(clasificar, axis=1, result_type="expand")
    df["categoria"], df["buy_decision"] = res[0], res[1]
    df["sort_rank"] = df["categoria"].map({"compra": 1, "estaca": 2, "no_compra": 3})
    
    cols = ["name", "set_code", "foil", "quantity", "purchase_price", "cash_buy_price_clp", "buy_decision", "categoria"]
    return df.sort_values(by=["sort_rank", "purchase_price"], ascending=[True, False])[cols].fillna("").to_dict(orient="records")

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