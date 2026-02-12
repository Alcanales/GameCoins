# GameCoins/services.py
import pandas as pd
import io
from .models import PriceCache

async def analizar_manabox_ck(content: bytes, db):
    df = pd.read_csv(io.BytesIO(content))
    results = []
    tasa_dolar = 950 # Puedes mover esto a settings o SystemConfig
    
    for _, row in df.iterrows():
        s_id = str(row.get('Scryfall ID'))
        name = row.get('Name')
        precio_ck = row.get('Purchase price') # Valor CK en el CSV de ManaBox

        # Manejo de Caché
        if pd.notnull(precio_ck) and precio_ck > 0:
            cache = db.query(PriceCache).filter(PriceCache.scryfall_id == s_id).first()
            if cache: cache.price_usd = precio_ck
            else: db.add(PriceCache(scryfall_id=s_id, name=name, price_usd=precio_ck))
            valor_ck = precio_ck
        else:
            cache = db.query(PriceCache).filter(PriceCache.scryfall_id == s_id).first()
            valor_ck = cache.price_usd if cache else 0

        # Tu regla de negocio: Compras al 50% del valor CK
        oferta_clp = int(valor_ck * tasa_dolar * 0.5)

        results.append({
            "name": name,
            "qty": int(row.get('Quantity', 1)),
            "price_usd": valor_ck,
            "offer_clp": oferta_clp,
            "condition": row.get('Condition', 'NM')
        })
    
    db.commit()
    return results