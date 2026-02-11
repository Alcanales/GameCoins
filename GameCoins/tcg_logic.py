import pandas as pd
import aiohttp
import asyncio
from io import BytesIO
from .config import settings

async def fetch_scryfall_prices(session, scryfall_id):
    """Obtiene precios de referencia de Scryfall."""
    if not scryfall_id or pd.isna(scryfall_id):
        return 0.0, 0.0
    url = f"https://api.scryfall.com/cards/{scryfall_id}"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                prices = data.get("prices", {})
                return float(prices.get("usd") or 0), float(prices.get("usd_foil") or 0)
    except:
        pass
    return 0.0, 0.0

async def analizar_csv_simple(file_content: bytes):
    try:
        df = pd.read_csv(BytesIO(file_content))
        # Normalizar columnas (Purchase price -> purchase_price)
        df.columns = [str(c).lower().strip().replace(' ', '_') for c in df.columns]
        
        # Detectar columnas clave
        has_csv_price = 'purchase_price' in df.columns
        has_scryfall = 'scryfall_id' in df.columns
        
        res = []
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            if has_scryfall:
                for _, row in df.iterrows():
                    tasks.append(fetch_scryfall_prices(session, row.get('scryfall_id')))
                scryfall_data = await asyncio.gather(*tasks)
            else:
                # Si no hay ID de Scryfall, asumimos ceros
                scryfall_data = [(0.0, 0.0)] * len(df)

        for i, (_, row) in enumerate(df.iterrows()):
            # 1. Precios de Referencia (Scryfall)
            sf_pn, sf_pf = scryfall_data[i]
            
            # 2. Precio del CSV (Prioridad del Usuario)
            csv_price = float(row.get('purchase_price', 0)) if has_csv_price else 0.0
            
            # 3. Determinar acabado de la carta física
            is_foil = str(row.get('foil', '')).lower() == 'foil'
            
            # 4. ASIGNACIÓN DE PRECIOS (Lógica de Prioridad)
            if is_foil:
                # Si es foil, el precio del CSV es el precio Foil
                pf = csv_price if csv_price > 0 else sf_pf
                pn = sf_pn # Siempre usamos Scryfall para la normal (referencia)
            else:
                # Si es normal, el precio del CSV es el precio Normal
                pn = csv_price if csv_price > 0 else sf_pn
                pf = sf_pf
            
            # 5. Análisis de Riesgo (Estaca)
            status = "APROBADO"
            
            if is_foil:
                # Comparamos PF (que puede venir del CSV) vs PN (de Scryfall)
                if pf >= 20.0 and pn < 20.0:
                    ratio = pf / pn if pn > 0 else 999
                    if ratio > settings.STAKE_RATIO_THRESHOLD and (pf - pn) > settings.STAKE_DIFF_THRESHOLD:
                        status = "RECHAZADO (ESTACA)"
                
                # Excepción High End: Si la base (Scryfall) vale más de $20, se aprueba
                if pn >= 20.0:
                    status = "HIGH END"

            res.append({
                "name": str(row.get('name', 'Carta Desconocida')),
                "price_normal": pn,
                "price_foil": pf,
                "cash_normal": round(pn * settings.CASH_MULTIPLIER * settings.USD_TO_CLP),
                "gc_normal": round(pn * settings.GAMECOIN_MULTIPLIER * settings.USD_TO_CLP),
                "cash_foil": round(pf * settings.CASH_MULTIPLIER * settings.USD_TO_CLP),
                "gc_foil": round(pf * settings.GAMECOIN_MULTIPLIER * settings.USD_TO_CLP),
                "status": status,
                # Metadata para debug o comparativa visual si se requiere
                "source_price": "CSV" if csv_price > 0 else "Scryfall",
                "scryfall_ref": sf_pf if is_foil else sf_pn
            })
            
        df_res = pd.DataFrame(res)
        df_res['rank'] = df_res['status'].apply(lambda s: 0 if "HIGH" in s else (1 if "APRO" in s else 2))
        return df_res.sort_values('rank').drop(columns=['rank'])
    except Exception as e:
        return {"error": str(e)}