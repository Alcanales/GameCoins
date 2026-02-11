import pandas as pd
import aiohttp
import asyncio
from io import BytesIO
from .config import settings

async def fetch_scryfall_prices(session, scryfall_id):
    """Consulta precio normal y foil a Scryfall."""
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
        # Normalizar columnas
        df.columns = [str(c).lower().strip().replace(' ', '_') for c in df.columns]
        
        # Detectar columnas clave
        has_csv_price = 'purchase_price' in df.columns
        has_scryfall = 'scryfall_id' in df.columns
        
        res = []
        
        # 1. Obtener Precios de Referencia (Scryfall)
        async with aiohttp.ClientSession() as session:
            tasks = []
            if has_scryfall:
                for _, row in df.iterrows():
                    tasks.append(fetch_scryfall_prices(session, row.get('scryfall_id')))
                scryfall_data = await asyncio.gather(*tasks)
            else:
                scryfall_data = [(0.0, 0.0)] * len(df)

        # 2. Análisis Fila por Fila
        for i, (_, row) in enumerate(df.iterrows()):
            # Referencias de Scryfall (Base de comparación)
            sf_pn, sf_pf = scryfall_data[i]
            
            # Precio del Usuario (CSV)
            csv_price = float(row.get('purchase_price', 0)) if has_csv_price else 0.0
            
            # ¿Es Foil la carta física?
            is_foil = str(row.get('foil', '')).lower() == 'foil'
            
            # --- LÓGICA DE PRIORIDAD DE PRECIOS ---
            # Si el usuario puso precio en el CSV, ese MANDA. Si no, Scryfall.
            if is_foil:
                pf = csv_price if csv_price > 0 else sf_pf
                pn = sf_pn # La normal siempre es referencia Scryfall
            else:
                pn = csv_price if csv_price > 0 else sf_pn
                pf = sf_pf
            
            # --- ANÁLISIS DE ESTACAS (Riesgo) ---
            status = "APROBADO"
            
            if is_foil:
                # Comparamos el Precio Foil Real (PF) vs Precio Normal Referencia (PN)
                # Guía Maestra: Ratio > 2.5 y Diferencia > $10 USD
                if pf >= 20.0 and pn < 20.0:
                    ratio = pf / pn if pn > 0 else 999
                    if ratio > settings.STAKE_RATIO_THRESHOLD and (pf - pn) > settings.STAKE_DIFF_THRESHOLD:
                        status = "RECHAZADO (ESTACA)"
                
                # Excepción High End: Si la normal vale > $20, se aprueba siempre
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
                "status": status
            })
            
        df_res = pd.DataFrame(res)
        # Ordenar: High End primero, luego Aprobados, al final Rechazados
        df_res['rank'] = df_res['status'].apply(lambda s: 0 if "HIGH" in s else (1 if "APRO" in s else 2))
        return df_res.sort_values('rank').drop(columns=['rank'])
    except Exception as e:
        return {"error": str(e)}