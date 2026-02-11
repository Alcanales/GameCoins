import pandas as pd
from io import BytesIO
from config import settings

def analizar_csv_simple(content):
    try:
        df = pd.read_csv(BytesIO(content))
        df.columns = [str(c).lower().strip() for c in df.columns]
        res = []
        for _, row in df.iterrows():
            pn = float(row.get('price_normal', 0))
            pf = float(row.get('price_foil', 0))
            status = "APROBADO"
            if pf >= 20.0 and pn < 20.0 and (pf/pn > settings.STAKE_RATIO_THRESHOLD if pn>0 else True) and (pf-pn > settings.STAKE_DIFF_THRESHOLD):
                status = "RECHAZADO (ESTACA)"
            elif pn >= 20.0: status = "HIGH END"

            res.append({
                "name": row.get('name', 'Unknown'),
                "cash_normal": round(pn * settings.CASH_MULTIPLIER),
                "gc_normal": round(pn * settings.GAMECOIN_MULTIPLIER),
                "cash_foil": round(pf * settings.CASH_MULTIPLIER),
                "gc_foil": round(pf * settings.GAMECOIN_MULTIPLIER),
                "status": status,
                "price_normal": pn, "price_foil": pf
            })
        
        df_res = pd.DataFrame(res)
        df_res['rank'] = df_res['status'].apply(lambda s: 0 if "HIGH" in s else (1 if "APRO" in s else 2))
        return df_res.sort_values('rank').drop(columns=['rank'])
    except Exception as e: return {"error": str(e)}