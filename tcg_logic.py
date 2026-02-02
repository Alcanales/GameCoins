import pandas as pd
from io import BytesIO

def analizar_csv_estacas(file_content: bytes):
    try:
        df = pd.read_csv(BytesIO(file_content))
        df.columns = [str(c).lower().strip() for c in df.columns]
        res = []
        for _, row in df.iterrows():
            pn, pf = float(row.get('price_normal', 0)), float(row.get('price_foil', 0))
            status, razon = "APROBADO", "OK"
            if pf >= 20.0 and pn < 20.0:
                ratio = pf / pn if pn > 0 else 999
                if ratio > 2.5 and (pf - pn) > 10.0:
                    status, razon = "RECHAZADO (ESTACA)", f"Ratio {ratio:.1f}x"
            elif pn >= 20.0: status, razon = "HIGH END", "Staple Seguro"
            res.append({"name": row.get('name'), "status": status, "razon": razon})
        return pd.DataFrame(res)
    except Exception as e: return {"error": str(e)}
