import pandas as pd
from io import BytesIO

def analizar_csv_estacas(file_content: bytes):
    try:
        df = pd.read_csv(BytesIO(file_content))
        # Normalizar columnas
        df.columns = [str(c).lower().strip() for c in df.columns]
        
        resultados = []
        for _, row in df.iterrows():
            nombre = str(row.get('name', 'Desconocido'))
            pn = float(row.get('price_normal', 0))
            pf = float(row.get('price_foil', 0))
            
            status, razon = "APROBADO", "OK"
            
            # Algoritmo de Detección de Estacas
            if pf >= 20.0 and pn < 20.0:
                ratio = pf / pn if pn > 0 else 999
                if ratio > 2.5 and (pf - pn) > 10.0:
                    status, razon = "RECHAZADO (ESTACA)", f"Ratio {ratio:.1f}x peligroso"
            elif pn >= 20.0:
                status, razon = "HIGH END", "Staple Seguro"
            
            resultados.append({
                "name": nombre, "price_normal": pn, "price_foil": pf, 
                "status": status, "razon": razon
            })
        return pd.DataFrame(resultados)
    except Exception as e: 
        return {"error": f"Error procesando CSV: {str(e)}"}