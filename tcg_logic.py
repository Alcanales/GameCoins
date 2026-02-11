import pandas as pd
from io import BytesIO
from config import settings

def analizar_csv_estacas(file_content: bytes):
    try:
        df = pd.read_csv(BytesIO(file_content))
        # Normalizar columnas (limpieza)
        df.columns = [str(c).lower().strip() for c in df.columns]
        
        res = []
        for _, row in df.iterrows():
            # Obtener datos con valores por defecto 0
            pn = float(row.get('price_normal', 0))
            pf = float(row.get('price_foil', 0))
            name = str(row.get('name', 'Carta Desconocida'))
            current_stock = int(row.get('current_stock', 0))
            stock_limit = int(row.get('stock_limit', 0))
            
            status, razon = "APROBADO", "Compra Estándar"
            
            # --- DETECCIÓN DE ESTACAS ---
            # Regla: Foil caro (>20) pero Normal barato (<20) y ratio > 2.5x
            if pf >= 20.0 and pn < 20.0:
                ratio = pf / pn if pn > 0 else 999
                if ratio > 2.5 and (pf - pn) > 10.0:
                    status, razon = "RECHAZADO (ESTACA)", f"Diferencia sospechosa ({ratio:.1f}x)"
            elif pn >= 20.0: 
                status, razon = "HIGH END", "Alta Demanda / Staple"
            
            # --- CÁLCULO DE PRECIOS DE COMPRA ---
            # Redondeamos a entero porque el peso chileno (CLP) no usa decimales
            cash_normal = round(pn * settings.CASH_MULTIPLIER)
            gc_normal = round(pn * settings.GAMECOIN_MULTIPLIER)
            cash_foil = round(pf * settings.CASH_MULTIPLIER)
            gc_foil = round(pf * settings.GAMECOIN_MULTIPLIER)
            
            res.append({
                "name": name,
                "price_normal": pn,
                "price_foil": pf,
                "current_stock": current_stock,
                "stock_limit": stock_limit,
                "status": status,
                "razon": razon,
                "cash_normal": cash_normal,
                "gc_normal": gc_normal,
                "cash_foil": cash_foil,
                "gc_foil": gc_foil
            })
            
        df_res = pd.DataFrame(res)
        
        # --- ORDENAMIENTO INTELIGENTE ---
        # 1. High End (Lo mejor)
        # 2. Aprobado (Lo estándar)
        # 3. Estaca (Alerta)
        # 4. Otros Rechazados
        def get_rank(s):
            if "HIGH END" in s: return 0
            if "APROBADO" in s: return 1
            if "ESTACA" in s: return 2
            return 3
            
        df_res['rank'] = df_res['status'].apply(get_rank)
        df_res = df_res.sort_values(by='rank').drop(columns=['rank'])
        
        return df_res
    except Exception as e: 
        return {"error": f"Error leyendo CSV: {str(e)}"}