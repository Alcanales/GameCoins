import pandas as pd
from io import BytesIO
from config import settings

def analizar_csv_simple(file_content: bytes):
    """
    Realiza un análisis rápido del CSV para la Buylist Pública.
    Calcula ofertas en Cash y GameCoins basadas en los multiplicadores de Render.
    """
    try:
        # Leer el contenido del CSV
        df = pd.read_csv(BytesIO(file_content))
        
        # Limpiar y normalizar los nombres de las columnas a minúsculas
        df.columns = [str(c).lower().strip() for c in df.columns]
        
        res = []
        for _, row in df.iterrows():
            # Obtener precios base (por defecto 0 si no existen)
            pn = float(row.get('price_normal', 0))
            pf = float(row.get('price_foil', 0))
            name = str(row.get('name', 'Carta Desconocida'))
            
            # Estado por defecto
            status = "APROBADO"
            
            # --- LÓGICA DE DETECCIÓN DE ESTACAS (FILTROS DE SEGURIDAD) ---
            # Se activan si el precio Foil es sospechosamente alto respecto al Normal
            if pf >= 20.0 and pn < 20.0:
                ratio = pf / pn if pn > 0 else 999
                # Verifica si el ratio supera el umbral (ej: 2.5x) y la diferencia es significativa
                if ratio > settings.STAKE_RATIO_THRESHOLD and (pf - pn) > settings.STAKE_DIFF_THRESHOLD:
                    status = "RECHAZADO (ESTACA)"
            
            # Identificar cartas de alta demanda (Staples)
            elif pn >= 20.0: 
                status = "HIGH END"
            
            # --- CÁLCULO DE OFERTAS EN CLP ---
            # Multiplica el precio base por los factores configurados en Render
            # Se redondea al entero más cercano para formato de moneda chilena
            cash_normal = round(pn * settings.CASH_MULTIPLIER)
            gc_normal = round(pn * settings.GAMECOIN_MULTIPLIER)
            cash_foil = round(pf * settings.CASH_MULTIPLIER)
            gc_foil = round(pf * settings.GAMECOIN_MULTIPLIER)
            
            res.append({
                "name": name,
                "price_normal": pn,
                "price_foil": pf,
                "status": status,
                "cash_normal": cash_normal,
                "gc_normal": gc_normal,
                "cash_foil": cash_foil,
                "gc_foil": gc_foil,
                "stock_limit": 0,    # No se muestra stock interno en la pública
                "current_stock": 0
            })
            
        # Crear DataFrame para facilitar el ordenamiento
        df_res = pd.DataFrame(res)
        
        # --- ORDENAMIENTO POR PRIORIDAD ---
        # 1. High End (Primero)
        # 2. Aprobado (Estándar)
        # 3. Rechazado (Al final)
        def get_rank(s):
            if "HIGH END" in s: return 0
            if "APROBADO" in s: return 1
            return 2
            
        df_res['rank'] = df_res['status'].apply(get_rank)
        df_res = df_res.sort_values(by='rank').drop(columns=['rank'])
        
        return df_res

    except Exception as e:
        # En caso de error en el formato del archivo
        return {"error": f"Error procesando el archivo CSV: {str(e)}"}