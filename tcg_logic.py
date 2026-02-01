import pandas as pd
from io import BytesIO

# --- CONSTANTES DE LA GUÍA MAESTRA ---
STAKE_MIN_PRICE = 20.0       # Filtro de entrada
STAKE_RATIO_THRESHOLD = 2.5  # Multiplicador de vanidad
MIN_STAKE_SPREAD = 10.0      # Margen de riesgo absoluto
NONFOIL_MAX_TYPICAL = 20.0   # Excepción de Staples (High End)

def analizar_csv_estacas(file_content: bytes):
    """
    Procesa el CSV de inventario y detecta estacas según la Guía Maestra.
    Retorna un DataFrame procesado y un resumen JSON.
    """
    try:
        df = pd.read_csv(BytesIO(file_content))
        
        # Normalización básica de columnas (ajustar según tu CSV real)
        df.columns = [str(c).lower().strip() for c in df.columns]
        
        # Validación mínima de columnas requeridas
        required = {'name', 'price_normal', 'price_foil'}
        if not required.issubset(set(df.columns)):
            return {"error": f"Faltan columnas requeridas. Se encontró: {list(df.columns)}"}

        resultados = []
        
        for index, row in df.iterrows():
            nombre = str(row.get('name', 'Unknown'))
            
            # Manejo seguro de floats
            try:
                p_normal = float(row.get('price_normal', 0))
                p_foil = float(row.get('price_foil', 0))
            except ValueError:
                continue # Saltar filas con precios inválidos
            
            status = "APROBADO"
            razon = "OK"
            
            
            # 1. Filtro de entrada: Si el foil es barato, ignorar riesgo
            if p_foil < STAKE_MIN_PRICE:
                resultados.append({"name": nombre, "status": "APROBADO", "razon": "Bajo Riesgo", "p_normal": p_normal, "p_foil": p_foil})
                continue

            # 2. Excepción de Staples (High End)
            if p_normal >= NONFOIL_MAX_TYPICAL:
                status = "HIGH END"
                razon = "Staple Seguro"
            
            else:
                # 3. Análisis de Ratio (Detección de Estaca)
                ratio = p_foil / p_normal if p_normal > 0 else 999.0
                spread = p_foil - p_normal
                
                if ratio > STAKE_RATIO_THRESHOLD:
                    if spread > MIN_STAKE_SPREAD:
                        status = "RECHAZADO (ESTACA)"
                        razon = f"Ratio Alto ({ratio:.1f}x) y Spread > $10"
                    else:
                        status = "APROBADO"
                        razon = "Spread bajo (Bulk Foil)"
                else:
                    status = "APROBADO"
                    razon = "Ratio saludable"

            resultados.append({
                "name": nombre,
                "price_normal": p_normal,
                "price_foil": p_foil,
                "status": status,
                "razon": razon
            })
            
        return pd.DataFrame(resultados)

    except Exception as e:
        return {"error": str(e)}