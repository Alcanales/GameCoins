import asyncio
import aiohttp
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from io import BytesIO, StringIO
from sqlalchemy.orm import Session
from models import SystemConfig, GameCoinUser
from config import settings

async def get_jumpseller_stock(session, name, login, token):
    if not name: return 0
    try:
        url = f"{settings.JUMPSELLER_API_BASE}/products/search.json"
        async with session.get(url, params={'login':login, 'authtoken':token, 'query':name, 'fields':'stock'}) as r:
            if r.status == 200:
                data = await r.json()
                return sum(p.get('stock', 0) for p in data)
    except: pass
    return 0

async def analizar_csv_con_stock_real(content, db):
    try:
        t = db.query(SystemConfig).filter(SystemConfig.key=="JUMPSELLER_API_TOKEN").first()
        s = db.query(SystemConfig).filter(SystemConfig.key=="JUMPSELLER_STORE").first()
        creds = (s.value, t.value) if t and s else None
        
        df = pd.read_csv(BytesIO(content))
        df.columns = [str(c).lower().strip() for c in df.columns]
        
        async with aiohttp.ClientSession() as sess:
            tasks = []
            for _, row in df.iterrows():
                tasks.append(get_jumpseller_stock(sess, row.get('name'), *creds) if creds else asyncio.sleep(0))
            results = await asyncio.gather(*tasks)

        processed = []
        for i, (_, row) in enumerate(df.iterrows()):
            stock = int(results[i]) if creds else 0
            pn, pf = float(row.get('price_normal', 0)), float(row.get('price_foil', 0))
            
            status, razon = "APROBADO", "OK"
            limit = settings.STOCK_LIMIT_HIGH_DEMAND if pn >= 20.0 else settings.STOCK_LIMIT_DEFAULT
            
            if stock >= limit: status, razon = "RECHAZADO (FULL)", f"Stock {stock}/{limit}"
            elif pf >= 20.0 and pn < 20.0 and (pf/pn > settings.STAKE_RATIO_THRESHOLD if pn > 0 else True) and (pf-pn > settings.STAKE_DIFF_THRESHOLD):
                status, razon = "RECHAZADO (ESTACA)", "Relación sospechosa"
            elif pn >= 20.0: status, razon = "HIGH END", "Alta Demanda"

            processed.append({
                "name": row.get('name','Unknown'),
                "price_normal": pn, "price_foil": pf,
                "cash_normal": round(pn * settings.CASH_MULTIPLIER),
                "gc_normal": round(pn * settings.GAMECOIN_MULTIPLIER),
                "cash_foil": round(pf * settings.CASH_MULTIPLIER),
                "gc_foil": round(pf * settings.GAMECOIN_MULTIPLIER),
                "current_stock": stock, "stock_limit": limit,
                "status": status, "razon": razon
            })
            
        df_res = pd.DataFrame(processed)
        df_res['rank'] = df_res['status'].apply(lambda s: 0 if "HIGH" in s else (1 if "APRO" in s else 2))
        return df_res.sort_values('rank').drop(columns=['rank'])
    except Exception as e: return {"error": str(e)}

def enviar_correo_cotizacion(data):
    try:
        msg = MIMEMultipart()
        msg['From'] = settings.SMTP_USER
        msg['To'] = f"{settings.TARGET_EMAIL}, {data['email']}"
        msg['Subject'] = f"Venta GameQuest - {data['rut']}"
        body = f"Nueva solicitud:\nNombre: {data['nombre']} {data['apellido']}\nRUT: {data['rut']}\nTel: {data['telefono']}\nPago: {data['pago']}"
        msg.attach(MIMEText(body, 'plain'))
        df = pd.DataFrame(data['cartas'])
        csv_out = StringIO()
        df.to_csv(csv_out, index=False)
        part = MIMEBase('application', "octet-stream")
        part.set_payload(csv_out.getvalue().encode("utf-8"))
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="oferta_{data["rut"]}.csv"')
        msg.attach(part)
        with smtplib.SMTP(settings.SMTP_SERVER, settings.SMTP_PORT) as server:
            server.starttls()
            server.login(settings.SMTP_USER, settings.SMTP_PASS)
            server.send_message(msg)
        return True
    except: return False