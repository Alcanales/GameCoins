import asyncio
import aiohttp
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from io import BytesIO, StringIO
from datetime import datetime
from sqlalchemy.orm import Session
from models import SystemConfig, GameCoinUser
from config import settings

async def fetch_scryfall_prices(session, scryfall_id):
    if not scryfall_id or str(scryfall_id) == 'nan': return {'price_normal':0.0, 'price_foil':0.0}
    try:
        async with session.get(f"https://api.scryfall.com/cards/{scryfall_id}") as r:
            if r.status == 200:
                d = await r.json()
                return {'price_normal': float(d.get('prices',{}).get('usd') or 0), 'price_foil': float(d.get('prices',{}).get('usd_foil') or 0)}
    except: pass
    return {'price_normal':0.0, 'price_foil':0.0}

async def get_jumpseller_stock(session, name, login, token):
    if not name: return 0
    try:
        async with session.get(f"{settings.JUMPSELLER_API_BASE}/products/search.json", params={'login':login, 'authtoken':token, 'query':name, 'fields':'stock'}) as r:
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
                tasks.append(fetch_scryfall_prices(sess, row.get('scryfall id')))
                tasks.append(get_jumpseller_stock(sess, row.get('name'), *creds) if creds else asyncio.sleep(0))
            results = await asyncio.gather(*tasks)

        processed = []
        for i, (_, row) in enumerate(df.iterrows()):
            p_data, s_data = results[i*2], results[i*2+1]
            pn, pf = p_data.get('price_normal',0), p_data.get('price_foil',0)
            stock = int(s_data) if creds else 0
            
            status, razon = "APROBADO", "OK"
            limit = settings.STOCK_LIMIT_HIGH_DEMAND if pn >= 20.0 else settings.STOCK_LIMIT_DEFAULT
            
            if stock >= limit: status, razon = "RECHAZADO (FULL)", f"Stock {stock}/{limit}"
            elif pf >= 20.0 and pn < 20.0 and (pf/pn > settings.STAKE_RATIO_THRESHOLD if pn > 0 else True) and (pf-pn > settings.STAKE_DIFF_THRESHOLD):
                status, razon = "RECHAZADO (ESTACA)", "Posible Estaca"
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

async def procesar_canje_atomico(email, monto, db):
    if settings.MAINTENANCE_MODE_CANJE: return {"status":"error", "detail":"Mantenimiento"}
    user = db.query(GameCoinUser).filter(GameCoinUser.email == email).first()
    if not user or user.saldo < monto: return {"status":"error", "detail":"Saldo insuficiente"}
    
    try:
        codigo = f"GQ-{email.split('@')[0]}-{monto}"
        user.saldo -= monto
        user.historico_canjeado += monto
        db.flush()
        
        t = db.query(SystemConfig).filter(SystemConfig.key=="JUMPSELLER_API_TOKEN").first()
        s = db.query(SystemConfig).filter(SystemConfig.key=="JUMPSELLER_STORE").first()
        
        url = f"{settings.JUMPSELLER_API_BASE}/promotions.json"
        payload = {"promotion":{"name":f"Canje {code}","code":code,"discount_amount":monto,"status":"active","usage_limit":1,"customer_emails":[email],"begins_at":datetime.now().strftime("%Y-%m-%d")}}
        
        async with aiohttp.ClientSession() as sess:
            async with sess.post(url, params={'login':s.value,'authtoken':t.value}, json=payload) as r:
                if r.status != 201: raise Exception(await r.text())
        
        db.commit()
        return {"status":"ok", "cupon_codigo":code}
    except:
        db.rollback()
        return {"status":"error", "detail":"Error generando cupón"}

def enviar_correo_cotizacion(data):
    try:
        msg = MIMEMultipart()
        msg['From'] = settings.SMTP_USER
        msg['To'] = f"{settings.TARGET_EMAIL}, {data['email']}"
        msg['Subject'] = f"Nueva Cotización Buylist - {data['rut']}"
        
        body = f"""
        Nueva solicitud de venta:
        Nombre: {data['nombre']} {data['apellido']}
        RUT: {data['rut']}
        Teléfono: {data['telefono']}
        Pago Preferido: {data['pago']}
        
        Se adjunta el detalle de las cartas seleccionadas.
        """
        msg.attach(MIMEText(body, 'plain'))
        
        # Generar CSV
        df = pd.DataFrame(data['cartas'])
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        part = MIMEBase('application', "octet-stream")
        part.set_payload(csv_buffer.getvalue().encode("utf-8"))
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="cotizacion_{data["rut"]}.csv"')
        msg.attach(part)
        
        server = smtplib.SMTP(settings.SMTP_SERVER, settings.SMTP_PORT)
        server.starttls()
        server.login(settings.SMTP_USER, settings.SMTP_PASS)
        server.send_message(msg)
        server.quit()
        return True
    except Exception as e:
        print(f"Error mail: {e}")
        return False