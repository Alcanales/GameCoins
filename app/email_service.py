"""
email_service.py — GameQuest v4.0
Todas las funciones son async. SMTP corre en thread pool para no bloquear FastAPI.
"""
import asyncio
import smtplib
import logging
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List

from .config import settings

logger    = logging.getLogger(__name__)
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587


# ── Core: envío asíncrono (SMTP en thread pool, no bloquea event loop) ────────

def _send_sync(to: str, subject: str, html_body: str) -> bool:
    """
    Blocking SMTP — se llama desde run_in_executor.

    FIX M-04: timeout aumentado a 30s (era 15s) y reintentos automáticos.
    Gmail TLS puede tardar hasta ~20s en picos de carga del servidor.

    Política de reintentos (máx 3 intentos totales):
      - SMTPServerDisconnected: el servidor cerró la conexión antes de tiempo → reintentar
      - OSError / TimeoutError:  fallo de red o timeout → reintentar
      - Otros errores (auth, dest inválido): no reintentar (no mejoran con el tiempo)
    Backoff: 5s entre intentos (no exponencial — Gmail no requiere esperas largas).
    """
    import time
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"GameQuest <{settings.SMTP_EMAIL}>"
    msg["To"]      = to
    msg.attach(MIMEText(html_body, "html"))

    _RETRYABLE = (smtplib.SMTPServerDisconnected, OSError, TimeoutError)
    last_error: Exception | None = None

    for attempt in range(1, 4):   # intentos 1, 2, 3
        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as srv:
                srv.ehlo()
                srv.starttls()
                srv.login(settings.SMTP_EMAIL, settings.SMTP_PASSWORD)
                srv.sendmail(settings.SMTP_EMAIL, to, msg.as_string())
            logger.info(f"[EMAIL] ✅ → {to} | {subject}")
            return True
        except _RETRYABLE as e:
            last_error = e
            if attempt < 3:
                logger.warning(
                    f"[EMAIL] ⚠️ Intento {attempt}/3 fallido ({type(e).__name__}) "
                    f"→ {to} | reintentando en 5s…"
                )
                time.sleep(5)
            else:
                logger.error(
                    f"[EMAIL] ❌ 3/3 intentos agotados → {to} | {subject} | {e}"
                )
        except Exception as e:
            # Errores no recuperables (auth fallida, destino inválido, etc.)
            logger.error(f"[EMAIL] ❌ Error no recuperable → {to} | {e}")
            return False

    return False


async def _send(to: str, subject: str, html_body: str) -> bool:
    """Wrapper async: mueve SMTP al thread pool."""
    if not settings.SMTP_EMAIL or not settings.SMTP_PASSWORD:
        logger.warning("[EMAIL] SMTP no configurado — email omitido")
        return False
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _send_sync, to, subject, html_body)


async def _send_both(
    vendor_email: str,
    subject_vendor: str,
    html_vendor: str,
    subject_store:  str,
    html_store:     str,
) -> dict:
    """Envía ambos emails en paralelo."""
    results = await asyncio.gather(
        _send(vendor_email,        subject_vendor, html_vendor),
        _send(settings.TARGET_EMAIL, subject_store,  html_store),
        return_exceptions=True,
    )
    # EMAIL-02 FIX: Exception es truthy en Python — verificar tipo explícitamente
    return {
        "vendor": isinstance(results[0], bool) and results[0],
        "store":  isinstance(results[1], bool) and results[1],
    }


# ── Base template ──────────────────────────────────────────────────────────────

def _base(title: str, subtitle: str, body: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="es"><head><meta charset="UTF-8">
<style>
  body{{margin:0;padding:0;background:#f0f4f2;font-family:'Inter',Arial,sans-serif;}}
  .w{{max-width:640px;margin:32px auto;background:#fff;border-radius:16px;
      overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,.08);}}
  .hd{{background:#0f172a;padding:24px 32px;}}
  .hd h1{{margin:0;color:#eab308;font-size:20px;font-weight:900;letter-spacing:-.5px;}}
  .hd p{{margin:4px 0 0;color:#94a3b8;font-size:11px;text-transform:uppercase;letter-spacing:1px;}}
  .bd{{padding:28px 32px;color:#1e293b;font-size:14px;line-height:1.7;}}
  table.t{{width:100%;border-collapse:collapse;margin:16px 0;font-size:13px;}}
  table.t th{{background:#f8fafc;padding:9px 11px;text-align:left;font-size:10px;
              text-transform:uppercase;letter-spacing:.7px;color:#64748b;
              border-bottom:2px solid #e2e8f0;}}
  table.t td{{padding:9px 11px;border-bottom:1px solid #f1f5f9;vertical-align:top;}}
  table.t tr:last-child td{{border:none;}}
  .tr{{background:#f8fafc;font-weight:700;}}
  .hi{{color:#dc2626;font-weight:700;}}
  .ok{{color:#16a34a;font-weight:700;}}
  .warn{{color:#d97706;font-weight:700;}}
  .badge{{display:inline-block;padding:2px 10px;border-radius:20px;font-size:10px;font-weight:700;text-transform:uppercase;}}
  .b-cred{{background:#fef9c3;color:#854d0e;}}
  .b-cash{{background:#dcfce7;color:#166534;}}
  .b-mix {{background:#ede9fe;color:#4c1d95;}}
  .b-int {{background:#f1f5f9;color:#475569;}}
  .alert-box{{margin:20px 0;padding:14px 16px;border-radius:8px;font-size:12px;}}
  .alert-warn{{background:#fef9c3;border-left:3px solid #d97706;color:#713f12;}}
  .alert-info{{background:#eff6ff;border-left:3px solid #3b82f6;color:#1e3a8a;}}
  .ft{{background:#f8fafc;padding:18px 32px;text-align:center;font-size:11px;
       color:#94a3b8;border-top:1px solid #e2e8f0;}}
  .ft a{{color:#94a3b8;}}
</style>
</head>
<body>
<div class="w">
  <div class="hd"><h1>🎮 GameQuest</h1><p>{title}</p></div>
  <div class="bd">{body}</div>
  <div class="ft">GameQuest &middot; <a href="https://gamequest.cl">gamequest.cl</a> &middot; contacto@gamequest.cl<br>
  <span style="font-size:10px;color:#cbd5e1;">{subtitle}</span></div>
</div>
</body></html>"""


# ── Helpers de tabla ───────────────────────────────────────────────────────────

def _badge(pref: str) -> str:
    m = {"credito":"b-cred 💎 Crédito QP","cash":"b-cash 💵 Cash","mixto":"b-mix 🔀 Mixto","interno":"b-int 🔧 Interno"}
    v = m.get(pref, f"b-int {pref}")
    cls, lbl = v.split(" ", 1)
    return f'<span class="badge {cls}">{lbl}</span>'


def _items_table(items: list, show_alerts: bool = False) -> str:
    foil_label = {"foil": "✨ Foil", "etched": "⬡ Etched", "normal": ""}
    cond_label = {"near_mint": "", "lightly_played": "LP", "moderately_played": "MP",
                  "heavily_played": "HP", "damaged": "DMG"}

    rows = ""
    for it in items:
        foil    = foil_label.get(it.get("foil", "normal"), "")
        cond    = cond_label.get(it.get("condition", "near_mint"), "")
        version = (it.get("version") or "").strip()
        is_est  = it.get("is_estaca", False)

        # Tags de versión: Foil, condición, versión especial
        tag_parts = []
        if foil:
            tag_parts.append(f'<span style="background:#ede9fe;color:#5b21b6;padding:1px 7px;border-radius:20px;font-size:10px;font-weight:700;">{foil}</span>')
        if version:
            tag_parts.append(f'<span style="background:#f0fdf4;color:#166534;padding:1px 7px;border-radius:20px;font-size:10px;font-weight:700;">{version}</span>')
        elif is_est and not foil:
            tag_parts.append('<span style="background:#fdf4ff;color:#7c3aed;padding:1px 7px;border-radius:20px;font-size:10px;font-weight:700;">★ De Nicho</span>')
        if cond:
            tag_parts.append(f'<span style="background:#fef9c3;color:#713f12;padding:1px 7px;border-radius:20px;font-size:10px;font-weight:700;">{cond}</span>')
        tags_html = (" " + " ".join(tag_parts)) if tag_parts else ""

        # Precio: mostrar raw (CK original) + nota de estaca si aplica
        price_raw = it.get("price_usd_raw") or it.get("price_usd") or 0
        price_adj = it.get("price_usd") or 0
        if is_est and price_raw != price_adj:
            price_html = f'${price_raw:.2f} <span style="color:#7c3aed;font-size:10px;">→ ${price_adj:.2f} ×nicho</span>'
        else:
            price_html = f'${price_adj:.2f}'

        alerts_html = ""
        if show_alerts and it.get("alerts"):
            for a in it["alerts"]:
                color = {"danger": "#dc2626", "warning": "#d97706", "info": "#3b82f6"}.get(a["type"], "#64748b")
                alerts_html += f'<br><span style="font-size:10px;color:{color};">⚠ {a["msg"]}</span>'

        qty = it.get("qty") or it.get("qty_csv") or 1
        rows += f"""<tr>
          <td>{it.get('name','')}{tags_html}{alerts_html}</td>
          <td style="text-align:center">{qty}</td>
          <td style="text-align:right;color:#64748b;font-size:12px;">{price_html} USD</td>
          <td style="text-align:right"><strong>${it.get('price_credito',0):,}</strong></td>
          <td style="text-align:right"><strong>${it.get('price_cash',0):,}</strong></td>
        </tr>"""

    return f"""<table class="t">
      <thead><tr>
        <th>Carta</th><th style="text-align:center">Cant.</th>
        <th style="text-align:right">Ref. CK (USD)</th>
        <th style="text-align:right">💎 Crédito QP</th>
        <th style="text-align:right">💵 Cash CLP</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


def _totals_table(total_credito: float, total_cash: float) -> str:
    return f"""<table class="t">
      <tr class="tr">
        <td colspan="3"><strong>TOTAL CRÉDITO QUESTPOINTS</strong></td>
        <td colspan="2" style="text-align:right"><span class="hi">${total_credito:,.0f} QP</span></td>
      </tr>
      <tr class="tr">
        <td colspan="3"><strong>TOTAL CASH (CLP)</strong></td>
        <td colspan="2" style="text-align:right"><span class="hi">${total_cash:,.0f}</span></td>
      </tr>
    </table>"""


# ── EMAIL 1: Confirmación al vendedor (buylist pública) ───────────────────────

async def send_public_buylist_vendor(
    vendor_email: str, rut: str, payment_pref: str,
    items: list, total_credito: float, total_cash: float, order_id: int,
) -> bool:
    body = f"""
    <p>Hola,</p>
    <p>Recibimos tu cotización Buylist Quest. Aquí está tu resumen:</p>
    <table class="t">
      <tr><td><strong>Orden #</strong></td><td><strong class="ok">#{order_id}</strong></td></tr>
      <tr><td><strong>RUT</strong></td><td>{rut}</td></tr>
      <tr><td><strong>Preferencia de pago</strong></td><td>{_badge(payment_pref)}</td></tr>
      <tr><td><strong>Cartas cotizadas</strong></td><td>{len(items)}</td></tr>
    </table>
    {_items_table(items)}
    {_totals_table(total_credito, total_cash)}
    <div class="alert-box alert-warn">
      <strong>📋 Nota:</strong> Los precios son referenciales y no constituyen una obligación de compra. Las cartas se evalúan presencialmente en Near Mint — foil y condición pueden ajustarse en inspección. Nos contactaremos contigo para coordinar.
    </div>
    <p>Te contactaremos pronto. ¡Gracias por elegir GameQuest! 🎮</p>"""

    return await _send(
        vendor_email,
        f"✅ GameQuest — Cotización Buylist #{order_id} Recibida",
        _base("Cotización Recibida — Sin Compromiso", f"Orden #{order_id} · {datetime.now().strftime('%d/%m/%Y %H:%M')}", body),
    )


# ── EMAIL 2: Respaldo interno (buylist pública → GameQuest) ───────────────────

async def send_public_buylist_store(
    vendor_email: str, rut: str, payment_pref: str,
    items: list, total_credito: float, total_cash: float, order_id: int,
) -> bool:
    body = f"""
    <p>🔔 <strong>Nueva orden de Buylist pública</strong> — revisión requerida.</p>
    <table class="t">
      <tr><td><strong>Orden #</strong></td><td><strong class="ok">#{order_id}</strong></td></tr>
      <tr><td><strong>Vendedor</strong></td><td>{vendor_email}</td></tr>
      <tr><td><strong>RUT</strong></td><td>{rut}</td></tr>
      <tr><td><strong>Preferencia pago</strong></td><td>{_badge(payment_pref)}</td></tr>
      <tr><td><strong>Total cartas</strong></td><td>{len(items)}</td></tr>
    </table>
    {_items_table(items)}
    {_totals_table(total_credito, total_cash)}
    <div class="alert-box alert-info">
      <strong>ℹ️ Acción requerida:</strong> Contactar al vendedor en <strong>{vendor_email}</strong>
      para coordinar la revisión de las cartas.
    </div>"""

    return await _send(
        settings.TARGET_EMAIL,
        f"📋 Buylist #{order_id} — {vendor_email} [{payment_pref.upper()}] ${total_cash:,.0f} CLP",
        _base("Nueva Cotización Buylist Pública", f"Recibida {datetime.now().strftime('%d/%m/%Y %H:%M')}", body),
    )


# ── EMAIL 3 + 4: Ambos en paralelo (buylist pública) ─────────────────────────

async def send_public_buylist_both(
    vendor_email: str, rut: str, payment_pref: str,
    items: list, total_credito: float, total_cash: float, order_id: int,
) -> dict:
    return await asyncio.gather(
        send_public_buylist_vendor(vendor_email, rut, payment_pref, items, total_credito, total_cash, order_id),
        send_public_buylist_store(vendor_email, rut, payment_pref, items, total_credito, total_cash, order_id),
        return_exceptions=True,
    )


# ── EMAIL 5: Reporte análisis interno (Buylist Interna → admin) ───────────────

async def send_internal_analysis_report(
    items: list,
    summary: dict,
    filename: str = "análisis",
) -> bool:
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    danger_items  = [c for c in items if c.get("status") == "danger"]
    warning_items = [c for c in items if c.get("status") == "warning"]
    ok_items      = [c for c in items if c.get("status") == "ok"]

    def _alert_section(cards: list, color: str, icon: str) -> str:
        if not cards:
            return ""
        rows = "".join(
            f"<tr><td>{c['name']}</td>"
            f"<td style='text-align:center'>{c.get('qty') or c.get('qty_csv') or 1}</td>"
            f"<td style='text-align:right;color:#64748b;'>${c.get('price_usd',0):.2f}</td>"
            f"<td style='text-align:right;color:{color};font-weight:700;'>${(c.get('price_cash',0)*(c.get('qty') or c.get('qty_csv') or 1)):,}</td>"
            f"<td style='font-size:11px;color:{color};'>"
            + " | ".join(a["msg"] for a in c.get("alerts", [])) +
            f"</td></tr>"
            for c in cards
        )
        return f"""<table class="t">
          <thead><tr>
            <th>{icon} Carta</th><th style="text-align:center">Cant.</th>
            <th style="text-align:right">USD</th><th style="text-align:right">Cash</th>
            <th>Alertas</th>
          </tr></thead>
          <tbody>{rows}</tbody>
        </table>"""

    body = f"""
    <p>Reporte de análisis de compra generado el <strong>{now}</strong>.</p>
    <p>Archivo analizado: <code>{filename}</code></p>

    <table class="t">
      <tr><td>Total cartas</td><td><strong>{summary.get('total_cards',0)}</strong></td></tr>
      <tr><td style="color:#dc2626">🔴 Peligro</td><td><strong style="color:#dc2626">{summary.get('danger_count',0)}</strong></td></tr>
      <tr><td style="color:#d97706">🟡 Alerta</td><td><strong style="color:#d97706">{summary.get('warning_count',0)}</strong></td></tr>
      <tr><td style="color:#16a34a">🟢 OK</td><td><strong style="color:#16a34a">{summary.get('ok_count',0)}</strong></td></tr>
      <tr><td>Total USD a pagar</td><td><strong>${summary.get('total_usd_compra',0):.2f} USD</strong></td></tr>
      <tr class="tr"><td>Total CLP Cash</td><td><span class="hi">${summary.get('total_clp_cash',0):,.0f} CLP</span></td></tr>
      <tr class="tr"><td>Total CLP Crédito</td><td><span class="ok">${summary.get('total_clp_credito',0):,.0f} QP</span></td></tr>
    </table>

    {"<h3 style='color:#dc2626;margin-top:24px;'>🔴 Cartas con Peligro</h3>" + _alert_section(danger_items, "#dc2626", "🔴") if danger_items else ""}
    {"<h3 style='color:#d97706;margin-top:24px;'>🟡 Cartas con Alerta</h3>" + _alert_section(warning_items, "#d97706", "🟡") if warning_items else ""}
    {"<h3 style='color:#16a34a;margin-top:24px;'>🟢 Cartas OK</h3>" + _alert_section(ok_items, "#16a34a", "🟢") if ok_items else ""}

    <div class="alert-box alert-info" style="margin-top:24px;">
      Este reporte es generado automáticamente por el sistema de análisis de compra GameQuest.
      Revisa las alertas DANGER antes de comprometer cualquier compra.
    </div>"""

    return await _send(
        settings.TARGET_EMAIL,
        f"📊 Análisis Compra GameQuest — {summary.get('total_cards',0)} cartas · ${summary.get('total_clp_cash',0):,.0f} CLP · {now}",
        _base("Reporte Interno de Compra", f"Generado {now}", body),
    )
