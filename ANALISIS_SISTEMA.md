# Análisis Técnico y de Negocio — GameQuest Buylist System
**Equipo:** Senior MTG Business Analyst + Senior Dev Team  
**Fecha:** 2025 · Versión 3.0 → 4.0

---

## 🔴 VULNERABILIDADES CRÍTICAS (resolver ahora)

### 1. El precio del CSV no es precio de mercado — es costo histórico
**El problema más grave del sistema.**  
El campo `Purchase price` de Manabox es lo que el VENDEDOR pagó cuando compró la carta, no el precio actual del mercado. Un vendedor que compró un Black Lotus por $1 hace 10 años recibiría una oferta de $450 CLP. Uno que compró una carta "de moda" por $30 que hoy vale $2 os costaría $13.500 CLP en pérdidas.

**Fix implementado:** Validación cruzada con Scryfall API (gratuita, 10 req/s). Si el precio del CSV diverge >50% del precio Scryfall, se genera alerta `danger`.

### 2. Foil y Condición completamente ignorados
El CSV de Manabox trae columnas `Foil` (`normal`, `foil`, `etched`) y `Condition` (`near_mint`, `lightly_played`, `moderately_played`, `heavily_played`, `damaged`). El sistema las ignoraba completamente.

- Una carta foil se compra/vende a 2-5x el precio NM — ignorarla = pérdidas masivas
- Una carta "heavily_played" vale ~50% de NM — comprarla a precio NM = sobredepagar

**Fix implementado:** Multiplicadores por foil y condición en ambos endpoints.

### 3. `commit_buylist` sin autenticación ni rate limiting
El endpoint `/api/public/commit_buylist` era completamente abierto. Cualquier bot podía spamear miles de órdenes, llenar la BD y saturar el SMTP de Gmail (límite 500 emails/día).

**Fix implementado:** Rate limiting por IP (máx 3 submits/hora), validación de honeypot, tamaño máximo de CSV.

### 4. SMTP sincrónico bloqueando el event loop de FastAPI
`smtplib.SMTP` es blocking I/O. En FastAPI async, bloquea el thread principal durante 2-5 segundos por email. Con carga concurrente esto mata el servidor.

**Fix implementado:** `asyncio.get_event_loop().run_in_executor()` para correr SMTP en thread pool.

### 5. Mismo token para admin y webhook de Jumpseller
`STORE_TOKEN` se usa como Bearer token para `/admin/*` Y como `X-Store-Token` header para el endpoint de canje. Si alguien intercepta el header de una request de canje, tiene acceso admin completo.

**Recomendación:** Crear `ADMIN_TOKEN` separado de `STORE_TOKEN`. No implementado para no romper integración con Jumpseller JS existente.

### 6. `_fetch_js_products_stock()` sin caché
Descarga el catálogo completo de Jumpseller en cada análisis. Con 500 productos = 10+ páginas × 50ms = 500ms mínimo bloqueando. Sin caché, el endpoint puede tardar 30+ segundos con catálogo grande y timeout de Render.

**Fix implementado:** Cache en memoria con TTL de 5 minutos.

---

## 🟡 PROBLEMAS DE NEGOCIO MTG (impacto en rentabilidad)

### 7. Margin factor uniforme 2.5x para todo
Un `Force of Will` (staple vintage, $80) y una `Island` (básica, $0.20) tienen el mismo multiplicador de margen. El mercado de singles MTG funciona por capas:

| Tier | Precio USD | Margen real |
|------|-----------|-------------|
| Bulk (<$0.50) | irrelevante | No comprar salvo lote |
| Mid ($1-$5) | mayor rotación | 40-60% |
| Premium ($5-$30) | baja rotación | 50-70% |
| High-end (>$30) | muy baja rotación | 60-80% |

Con factor 2.5x sobre precio cash (×450): compras a $0.45 USD equivalente y vendes a $1.125 USD. Eso es 60% gross margin en USD, que es razonable para mid/premium. Para bulk ($0.20) absorbe el overhead de procesamiento (tiempo staff, clasificación, listing). La alerta de MIN_PURCHASE_USD ($3) es correcta para este modelo.

### 8. Sin presupuesto diario/semanal de compra
No existe límite de cuánto puede gastar el negocio en un día. Si el link del buylist público se viraliza en Reddit o Twitter, podrías recibir 200 submissions overnight con $50.000 USD en compromisos.

**Fix implementado:** `BUYLIST_DAILY_BUDGET_USD` en env — cierra automáticamente el buylist cuando se supera el límite diario.

### 9. Sin diferenciación de Set/Edición
"Black Lotus" de Alpha vs Beta vs Unlimited tienen valores radicalmente distintos. El nombre solo no es suficiente para pricing correcto. El CSV trae `Set code` y `Collector number`.

**Recomendación futura:** Usar Scryfall ID (ya está en el CSV como `Scryfall ID`) para matching exacto en lugar de nombre.

### 10. Nombre de producto en Jumpseller vs nombre en Manabox
MTG tiene naming conventions variables: "Lightning Bolt (M11)", "Lightning Bolt [M11]", etc. El matching por lowercase exacto falla con cualquier variación.

**Fix implementado:** Normalización Unicode (unicodedata.normalize) + strip de paréntesis/corchetes para fuzzy matching.

---

## 🔵 DEUDA TÉCNICA

### 11. BuylistOrder sin `updated_at`, sin audit trail
Si un admin cambia el status de una orden, no hay registro de quién lo hizo ni cuándo.

**Fix implementado:** Columna `updated_at` + columna `reviewed_by` en BuylistOrder.

### 12. Render Free Tier = Cold Starts de 30+ segundos
El servidor duerme después de 15 minutos de inactividad. El primer request después del sleep (que puede ser un cliente real en el buylist público) espera 30+ segundos y probablemente abandona.

**Recomendación:** Tier pagado de Render ($7/mes) o ping keep-alive con UptimeRobot (gratuito).

### 13. No hay logging estructurado
Los `print()` y `logger.info()` no están en formato JSON. En producción en Render, los logs son difíciles de filtrar.

**Recomendación futura:** `python-json-logger` para logs parseables.

### 14. Columna `Foil` ignorada implica pérdidas garantizadas
Si alguien vende 10x Thassa's Oracle foil ($8 foil vs $3 NM) con el sistema actual, les pagas $3 × 450 = $1.350 CLP y los vendes a $8 × 1000 = $8.000 CLP c/u. Eso está bien para ti. Pero si fijas el precio en Jumpseller basado en el precio NM y alguien compra la foil a precio NM ($3.000 CLP), perdiste dinero.

**El problema real es consistencia:** el sistema compra a precio-foil-justo pero Jumpseller puede tener el precio NM si se cargó antes. El stock_check necesita alertar cuando un producto foil tiene precio NM en JS.

---

## ✅ LO QUE ESTÁ BIEN

- Arquitectura FastAPI + SQLAlchemy es sólida y escalable
- El sistema de cupones en Jumpseller con QP- prefix y regex validation es elegante
- El webhook de burn_coupon con background tasks es correcto
- La lógica de canje con `with_for_update()` previene race conditions
- El sistema de staples con override granular es correcto
- Emails con templates HTML se ven profesionales
- render.yaml bien configurado

---

## 📋 ROADMAP PRIORIZADO

### Inmediato (implementado en v4.0)
- [x] SMTP asíncrono
- [x] Foil/Condition multipliers
- [x] Scryfall price validation
- [x] Rate limiting en commit_buylist
- [x] Cache de stock JS
- [x] Email en internal buylist
- [x] Budget diario
- [x] Name normalization
- [x] Email respaldo en TODAS las buylists

### Corto plazo (próximos sprints)
- [ ] Separar ADMIN_TOKEN de STORE_TOKEN
- [ ] Tier pagado de Render o keep-alive
- [ ] Matching por Scryfall ID en lugar de nombre
- [ ] Logging estructurado JSON
- [ ] Tests unitarios para lógica de precios

### Medio plazo
- [ ] Dashboard de analytics (ventas vs compras por carta)
- [ ] Integración Cardmarket para precio EU
- [ ] Historial de cambios de precio en Jumpseller
- [ ] Sistema de reservas/turnos para el buylist
