# 🚀 Guía de Deploy en Render

## Configuración para Render.com

### Opción 1: Usar requirements.txt (Recomendado - Más Simple)

En Render, simplemente usa el `requirements.txt` existente:

**Build Command:**
```bash
pip install -r requirements.txt
```

**Start Command:**
```bash
gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:$PORT
```

---

### Opción 2: Usar Script de Instalación Personalizado

Si quieres más control:

**Build Command:**
```bash
chmod +x install.sh && ./install.sh
```

**Start Command:**
```bash
gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:$PORT
```

---

### Opción 3: Separar Producción y Desarrollo

Para optimizar tamaño y velocidad:

**Build Command:**
```bash
pip install -r requirements-prod.txt
```

**Start Command:**
```bash
gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:$PORT
```

---

## ⚙️ Variables de Entorno en Render

Ve a tu servicio en Render → **Environment**

### Variables OBLIGATORIAS:
```bash
DATABASE_URL=postgresql://...  # Render te da esto automáticamente si usas Postgres
ADMIN_USER=tu_admin
ADMIN_PASS=Tu_Password_Seguro_123
STORE_TOKEN=tu_store_token_secreto
JWT_SECRET_KEY=tu_jwt_secret_min_32_caracteres
```

### Variables OPCIONALES:
```bash
JWT_EXPIRATION_MINUTES=60
RATE_LIMIT_PER_MINUTE=10
LOG_LEVEL=INFO
SENTRY_DSN=tu_sentry_dsn  # Si usas Sentry
REDIS_URL=redis://...     # Si usas Redis
```

---

## 📋 Checklist de Deploy en Render

### 1. Crear Web Service
- [ ] Ir a [Render Dashboard](https://dashboard.render.com/)
- [ ] Click en "New +" → "Web Service"
- [ ] Conectar repositorio de GitHub

### 2. Configuración Básica
- [ ] **Name:** `gamequest-api`
- [ ] **Region:** Oregon (US West) o el más cercano
- [ ] **Branch:** `main`
- [ ] **Root Directory:** `.` (raíz del proyecto)
- [ ] **Runtime:** `Python 3`

### 3. Build & Start Commands
Elige UNA de estas opciones:

**Opción Simple (Recomendada):**
```
Build: pip install -r requirements.txt
Start: gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:$PORT
```

**Opción Optimizada:**
```
Build: pip install -r requirements-prod.txt
Start: gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:$PORT
```

**Opción con Script:**
```
Build: chmod +x install.sh && ./install.sh
Start: gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:$PORT
```

### 4. Plan
- [ ] **Free Tier:** OK para testing/desarrollo
- [ ] **Starter ($7/mes):** Recomendado para producción
- [ ] **Standard ($25/mes):** Para alto tráfico

### 5. Base de Datos PostgreSQL
- [ ] Click en "New +" → "PostgreSQL"
- [ ] **Name:** `gamequest-db`
- [ ] **Plan:** Free (1GB) o Starter (10GB - $7/mes)
- [ ] Copiar la "Internal Database URL"
- [ ] En tu Web Service, agregar variable: `DATABASE_URL=<la_url_copiada>`

### 6. Variables de Entorno
Agregar en la sección **Environment**:

```bash
# Obligatorias
ADMIN_USER=admin_gamequest
ADMIN_PASS=PasswordSeguro123ABC
STORE_TOKEN=<generar con: openssl rand -hex 24>
JWT_SECRET_KEY=<generar con: openssl rand -hex 32>

# Opcionales
JWT_EXPIRATION_MINUTES=60
GAMECOIN_MULTIPLIER=0.55
MIN_PURCHASE_USD=3
LOG_LEVEL=INFO
```

### 7. Health Check Path
- [ ] Configurar: `/health`
- [ ] Render monitoreará este endpoint automáticamente

### 8. Deploy!
- [ ] Click en "Create Web Service"
- [ ] Esperar ~3-5 minutos
- [ ] Verificar en logs que todo inició correctamente

---

## ✅ Verificación Post-Deploy

Una vez deployado, verifica que todo funcione:

```bash
# 1. Health Check
curl https://tu-app.onrender.com/health

# 2. Documentación API
# Abre en navegador:
https://tu-app.onrender.com/docs

# 3. Test de balance
curl https://tu-app.onrender.com/api/public/balance/test@example.com

# 4. Test de login admin
curl -X POST https://tu-app.onrender.com/admin/login \
  -H "Content-Type: application/json" \
  -d '{"username":"tu_admin","password":"tu_password"}'
```

---

## 🐛 Troubleshooting Render

### "Application failed to respond"
- Verifica que el `PORT` no esté hardcodeado
- Render usa `$PORT` dinámico
- Tu start command debe incluir: `--bind 0.0.0.0:$PORT`

### "Module not found"
- Verifica que `requirements.txt` tenga todas las dependencias
- Chequea los logs de build para errores de instalación

### "Database connection failed"
- Verifica que `DATABASE_URL` esté configurada
- Si usas PostgreSQL de Render, debe ser la "Internal Database URL"

### "Too many requests" (429)
- El rate limiting está funcionando
- Ajusta `RATE_LIMIT_PER_MINUTE` si es muy estricto

### Logs no muestran nada
- Verifica `LOG_LEVEL=INFO` en variables de entorno
- Render muestra logs en tiempo real en el dashboard

---

## 🔄 Auto-Deploy desde GitHub

Render puede auto-deployar cuando haces push:

1. Ve a tu Web Service → **Settings**
2. **Auto-Deploy:** ✅ Yes
3. Ahora cada push a `main` hace auto-deploy

---

## 💰 Estimación de Costos

| Componente | Plan | Precio/mes |
|------------|------|------------|
| Web Service | Free | $0 |
| PostgreSQL | Free | $0 |
| **Total Free** | | **$0** |
|||
| Web Service | Starter | $7 |
| PostgreSQL | Starter | $7 |
| **Total Producción** | | **$14/mes** |

**Nota:** Free tier tiene limitaciones (sleep después de inactividad, menos recursos)

---

## 📚 Recursos Adicionales

- [Render Docs - Python](https://render.com/docs/deploy-fastapi)
- [Render Postgres Guide](https://render.com/docs/databases)
- [Environment Variables](https://render.com/docs/environment-variables)

---

## 🆘 Necesitas Ayuda?

Si tienes problemas:
1. Revisa los **logs** en Render Dashboard
2. Verifica las **variables de entorno**
3. Prueba localmente primero: `uvicorn main:app --reload`
4. Consulta [Render Community](https://community.render.com/)
