# GameQuest API — v5.5

Backend del sistema de **Buylist y QuestPoints** para la tienda GameQuest.

## Stack
- FastAPI + Uvicorn/Gunicorn
- PostgreSQL (Render)
- SQLAlchemy 2.0
- Jumpseller storefront
- CardKingdom Buylist (precios externos)

## Endpoints Admin destacados
- GET /api/admin/catalog/export_manabox — Exportar stock JS en formato Manabox CSV
- POST /api/admin/staples/bulk — Importar staples masivo
- POST /api/admin/catalog/sync — Sincronizar catálogo JS
- POST /api/admin/sync_ck_prices — Sync precios CardKingdom
- GET /api/admin/users — Bóveda de clientes

## Variables de entorno
Ver .env.example para la lista completa.
validate_production_secrets() bloquea el arranque si hay credenciales inseguras.

## Deploy
render.yaml configurado con autoDeploy: true — push a main = deploy automático.
