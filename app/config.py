import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)
    ADMIN_USER:  str = os.getenv("ADMIN_USER",  "admin")
    ADMIN_PASS:  str = os.getenv("ADMIN_PASS",  "password")
    STORE_TOKEN: str = os.getenv("STORE_TOKEN", "secret_token")

    # C-02 FIX: Token PÚBLICO para endpoints accedidos desde el browser del cliente
    # (account.liquid). DEBE ser diferente a STORE_TOKEN en producción.
    #
    # STORE_TOKEN       → solo /api/admin/* (Bearer, nunca expuesto al cliente)
    # PUBLIC_STORE_TOKEN → /api/canje + /api/*/balance/* (x-store-token desde theme)
    #
    # Generar con: python -c "import secrets; print(secrets.token_urlsafe(32))"
    # Configurar en Render Dashboard como variable de entorno separada.
    PUBLIC_STORE_TOKEN: str = os.getenv("PUBLIC_STORE_TOKEN", "gq_public_key_2025_secure")

    JS_LOGIN_CODE:       str = os.getenv("JS_LOGIN_CODE", "")
    JS_AUTH_TOKEN:       str = os.getenv("JS_AUTH_TOKEN", "")
    JUMPSELLER_API_BASE: str = "https://api.jumpseller.com/v1"
    # FIX A1: Token para verificar la firma HMAC-SHA256 de los webhooks de Jumpseller.
    # Obtener desde: Admin Jumpseller → Config → Notificaciones / Webhooks → Hooks Token.
    # Si está vacío, la verificación se omite (modo dev/fallback); en producción debe estar.
    JUMPSELLER_HOOKS_TOKEN: str = os.getenv("JUMPSELLER_HOOKS_TOKEN", "")

    USD_TO_CLP:       int   = int(os.getenv("USD_TO_CLP",       1000))
    MIN_PURCHASE_USD: float = float(os.getenv("MIN_PURCHASE_USD", 3.0))
    MIN_CANJE:        int   = int(os.getenv("MIN_CANJE",          100))

    # Buylist — factores de conversión USD → CLP (precio CK Buylist via Manabox)
    BUYLIST_FACTOR_CREDITO: int = int(os.getenv("BUYLIST_FACTOR_CREDITO", 500))
    BUYLIST_FACTOR_CASH:    int = int(os.getenv("BUYLIST_FACTOR_CASH",    450))

    # Condición — descuentos sobre precio NM de CK
    COND_NM:  float = float(os.getenv("COND_NM",  1.00))
    COND_LP:  float = float(os.getenv("COND_LP",  0.85))
    COND_MP:  float = float(os.getenv("COND_MP",  0.70))
    COND_HP:  float = float(os.getenv("COND_HP",  0.50))
    COND_DMG: float = float(os.getenv("COND_DMG", 0.25))

    # Estaca — multiplicador para versiones especiales (Foil, Etched, Borderless,
    # Extended Art, Showcase, etc.). Se aplica sobre la versión base más barata
    # de esa carta que aparezca en el mismo CSV.
    # Ej: 1.5 → oferta = precio_base_min × 1.5
    STAKE_MULTIPLIER: float = float(os.getenv("STAKE_MULTIPLIER", 1.5))

    # Buylist — control operacional
    BUYLIST_OPEN:             bool  = os.getenv("BUYLIST_OPEN", "true").lower() == "true"

    # Buylist — presupuesto diario de CASH en CLP.
    # 0.0 = sin límite (default, backward compatible).
    # Ejemplo: BUYLIST_DAILY_BUDGET_CASH=500000 → límite de $500.000 CLP/día en compras cash.
    # El conteo se reinicia automáticamente a medianoche (reloj del servidor).
    # Solo aplica al endpoint público (/api/public/commit_buylist).
    # El endpoint admin (/api/admin/commit_buylist) no tiene límite.
    BUYLIST_DAILY_BUDGET_CASH: float = float(os.getenv("BUYLIST_DAILY_BUDGET_CASH", 0.0))

    # SMTP
    SMTP_EMAIL:    str = os.getenv("SMTP_EMAIL",    "")
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")
    TARGET_EMAIL:  str = os.getenv("TARGET_EMAIL",  "contacto@gamequest.cl")

    CASH_ENABLED: bool = os.getenv("CASH_ENABLED", "true").lower() == "true"
    MIN_STOCK_NORMAL: int = int(os.getenv("MIN_STOCK_NORMAL", 4))
    MIN_STOCK_ALTA:   int = int(os.getenv("MIN_STOCK_ALTA",   8))
    MAINTENANCE_MODE_CANJE: bool = os.getenv("MAINTENANCE_MODE_CANJE", "false").lower() == "true"

    class Config:
        case_sensitive = True

    def validate_production_secrets(self) -> None:
        """
        FIX CRÍTICO: Bloquea el arranque si estamos en producción (DATABASE_URL
        configurado) y alguna credencial crítica tiene el valor default inseguro.

        Detectar producción: DATABASE_URL contiene una IP/host real (no vacío).
        Esta validación corre en lifespan() antes de aceptar requests.

        Credenciales críticas y sus defaults inseguros:
          ADMIN_PASS              → 'password'
          STORE_TOKEN             → 'secret_token'
          PUBLIC_STORE_TOKEN      → 'gq_public_key_2025_secure'
          JUMPSELLER_HOOKS_TOKEN  → vacío (webhooks aceptados sin verificar HMAC)

        Si alguna está en estado inseguro, el proceso termina con RuntimeError claro.
        En desarrollo local (DATABASE_URL vacío), los defaults se permiten para
        no bloquear el flujo de trabajo sin base de datos real.
        """
        if not self.DATABASE_URL:
            return

        # Credenciales con defaults inseguros
        _INSECURE = {
            "ADMIN_PASS":         (self.ADMIN_PASS,         "password"),
            "STORE_TOKEN":        (self.STORE_TOKEN,        "secret_token"),
            "PUBLIC_STORE_TOKEN": (self.PUBLIC_STORE_TOKEN, "gq_public_key_2025_secure"),
        }
        insecure_vars = [
            name for name, (val, default) in _INSECURE.items()
            if val == default
        ]

        # FIX A-02: JUMPSELLER_HOOKS_TOKEN es obligatorio en producción.
        # Sin él, _verify_jumpseller_hmac() retorna True para cualquier request
        # → cualquier agente externo puede disparar cashback o quemar cupones QP.
        if not self.JUMPSELLER_HOOKS_TOKEN:
            insecure_vars.append(
                "JUMPSELLER_HOOKS_TOKEN (vacío — webhooks sin verificación HMAC-SHA256)"
            )
        if insecure_vars:
            raise RuntimeError(
                "\n\n"
                "══════════════════════════════════════════════════════════\n"
                "  ARRANQUE BLOQUEADO — CREDENCIALES INSEGURAS DETECTADAS  \n"
                "══════════════════════════════════════════════════════════\n"
                "  Las siguientes variables tienen valores por defecto inseguros:\n"
                + "".join(f"    • {v}\n" for v in insecure_vars)
                + "\n"
                "  Configurar en Render Dashboard → Environment Variables.\n"
                "  Generar tokens seguros con:\n"
                '    python -c "import secrets; print(secrets.token_urlsafe(32))"\n'
                "══════════════════════════════════════════════════════════\n"
            )

settings = Settings()
