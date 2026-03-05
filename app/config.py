import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)
    ADMIN_USER:  str = os.getenv("ADMIN_USER",  "admin")
    ADMIN_PASS:  str = os.getenv("ADMIN_PASS",  "password")
    STORE_TOKEN: str = os.getenv("STORE_TOKEN", "secret_token")

    JS_LOGIN_CODE:       str = os.getenv("JS_LOGIN_CODE", "")
    JS_AUTH_TOKEN:       str = os.getenv("JS_AUTH_TOKEN", "")
    JUMPSELLER_API_BASE: str = "https://api.jumpseller.com/v1"

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
    BUYLIST_DAILY_BUDGET_USD: float = float(os.getenv("BUYLIST_DAILY_BUDGET_USD", 500.0))

    # SMTP
    SMTP_EMAIL:    str = os.getenv("SMTP_EMAIL",    "")
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")
    TARGET_EMAIL:  str = os.getenv("TARGET_EMAIL",  "contacto@gamequest.cl")

    MAINTENANCE_MODE_CANJE: bool = os.getenv("MAINTENANCE_MODE_CANJE", "false").lower() == "true"

    class Config:
        case_sensitive = True

settings = Settings()
