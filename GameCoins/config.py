import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # --- INFRAESTRUCTURA ---
    _raw_db_url: str = os.getenv("DATABASE_URL", "")

    @property
    def DATABASE_URL(self) -> str:
        # Fix automático para Render/Postgres
        url = self._raw_db_url
        if url and url.endswith("/postgres"):
            return url.replace("/postgres", "/db_gamequest")
        return url
    
    # --- SEGURIDAD ---
    ADMIN_USER: str = os.getenv("ADMIN_USER", "admin")
    ADMIN_PASS: str = os.getenv("ADMIN_PASS", "change_me")
    STORE_TOKEN: str = os.getenv("STORE_TOKEN")
    
    # --- JUMPSELLER (RENOMBRADO PARA EVITAR ERRORES) ---
    # JS_LOGIN_CODE: Es el código que aparece en la casilla "Login"
    JS_LOGIN_CODE: str = os.getenv("JS_LOGIN_CODE", "")
    # JS_AUTH_TOKEN: Es el código que aparece en la casilla "Auth Token"
    JS_AUTH_TOKEN: str = os.getenv("JS_AUTH_TOKEN", "")
    
    JUMPSELLER_API_BASE: str = "https://api.jumpseller.com/v1"

    # --- ECONOMÍA ---
    USD_TO_CLP: int = int(os.getenv("USD_TO_CLP", 1000))
    MIN_CANJE: int = int(os.getenv("MIN_CANJE", 100)) # Mínimo $100 solicitado
    
    # --- OTROS ---
    MAINTENANCE_MODE_CANJE: bool = str(os.getenv("MAINTENANCE_MODE_CANJE", "false")).lower() == "true"
    
    # Configuración de Buylist
    MIN_PURCHASE_USD: float = float(os.getenv("MIN_PURCHASE_USD", 3.0))

    class Config:
        case_sensitive = True

settings = Settings()