import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Toma la URL de Render y solo cambia postgres:// por postgresql://
    DATABASE_URL: str = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)
    
    ADMIN_USER: str = os.getenv("ADMIN_USER", "admin")
    ADMIN_PASS: str = os.getenv("ADMIN_PASS", "change_me")
    STORE_TOKEN: str = os.getenv("STORE_TOKEN", "")
    
    JS_LOGIN_CODE: str = os.getenv("JS_LOGIN_CODE", "")
    JS_AUTH_TOKEN: str = os.getenv("JS_AUTH_TOKEN", "")
    JUMPSELLER_API_BASE: str = "https://api.jumpseller.com/v1"

    USD_TO_CLP: int = int(os.getenv("USD_TO_CLP", 1000))
    MIN_CANJE: int = int(os.getenv("MIN_CANJE", 100))
    MIN_PURCHASE_USD: float = float(os.getenv("MIN_PURCHASE_USD", 3.0))
    MAINTENANCE_MODE_CANJE: bool = str(os.getenv("MAINTENANCE_MODE_CANJE", "false")).lower() == "true"
    
    SMTP_EMAIL: str = os.getenv("SMTP_EMAIL", "")
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")
    TARGET_EMAIL: str = os.getenv("TARGET_EMAIL", "")

    class Config:
        case_sensitive = True

settings = Settings()
