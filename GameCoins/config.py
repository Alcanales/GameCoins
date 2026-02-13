import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # --- INFRAESTRUCTURA ---
    DATABASE_URL: str = os.getenv("DATABASE_URL")
    
    # --- SEGURIDAD ---
    ADMIN_USER: str = os.getenv("ADMIN_USER", "admin")
    ADMIN_PASS: str = os.getenv("ADMIN_PASS", "change_me_please")
    STORE_TOKEN: str = os.getenv("STORE_TOKEN")
    
    # --- ECONOMÍA Y PRECIOS ---
    USD_TO_CLP: int = int(os.getenv("USD_TO_CLP", 1000))
    
    # Multiplicadores Base (Referencia)
    CASH_MULTIPLIER: float = float(os.getenv("CASH_MULTIPLIER", 0.45))
    GAMECOIN_MULTIPLIER: float = float(os.getenv("GAMECOIN_MULTIPLIER", 0.55)) 
    
    # --- CONFIGURACIÓN DE CANJE ---
    MIN_CANJE: int = int(os.getenv("MIN_CANJE", 3000))
    MAINTENANCE_MODE_CANJE: bool = str(os.getenv("MAINTENANCE_MODE_CANJE", "false")).lower() == "true"
    
    # --- JUMPSELLER INTEGRATION ---
    JUMPSELLER_LOGIN: str = os.getenv("JUMPSELLER_LOGIN", "")
    JUMPSELLER_API_TOKEN: str = os.getenv("JUMPSELLER_API_TOKEN", "")
    JUMPSELLER_API_BASE: str = "https://api.jumpseller.com/v1"

    # --- MAIL ---
    SMTP_SERVER: str = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", 587))
    SMTP_EMAIL: str = os.getenv("SMTP_EMAIL", "")
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")
    TARGET_EMAIL: str = os.getenv("TARGET_EMAIL", "")

    class Config:
        case_sensitive = True

settings = Settings()