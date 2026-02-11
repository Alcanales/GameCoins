import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Infraestructura
    DATABASE_URL: str = os.getenv("DATABASE_URL")
    ADMIN_USER: str = os.getenv("ADMIN_USER")
    ADMIN_PASS: str = os.getenv("ADMIN_PASS")
    STORE_TOKEN: str = os.getenv("STORE_TOKEN")
    
    # Precios y Multiplicadores
    CASH_MULTIPLIER: float = float(os.getenv("CASH_MULTIPLIER", 0.45))
    GAMECOIN_MULTIPLIER: float = float(os.getenv("GAMECOIN_MULTIPLIER", 0.55)) # Para Buylist (Ventas del cliente)
    
    # --- NUEVA VARIABLE PARA FIDELIZACIÓN ---
    # Esto leerá el 0.01 de tu archivo .env
    LOYALTY_ACCUMULATION_RATE: float = float(os.getenv("LOYALTY_ACCUMULATION_RATE", 0.01)) 
    
    # Configuración de Canje
    MIN_CANJE: int = int(os.getenv("MIN_CANJE", 3000))
    
    # Stock y Estacas
    STOCK_LIMIT_DEFAULT: int = int(os.getenv("STOCK_LIMIT_DEFAULT", 4))
    STOCK_LIMIT_HIGH_DEMAND: int = int(os.getenv("STOCK_LIMIT_HIGH_DEMAND", 8))
    STAKE_RATIO_THRESHOLD: float = float(os.getenv("STAKE_RATIO_THRESHOLD", 2.5))
    STAKE_DIFF_THRESHOLD: float = float(os.getenv("STAKE_DIFF_THRESHOLD", 10.0))

    # APIs Externas
    JUMPSELLER_API_BASE: str = "https://api.jumpseller.com/v1"
    MAINTENANCE_MODE_CANJE: bool = str(os.getenv("MAINTENANCE_MODE_CANJE", "false")).lower() == "true"

    # Correo
    SMTP_SERVER: str = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", 587))
    SMTP_USER: str = os.getenv("SMTP_EMAIL")
    SMTP_PASS: str = os.getenv("SMTP_PASSWORD")
    TARGET_EMAIL: str = os.getenv("TARGET_EMAIL")

settings = Settings()