import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Infraestructura
    DATABASE_URL: str = os.getenv("DATABASE_URL")
    ADMIN_USER: str = os.getenv("ADMIN_USER") # Configura esto en Render
    ADMIN_PASS: str = os.getenv("ADMIN_PASS") # Configura esto en Render
    STORE_TOKEN: str = os.getenv("STORE_TOKEN") # Configura esto en Render
    JUMPSELLER_API_BASE: str = "https://api.jumpseller.com/v1"
    
    # Lógica de Negocio (Precios y Canje)
    CASH_MULTIPLIER: float = float(os.getenv("CASH_MULTIPLIER", 0.45))
    GAMECOIN_MULTIPLIER: float = float(os.getenv("GAMECOIN_MULTIPLIER", 0.55))
    MIN_CANJE: int = int(os.getenv("MIN_CANJE", 3000))
    
    # Límites de Stock
    STOCK_LIMIT_DEFAULT: int = int(os.getenv("STOCK_LIMIT_DEFAULT", 4))
    STOCK_LIMIT_HIGH_DEMAND: int = int(os.getenv("STOCK_LIMIT_HIGH_DEMAND", 8))
    
    # Filtros de Estacas
    STAKE_RATIO_THRESHOLD: float = float(os.getenv("STAKE_RATIO_THRESHOLD", 2.5))
    STAKE_DIFF_THRESHOLD: float = float(os.getenv("STAKE_DIFF_THRESHOLD", 10.0))

    # Configuración de Email (SMTP)
    SMTP_SERVER: str = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", 587))
    SMTP_USER: str = os.getenv("SMTP_EMAIL") # Tu correo de empresa
    SMTP_PASS: str = os.getenv("SMTP_PASSWORD") # Tu App Password de Google
    TARGET_EMAIL: str = os.getenv("TARGET_EMAIL") # Correo que recibe las listas

settings = Settings()