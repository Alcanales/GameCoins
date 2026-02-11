import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Infraestructura leída de Render Environment
    DATABASE_URL: str = os.getenv("DATABASE_URL")
    ADMIN_USER: str = os.getenv("ADMIN_USER")
    ADMIN_PASS: str = os.getenv("ADMIN_PASS")
    STORE_TOKEN: str = os.getenv("STORE_TOKEN")
    
    # Lógica de Negocio
    CASH_MULTIPLIER: float = float(os.getenv("CASH_MULTIPLIER", 0.45))
    GAMECOIN_MULTIPLIER: float = float(os.getenv("GAMECOIN_MULTIPLIER", 0.55))
    [cite_start]MIN_CANJE: int = int(os.getenv("MIN_CANJE", 3000)) # Arregla el error 500 [cite: 4]
    
    # Límites de Stock
    STOCK_LIMIT_DEFAULT: int = int(os.getenv("STOCK_LIMIT_DEFAULT", 4))
    STOCK_LIMIT_HIGH_DEMAND: int = int(os.getenv("STOCK_LIMIT_HIGH_DEMAND", 8))
    
    # APIs Externas
    JUMPSELLER_API_BASE: str = "https://api.jumpseller.com/v1"
    MAINTENANCE_MODE_CANJE: bool = str(os.getenv("MAINTENANCE_MODE_CANJE", "false")).lower() == "true"

settings = Settings()