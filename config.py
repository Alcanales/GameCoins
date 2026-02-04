import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Infraestructura & Base de Datos
    DATABASE_URL: str = os.getenv("DATABASE_URL")
    
    # Credenciales Administrativas (Para endpoints /admin)
    ADMIN_USER: str = os.getenv("ADMIN_USER", "Tomas_1_2_3")
    ADMIN_PASS: str = os.getenv("ADMIN_PASS", "GameQuest2025_1")
    
    # Seguridad Pública (Token para Jumpseller)
    STORE_TOKEN: str = os.getenv("STORE_TOKEN", "gq_public_key_2025_secure")
    
    # Kill-Switch (Apagado de Emergencia)
    MAINTENANCE_MODE_CANJE: bool = str(os.getenv("MAINTENANCE_MODE_CANJE", "false")).lower() == "true"
    
    # Constantes
    JUMPSELLER_API_BASE: str = "https://api.jumpseller.com/v1"
    
    # Agregadas para completitud (de .env)
    GAMECOIN_MULTIPLIER: float = float(os.getenv("GAMECOIN_MULTIPLIER", 0.55))
    MIN_PURCHASE_USD: int = int(os.getenv("MIN_PURCHASE_USD", 3))

settings = Settings()