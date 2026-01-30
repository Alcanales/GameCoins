import os
from typing import List
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    APP_NAME: str = "GameQuest API"
    ENV: str = os.getenv("ENV", "production")
    
    # Base de Datos
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", 
        "postgresql://db_gamequest_user:0xewyvxGcc0KfRqdSV8JOTweQ3lxje0X@dpg-d4ti8u3uibrs73annjfg-a/gamecoins"
    )
    
    # Credenciales Jumpseller (Dinámicas)
    JUMPSELLER_API_TOKEN: str = ""
    JUMPSELLER_STORE: str = ""
    JUMPSELLER_HOOKS_TOKEN: str = ""
    JUMPSELLER_API_BASE: str = "https://api.jumpseller.com/v1"
    
    # SEGURIDAD ADMIN (Para la Bóveda - Privado)
    ADMIN_USER: str = os.getenv("ADMIN_USER", "Tomas_1_2_3")
    ADMIN_PASS: str = os.getenv("ADMIN_PASS", "GameQuest2025_1")
    
    STORE_TOKEN: str = "gq_public_key_2025_secure"
    
    MAINTENANCE_MODE_CANJE: bool = False
    USD_TO_CLP: int = 1000
    CASH_MULTIPLIER: float = 0.45
    STAKE_MIN_PRICE_FOR_STAKE: float = 20.0
    STAKE_RATIO_THRESHOLD: float = 2.5
    STOCK_LIMIT_DEFAULT: int = 4

    class Config:
        case_sensitive = True
        extra = "ignore"

settings = Settings()

if settings.DATABASE_URL.startswith("postgres://"):
    settings.DATABASE_URL = settings.DATABASE_URL.replace("postgres://", "postgresql://", 1)
