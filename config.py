import os
from typing import List, Optional
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    APP_NAME: str = "GameQuest API"
    ENV: str = os.getenv("ENV", "production")
    
    # --- BASE DE DATOS ---
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", 
        "postgresql://db_gamequest_user:0xewyvxGcc0KfRqdSV8JOTweQ3lxje0X@dpg-d4ti8u3uibrs73annjfg-a/gamecoins"
    )
    
    # --- CREDENCIALES DINÁMICAS (Variables de clase, no constantes) ---
    # Se inician con valores por defecto o de entorno, pero pueden cambiar en ejecución
    JUMPSELLER_API_TOKEN: str = os.getenv("JUMPSELLER_API_TOKEN", "")
    JUMPSELLER_STORE: str = os.getenv("JUMPSELLER_STORE", "")
    JUMPSELLER_HOOKS_TOKEN: str = os.getenv("JUMPSELLER_HOOKS_TOKEN", "")
    JUMPSELLER_API_BASE: str = "https://api.jumpseller.com/v1"
    
    # --- SEGURIDAD ---
    ADMIN_USER: str = os.getenv("ADMIN_USER", "Tomas_1_2_3")
    ADMIN_PASS: str = os.getenv("ADMIN_PASS", "GameQuest2025_1")
    MASTER_USER: str = os.getenv("MASTER_USER", "Master_1_2_3")
    MASTER_PASS: str = os.getenv("MASTER_PASS", "GameQuest2025_1")
    
    MAINTENANCE_MODE_CANJE: bool = os.getenv("MAINTENANCE_MODE_CANJE", "False").lower() == "true"
    
    # --- LÓGICA DE NEGOCIO ---
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
