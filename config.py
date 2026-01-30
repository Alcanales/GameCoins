import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    APP_NAME: str = "GameQuest API"
    ENV: str = os.getenv("ENV", "production")
    
    # Base de Datos (PostgreSQL)
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", 
        "postgresql://db_gamequest_user:0xewyvxGcc0KfRqdSV8JOTweQ3lxje0X@dpg-d4ti8u3uibrs73annjfg-a/gamecoins"
    )
    
    # Credenciales Maestras (ADMIN - Solo Bóveda)
    ADMIN_USER: str = os.getenv("ADMIN_USER", "Tomas_1_2_3")
    ADMIN_PASS: str = os.getenv("ADMIN_PASS", "GameQuest2025_1")
    
    # Credencial Pública (STORE - Para Widget)
    STORE_TOKEN: str = "gq_public_key_2025_secure"
    
    # Valores por defecto de Jumpseller (Se sobrescriben con DB)
    JUMPSELLER_API_BASE: str = "https://api.jumpseller.com/v1"
    
    # Lógica de Negocio
    MAINTENANCE_MODE: bool = False

    class Config:
        extra = "ignore"

settings = Settings()

# Fix Render PostgreSQL
if settings.DATABASE_URL.startswith("postgres://"):
    settings.DATABASE_URL = settings.DATABASE_URL.replace("postgres://", "postgresql://", 1)
