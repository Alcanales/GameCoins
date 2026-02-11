import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str = os.getenv("DATABASE_URL")
    ADMIN_USER: str = os.getenv("ADMIN_USER", "Tomas_1_2_3")
    ADMIN_PASS: str = os.getenv("ADMIN_PASS", "GameQuest2025_1")
    STORE_TOKEN: str = os.getenv("STORE_TOKEN", "gq_public_key_2025_secure")
    MAINTENANCE_MODE_CANJE: bool = str(os.getenv("MAINTENANCE_MODE_CANJE", "false")).lower() == "true"
    JUMPSELLER_API_BASE: str = "https://api.jumpseller.com/v1"
    
    CASH_MULTIPLIER: float = float(os.getenv("CASH_MULTIPLIER", 0.45))     
    GAMECOIN_MULTIPLIER: float = float(os.getenv("GAMECOIN_MULTIPLIER", 0.55)) 

settings = Settings()