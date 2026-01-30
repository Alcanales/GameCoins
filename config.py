import os
from typing import List
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    APP_NAME: str = "GameQuest API"
    ENV: str = os.getenv("ENV", "production")
    
    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./local.db")
    
    # Integraciones
    JUMPSELLER_API_TOKEN: str = os.getenv("JUMPSELLER_API_TOKEN", "")
    JUMPSELLER_STORE: str = os.getenv("JUMPSELLER_STORE", "")
    JUMPSELLER_API_BASE: str = "https://api.jumpseller.com/v1"
    
    SMTP_EMAIL: str = os.getenv("SMTP_EMAIL", "")
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")
    TARGET_EMAIL: str = os.getenv("TARGET_EMAIL", "contacto@gamequest.cl")
    
    # Seguridad
    ADMIN_USER: str = os.getenv("ADMIN_USER", "Tomas_1_2_3")
    ADMIN_PASS: str = os.getenv("ADMIN_PASS", "GameQuest2025_1")
    MASTER_USER: str = os.getenv("MASTER_USER", "Master_1_2_3")
    MASTER_PASS: str = os.getenv("MASTER_PASS", "GameQuest2025_1")
    
    # --- KILL-SWITCH: MODO MANTENIMIENTO ---
    # Si True, bloquea endpoints de canje y buylist.
    MAINTENANCE_MODE_CANJE: bool = os.getenv("MAINTENANCE_MODE_CANJE", "False").lower() == "true"
    
    # Lógica de Negocio
    USD_TO_CLP: int = 1000
    CASH_MULTIPLIER: float = 0.45
    GAMECOIN_MULTIPLIER: float = 0.55
    MIN_PURCHASE_USD: float = 3.0
    STAKE_MIN_PRICE_FOR_STAKE: float = 20.0
    STAKE_RATIO_THRESHOLD: float = 2.5
    
    STOCK_LIMIT_DEFAULT: int = 4
    STOCK_LIMIT_HIGH_DEMAND: int = 12
    HIGH_DEMAND_CARDS_LIST: str = "sol ring,arcane signet,command tower,mana crypt,the one ring,rhystic study,cyclonic rift"

    @property
    def high_demand_cards(self) -> List[str]:
        if not self.HIGH_DEMAND_CARDS_LIST: return []
        return [c.strip().lower() for c in self.HIGH_DEMAND_CARDS_LIST.split(",") if c.strip()]

    class Config:
        case_sensitive = True
        extra = "ignore"

settings = Settings()

# Fix para Render PostgreSQL
if settings.DATABASE_URL.startswith("postgres://"):
    settings.DATABASE_URL = settings.DATABASE_URL.replace("postgres://", "postgresql://", 1)