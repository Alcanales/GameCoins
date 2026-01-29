import os
from typing import List
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # App Config
    APP_NAME: str = "GameQuest API"
    ENV: str = "production"
    
    # Database
    DATABASE_URL: str = "sqlite:///./local.db"
    
    # Jumpseller (Configurado en Render)
    JUMPSELLER_API_TOKEN: str = ""
    JUMPSELLER_STORE: str = ""
    JUMPSELLER_API_BASE: str = "https://api.jumpseller.com/v1"
    
    # SMTP / Email (Configurado en Render)
    SMTP_EMAIL: str = ""
    SMTP_PASSWORD: str = ""
    TARGET_EMAIL: str = "contacto@gamequest.cl"
    
    # Security
    ADMIN_USER: str = "Tomas1_2_3"
    ADMIN_PASS: str = "GameQuest2025_1"
    
    # Business Logic
    USD_TO_CLP: int = 1000
    CASH_MULTIPLIER: float = 0.45
    GAMECOIN_MULTIPLIER: float = 0.55
    MIN_PURCHASE_USD: float = 3.0
    
    # Risk Management
    STAKE_MIN_PRICE_FOR_STAKE: float = 20.0
    STAKE_RATIO_THRESHOLD: float = 2.5
    STAKE_MIN_SPREAD: float = 10.0
    
    # Stock Logic
    STOCK_LIMIT_DEFAULT: int = 4
    STOCK_LIMIT_HIGH_DEMAND: int = 12
    
    HIGH_DEMAND_CARDS_LIST: str = "sol ring,arcane signet,command tower,mana crypt,the one ring,rhystic study,cyclonic rift"

    # Propiedad corregida y robusta
    @property
    def high_demand_cards(self) -> List[str]:
        if not self.HIGH_DEMAND_CARDS_LIST:
            return []
        return [c.strip().lower() for c in self.HIGH_DEMAND_CARDS_LIST.split(",") if c.strip()]

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"

settings = Settings()

# Corrección automática para Postgres en Render
if settings.DATABASE_URL.startswith("postgres://"):
    settings.DATABASE_URL = settings.DATABASE_URL.replace("postgres://", "postgresql://", 1)