import os
from typing import List
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # --- Configuración General ---
    APP_NAME: str = "GameQuest API"
    ENV: str = "production"
    
    # --- Base de Datos ---
    DATABASE_URL: str = "sqlite:///./local.db"
    
    # --- Jumpseller (Configurar en Render) ---
    JUMPSELLER_API_TOKEN: str = ""
    JUMPSELLER_STORE: str = ""
    JUMPSELLER_API_BASE: str = "https://api.jumpseller.com/v1"
    
    # --- Email (Configurar en Render) ---
    SMTP_EMAIL: str = ""
    SMTP_PASSWORD: str = ""
    TARGET_EMAIL: str = "contacto@gamequest.cl"
    
    # --- SISTEMA DE DOBLE SEGURIDAD ---
    # 1. Acceso Render (Dinámico, se configura en el Dashboard)
    ADMIN_USER: str = "Tomas1_2_3"
    ADMIN_PASS: str = "S3cur3P@ss"
    
    # 2. Acceso Maestro (Estático, siempre funciona)
    MASTER_USER: str = "Tomas_1_2_3"
    MASTER_PASS: str = "GameQuest2025_1"
    
    # --- Lógica de Negocio ---
    USD_TO_CLP: int = 1000
    CASH_MULTIPLIER: float = 0.45
    GAMECOIN_MULTIPLIER: float = 0.55
    MIN_PURCHASE_USD: float = 3.0
    
    STAKE_MIN_PRICE_FOR_STAKE: float = 20.0
    STAKE_RATIO_THRESHOLD: float = 2.5
    STAKE_MIN_SPREAD: float = 10.0
    
    STOCK_LIMIT_DEFAULT: int = 4
    STOCK_LIMIT_HIGH_DEMAND: int = 12
    
    HIGH_DEMAND_CARDS_LIST: str = "sol ring,arcane signet,command tower,mana crypt,the one ring,rhystic study,cyclonic rift"

    @property
    def high_demand_cards(self) -> List[str]:
        if not self.HIGH_DEMAND_CARDS_LIST: return []
        return [c.strip().lower() for c in self.HIGH_DEMAND_CARDS_LIST.split(",") if c.strip()]

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"

settings = Settings()

# Corrección automática para PostgreSQL en Render
if settings.DATABASE_URL.startswith("postgres://"):
    settings.DATABASE_URL = settings.DATABASE_URL.replace("postgres://", "postgresql://", 1)