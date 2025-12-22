import os

class Config:
    DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./local.db")
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    JUMPSELLER_API_TOKEN = os.environ.get("JUMPSELLER_API_TOKEN", "")
    JUMPSELLER_STORE = os.environ.get("JUMPSELLER_STORE", "")
    JUMPSELLER_API_BASE = "https://api.jumpseller.com/v1"
    JUMPSELLER_HOOKS_TOKEN = os.environ.get("JUMPSELLER_HOOKS_TOKEN", "")

    SMTP_EMAIL = os.environ.get("SMTP_EMAIL", "")
    SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
    TARGET_EMAIL = "contacto@gamequest.cl"

    ADMIN_USER = os.environ.get("ADMIN_USER", "Tomas_1_2_3")
    ADMIN_PASS = os.environ.get("ADMIN_PASSWORD", "GameQuest2025_1")

    USD_TO_CLP = int(os.environ.get("USD_TO_CLP", 1000))
    CASH_MULTIPLIER = float(os.environ.get("CASH_MULTIPLIER", 0.40))
    GAMECOIN_MULTIPLIER = float(os.environ.get("GAMECOIN_MULTIPLIER", 0.50))
    MIN_PURCHASE_USD = float(os.environ.get("MIN_PURCHASE_USD", 1.19))
    STAKE_PRICE_THRESHOLD = float(os.environ.get("STAKE_PRICE_THRESHOLD", 10.0))

    STOCK_LIMIT_DEFAULT = int(os.environ.get("STOCK_LIMIT_DEFAULT", 8))
    STOCK_LIMIT_HIGH_DEMAND = int(os.environ.get("STOCK_LIMIT_HIGH_DEMAND", 20))
    
    HIGH_DEMAND_CARDS = [
        "sol ring", "arcane signet", "command tower", "swords to plowshares", 
        "counterspell", "cultivate", "kodama's reach", "dark ritual"
    ]

settings = Config()
