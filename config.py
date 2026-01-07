import os

class Config:
    # Base de Datos
    DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./local.db")
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    # Credenciales Jumpseller API
    JUMPSELLER_API_TOKEN = os.environ.get("JUMPSELLER_API_TOKEN", "")
    JUMPSELLER_STORE = os.environ.get("JUMPSELLER_STORE", "")
    JUMPSELLER_API_BASE = "https://api.jumpseller.com/v1"
    JUMPSELLER_HOOKS_TOKEN = os.environ.get("JUMPSELLER_HOOKS_TOKEN", "")

    # Credenciales Admin y Correo
    SMTP_EMAIL = os.environ.get("SMTP_EMAIL", "")
    SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
    TARGET_EMAIL = os.environ.get("TARGET_EMAIL", "contacto@gamequest.cl")
    ADMIN_USER = os.environ.get("ADMIN_USER", "Admin")
    ADMIN_PASS = os.environ.get("ADMIN_PASSWORD", "ChangeMe123!")

    # --- CONSTANTES DE NEGOCIO 
    USD_TO_CLP = int(os.environ.get("USD_TO_CLP", 1000))
    CASH_MULTIPLIER = float(os.environ.get("CASH_MULTIPLIER", 0.45))
    GAMECOIN_MULTIPLIER = float(os.environ.get("GAMECOIN_MULTIPLIER", 0.55))
    
    MIN_PURCHASE_USD = float(os.environ.get("MIN_PURCHASE_USD", 3.0))
    
    # Lógica de Estacas Avanzada
    STAKE_MIN_PRICE_FOR_STAKE = float(os.environ.get("STAKE_MIN_PRICE_FOR_STAKE", 20.0))
    STAKE_RATIO_THRESHOLD = float(os.environ.get("STAKE_RATIO_THRESHOLD", 2.5))
    MIN_STAKE_SPREAD = float(os.environ.get("MIN_STAKE_SPREAD", 10.0))
    NONFOIL_MAX_TYPICAL_FOR_STAKE = float(os.environ.get("NONFOIL_MAX_TYPICAL_FOR_STAKE", 20.0))

    # Stock
    STOCK_LIMIT_DEFAULT = int(os.environ.get("STOCK_LIMIT_DEFAULT", 8))
    STOCK_LIMIT_HIGH_DEMAND = int(os.environ.get("STOCK_LIMIT_HIGH_DEMAND", 20))
    
    HIGH_DEMAND_CARDS = [
        "sol ring", "arcane signet", "command tower", "swords to plowshares", 
        "counterspell", "cultivate", "kodama's reach", "dark ritual", 
        "rhystic study", "cyclonic rift", "dockside extortionist", "esper sentinel",
        "mana crypt", "jeweled lotus" "blood pet"
    ]

settings = Config()