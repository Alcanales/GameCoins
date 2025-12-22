import os

class Config:
    # --- BASE DE DATOS (Compatibilidad Render/Local) ---
    DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./local.db")
    # Corrección automática para SQLAlchemy en Render
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    # --- JUMPSELLER API ---
    JUMPSELLER_API_TOKEN = os.environ.get("JUMPSELLER_API_TOKEN", "")
    JUMPSELLER_STORE = os.environ.get("JUMPSELLER_STORE", "")
    JUMPSELLER_API_BASE = "https://api.jumpseller.com/v1"
    JUMPSELLER_HOOKS_TOKEN = os.environ.get("JUMPSELLER_HOOKS_TOKEN", "")

    # --- CORREO (Notificaciones) ---
    SMTP_EMAIL = os.environ.get("SMTP_EMAIL", "")
    SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
    TARGET_EMAIL = os.environ.get("TARGET_EMAIL", "contacto@gamequest.cl")

    # --- SEGURIDAD (Admin) ---
    ADMIN_USER = os.environ.get("ADMIN_USER", "Admin")
    ADMIN_PASS = os.environ.get("ADMIN_PASSWORD", "ChangeMe123!")

    USD_TO_CLP = int(os.environ.get("USD_TO_CLP", 1050))
    
    # Multiplicadores de compra
    CASH_MULTIPLIER = float(os.environ.get("CASH_MULTIPLIER", 0.40))       # 40% del valor
    GAMECOIN_MULTIPLIER = float(os.environ.get("GAMECOIN_MULTIPLIER", 0.50)) # 50% del valor
    
    # Filtros de compra
    MIN_PURCHASE_USD = float(os.environ.get("MIN_PURCHASE_USD", 1.19))
    STAKE_PRICE_THRESHOLD = float(os.environ.get("STAKE_PRICE_THRESHOLD", 10.0))

    STOCK_LIMIT_DEFAULT = int(os.environ.get("STOCK_LIMIT_DEFAULT", 8))
    
    STOCK_LIMIT_HIGH_DEMAND = int(os.environ.get("STOCK_LIMIT_HIGH_DEMAND", 20))
    
    HIGH_DEMAND_CARDS = [
        "sol ring", "arcane signet", "command tower", "swords to plowshares", 
        "counterspell", "cultivate", "kodama's reach", "dark ritual", 
        "rhystic study", "cyclonic rift", "dockside extortionist"
    ]

settings = Config()
