import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)
    ADMIN_USER: str = os.getenv("ADMIN_USER", "admin")
    ADMIN_PASS: str = os.getenv("ADMIN_PASS", "password")
    STORE_TOKEN: str = os.getenv("STORE_TOKEN", "secret_token")
    JS_LOGIN_CODE: str = os.getenv("JS_LOGIN_CODE", "")
    JS_AUTH_TOKEN: str = os.getenv("JS_AUTH_TOKEN", "")
    JUMPSELLER_API_BASE: str = "https://api.jumpseller.com/v1"
    USD_TO_CLP: int = int(os.getenv("USD_TO_CLP", 1000))
    MIN_PURCHASE_USD: float = float(os.getenv("MIN_PURCHASE_USD", 3.0))

    class Config:
        case_sensitive = True

settings = Settings()