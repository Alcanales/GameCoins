from pydantic import BaseModel, EmailStr, Field
from typing import Optional

# --- AUTH & ADMIN ---
class LoginRequest(BaseModel):
    username: str
    password: str

class BalanceAdjustment(BaseModel):
    email: EmailStr
    amount: int = Field(gt=0, description="Monto siempre positivo")
    operation: str # 'add' o 'subtract'
    motive: Optional[str] = "Manual Admin Adjustment"

# --- CANJE & PUBLIC ---
class CanjeRequest(BaseModel):
    email: EmailStr
    monto: int = Field(gt=0)

class TokenResponse(BaseModel):
    access_token: str
    token_type: str