from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List


class LoginRequest(BaseModel):
    username: str
    password: str

class BalanceAdjustment(BaseModel):
    email:     EmailStr
    amount:    int = Field(gt=0)
    operation: str
    motive:    Optional[str] = "Manual Admin Adjustment"

class CanjeRequest(BaseModel):
    email: EmailStr
    monto: int = Field(gt=0)

class TokenResponse(BaseModel):
    access_token: str
    token_type:   str

# ── Buylist ───────────────────────────────────────────────────────────────────

class BuylistItem(BaseModel):
    name:          str
    qty:           int             = 1
    price_usd:     float
    price_credito: int
    price_cash:    int
    # v4: foil y condición — opcionales para compatibilidad con CSV sin esas columnas
    foil:          Optional[str]   = "normal"    # normal | foil | etched
    condition:     Optional[str]   = "near_mint" # near_mint | lightly_played | ...

    # Permite recibir campos extra del frontend sin romper validación
    model_config = {"extra": "ignore"}


class BuylistCommitRequest(BaseModel):
    rut:                str
    email:              EmailStr
    payment_preference: str           # credito | cash | mixto
    items:              List[BuylistItem]
    total_credito:      float
    total_cash:         float
