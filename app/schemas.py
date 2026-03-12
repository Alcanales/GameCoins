from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import Optional, List, Literal
import re


class LoginRequest(BaseModel):
    username: str
    password: str

class BalanceAdjustment(BaseModel):
    email:     EmailStr
    amount:    int = Field(gt=0)
    operation: str
    motive:    Optional[str] = "Manual Admin Adjustment"

class CanjeRequest(BaseModel):
    email:      EmailStr
    monto:      int = Field(gt=0, description="QP a canjear (1 QP = 1 CLP)")
    cart_total: int = Field(gt=0, description=(
        "Total del carrito en CLP al momento del canje. "
        "El cupón se emite por min(monto, cart_total) para evitar pérdida "
        "de QP cuando el carrito vale menos que el monto solicitado."
    ))

class TokenResponse(BaseModel):
    access_token: str
    token_type:   str

# ── Buylist ───────────────────────────────────────────────────────────────────

class BuylistItem(BaseModel):
    name:          str
    qty:           int             = Field(default=1, ge=1, le=500)  # SCH-01 FIX: validar rango
    price_usd:     float
    price_credito: int
    price_cash:    int
    foil:          Optional[str]   = "normal"     # normal | foil | etched
    condition:     Optional[str]   = "near_mint"  # near_mint | lightly_played | ...
    version:       Optional[str]   = ""           # Extended Art, Showcase, etc.
    is_estaca:     Optional[bool]  = False        # True si es versión premium

    # Descarta campos de análisis interno que no se necesitan guardar
    model_config = {"extra": "ignore"}


# Regex RUT chileno: acepta con o sin puntos, con dígito o K verificador
# Ejemplos válidos: 12.345.678-9  /  12345678-9  /  12.345.678-K
_RUT_RE = re.compile(r"^\d{1,2}\.?\d{3}\.?\d{3}-[\dkK]$")


class BuylistCommitRequest(BaseModel):
    rut:                str
    email:              EmailStr
    payment_preference: Literal["credito", "cash", "mixto"]   # #14: solo valores válidos
    items:              List[BuylistItem]
    total_credito:      float = Field(ge=0)   # SCH-02 FIX: no permitir negativos
    total_cash:         float = Field(ge=0)   # SCH-02 FIX: no permitir negativos
    nombre:             Optional[str] = None  # SCH-FIX: campo del HTML (opcional)

    # Descartar cualquier campo extra que el HTML envíe (mismo patrón que BuylistItem)
    model_config = {"extra": "ignore"}

    @field_validator("rut")
    @classmethod
    def validate_rut(cls, v: str) -> str:
        """#15: Valida formato RUT chileno. Acepta con/sin puntos separadores."""
        cleaned = v.strip()
        if not _RUT_RE.match(cleaned):
            raise ValueError(
                "RUT inválido. Formato esperado: 12.345.678-9 o 12345678-9"
            )
        return cleaned.upper()   # normaliza K verificador a mayúscula
