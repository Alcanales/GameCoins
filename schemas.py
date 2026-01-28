from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import List, Optional, Any

class ClienteSchema(BaseModel):
    nombre: str = Field(..., min_length=2)
    rut: str = Field(..., min_length=8)
    email: EmailStr
    metodo_pago: str
    notas: Optional[str] = ""

class CartaSchema(BaseModel):
    name: str
    set_code: Optional[str] = ""
    quantity: int = Field(..., gt=0)
    price_unit: float
    price_total: float

class BuylistSubmitRequest(BaseModel):
    cliente: ClienteSchema
    cartas: List[CartaSchema]
    total_clp: str
    total_gc: str

    @field_validator('cartas')
    def check_cartas_not_empty(cls, v):
        if not v:
            raise ValueError('La lista de cartas no puede estar vacía')
        return v

class CanjeRequest(BaseModel):
    email: EmailStr
    monto: int = Field(..., gt=0)

class UpdateRequest(BaseModel):
    email: EmailStr
    monto: int = Field(..., gt=0)
    accion: str # "sumar" | "restar"