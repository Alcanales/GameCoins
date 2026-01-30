from pydantic import BaseModel
from typing import List, Optional

class UpdateRequest(BaseModel):
    email: str
    monto: int
    accion: str

class CanjeRequest(BaseModel):
    email: str
    monto: int

class ConfigRequest(BaseModel):
    api_token: str
    store_login: str
    hooks_token: str

class BuylistSubmitRequest(BaseModel):
    # Placeholder para futura expansión de Buylist
    pass
