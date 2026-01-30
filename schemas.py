from pydantic import BaseModel
from typing import List, Optional

class CartaItem(BaseModel):
    name: str
    quantity: int
    set_code: str
    foil: Optional[str] = None
    purchase_price: float
    mkt: float
    stock_tienda: int
    cat: str
    clean_name: str
    cash_clp: int

class ClienteInfo(BaseModel):
    nombre: str
    email: str
    rut: str
    metodo_pago: str

class BuylistSubmitRequest(BaseModel):
    cliente: ClienteInfo
    cartas: List[CartaItem]
    total_clp: str
    total_gc: str

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
