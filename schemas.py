from pydantic import BaseModel, EmailStr

class CanjeRequest(BaseModel):
    email: EmailStr
    monto: int

class ConfigRequest(BaseModel):
    api_token: str
    store_login: str
    hooks_token: str

class UpdateRequest(BaseModel):
    email: EmailStr
    monto: int
    accion: str 