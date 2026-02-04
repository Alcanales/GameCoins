from pydantic import BaseModel, EmailStr, Field

class CanjeRequest(BaseModel):
    email: EmailStr
    monto: int = Field(gt=0)  # Validación >0

class ConfigRequest(BaseModel):
    api_token: str
    store_login: str
    hooks_token: str

class UpdateRequest(BaseModel):
    email: EmailStr
    monto: int
    accion: str 