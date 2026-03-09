from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from .config import settings

DATABASE_URL = settings.DATABASE_URL
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,       # detecta conexiones muertas antes de usarlas
    pool_size=5,              # incrementado para soportar múltiples requests concurrentes + jobs
    max_overflow=10,          # margen amplio para picos de tráfico (Total máx: 15)
    pool_recycle=300,         # reciclar cada 5 min (evita timeout por inactividad)
    pool_timeout=30,          # aumentado a 30s para evitar Drop silencioso bajo carga
    connect_args={
        "options":         "-csearch_path=public",
        "connect_timeout": 10,  # timeout de conexión a DB
    }
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()