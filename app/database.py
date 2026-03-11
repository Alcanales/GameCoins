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
    pool_size=3,               # free tier: máx 25 conexiones totales — mantener bajo
    max_overflow=2,            # overflow extra conservador
    pool_recycle=300,          # reciclar cada 5 min (evita timeout por inactividad)
    pool_timeout=20,           # no quedar esperando indefinidamente un slot
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
