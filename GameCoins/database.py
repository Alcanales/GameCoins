import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if DATABASE_URL.endswith("/postgres"):
    DATABASE_URL = DATABASE_URL.replace("/postgres", "/db_gamequest")

engine = create_engine(
    DATABASE_URL, 
    pool_pre_ping=True,
    connect_args={"options": "-csearch_path=public"} # <--- LA MAGIA ESTÁ AQUÍ
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()