from sqlalchemy import create_engine
from urllib.parse import quote_plus

DB_USER = "root"
DB_PASSWORD = quote_plus("nabil@123")
DB_HOST = "host.docker.internal"   # 🔥 THIS IS THE KEY FIX
# DB_HOST= "127.0.0.1"
DB_PORT = 3306
DB_NAME = "finance_banking"

def get_engine():
    return create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        pool_pre_ping=True
    )