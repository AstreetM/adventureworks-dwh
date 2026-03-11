# etl/utils.py
import urllib.parse
from sqlalchemy import create_engine
from datetime import datetime

def get_engine(params: dict):
    """Crée une connexion SQLAlchemy vers SQL Server."""
    conn_str = urllib.parse.quote_plus(
        f"DRIVER={params['driver']};"
        f"SERVER={params['server']};"
        f"DATABASE={params['database']};"
        f"UID={params['username']};"
        f"PWD={params['password']};"
    )
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={conn_str}",
        fast_executemany=True
    )

def log(message: str):
    """Log simple avec timestamp."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}")