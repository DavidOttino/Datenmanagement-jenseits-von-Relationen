import os

import psycopg
from dotenv import load_dotenv

load_dotenv()

def get_conn_str() -> str:
    return (
        f"dbname={os.getenv('DB_NAME', 'e_commerce')} "
        f"user={os.getenv('DB_USER', '')} "
        f"password={os.getenv('DB_PASS', '')} "
        f"host={os.getenv('DB_HOST', 'localhost')} "
        f"port={os.getenv('DB_PORT', '5432')}"
    )


def get_connection():
    return psycopg.connect(get_conn_str())
