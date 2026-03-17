import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

def get_conn_str():
    return (
        f"dbname={os.getenv('DB_NAME','e_commerce')} "
        f"user={os.getenv('DB_USER','')} "
        f"password={os.getenv('DB_PASS','')} "
        f"host={os.getenv('DB_HOST','localhost')} "
        f"port={os.getenv('DB_PORT','5432')}"
    )

def toy_setup():
    try:
        with psycopg.connect(get_conn_str()) as conn:
            with conn.cursor() as cur:
                #TODO
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    toy_setup()
