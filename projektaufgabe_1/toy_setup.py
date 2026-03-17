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
                cur.execute("DROP TABLE IF EXISTS H_toy CASCADE;")
                cur.execute("CREATE TABLE H_toy (oid INT, a1 TEXT, a2 TEXT, a3 INT);")
                print("Created table H_toy")

                data_h = [
                    (1, 'a', 'b', None),
                    (2, None, 'c', 2),
                    (3, None, None, 3),
                    (4, None, None, None)
                ]

                cur.executemany("INSERT INTO H_toy VALUES (%s, %s, %s, %s)", data_h)
                print("Populated table H_toy")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    toy_setup()
