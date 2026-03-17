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
    print("[SETUP] Starting toy_setup...")
    try:
        with psycopg.connect(get_conn_str()) as conn:
            with conn.cursor() as cur:
                # --- Teil a ---
                cur.execute("DROP TABLE IF EXISTS H_toy CASCADE;")
                cur.execute("CREATE TABLE H_toy (oid INT, a1 TEXT, a2 TEXT, a3 INT);")
                print("[SETUP] Created table H_toy")

                data_h = [
                    (1, 'a', 'b', None),
                    (2, None, 'c', 2),
                    (3, None, None, 3),
                    (4, None, None, None)
                ]

                cur.executemany("INSERT INTO H_toy VALUES (%s, %s, %s, %s)", data_h)
                print("[SETUP] Populated table H_toy")

                # --- Teil b ---
                cur.execute("DROP TABLE IF EXISTS V_toy CASCADE;")
                cur.execute("CREATE TABLE V_toy (oid INT, key TEXT, val TEXT);")
                print("[SETUP] Created table V_toy")

                data_v = [
                    (1, 'a1', 'a'), (1, 'a2', 'b'),
                    (2, 'a2', 'c'), (2, 'a3', '2'),
                    (3, 'a3', '3'),
                    (4, None, None)
                ]

                cur.executemany("INSERT INTO V_toy VALUES (%s, %s, %s)", data_v)
                print("[SETUP] Populated table V_toy")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    toy_setup()
