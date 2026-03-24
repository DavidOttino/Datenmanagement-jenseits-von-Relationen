import os
import psycopg
import random
import string
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

def v2h(table_name: str, view_name: str = "H_VIEW"):
    print(f"[V2H] Creating view \"{view_name}\" from vertical table \"{table_name}\"...")
    try:
        with psycopg.connect(get_conn_str()) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT DISTINCT key FROM {table_name} WHERE key IS NOT NULL AND key != '' ORDER BY key;")
                attributes = [row[0] for row in cur.fetchall()]

                if not attributes:
                    print(f"[V2H] Error: No attributes found in {table_name}.")
                    return

                cur.execute(f"DROP VIEW IF EXISTS {view_name} CASCADE;")
                
                sql_parts = [f"CREATE VIEW {view_name} AS SELECT base.oid"]

                for i, attr in enumerate(attributes):
                    sql_parts.append(f", v{i}.val AS {attr}")
                
                sql_parts.append(f"\nFROM (SELECT DISTINCT oid FROM {table_name}) AS base")

                for i, attr in enumerate(attributes):
                    sql_parts.append(
                        f"LEFT JOIN {table_name} AS v{i} ON (base.oid = v{i}.oid AND v{i}.key = '{attr}')"
                    )

                sql = " ".join(sql_parts)

                sql += "\nORDER BY base.oid ASC;"

                cur.execute(sql)
                conn.commit()
                print(f"[V2H] View \"{view_name}\" successfully created with {len(attributes)} attributes.")
    except Exception as e:
        print(f"[SETUP] Error: {e}")


if __name__ == "__main__":
    v2h("v_toy")