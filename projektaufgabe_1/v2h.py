import os
import psycopg
import random
import string
from setup import get_conn_str
from dotenv import load_dotenv

load_dotenv()

def v2h(relation: str = "H"):
    view_name = f"{relation}_VIEW"
    v_int = f"{relation}_V_INT"
    v_text = f"{relation}_V_TEXT"
    print(f"[V2H] Creating view \"{view_name}\" from vertical tables {v_int} and {v_text}...")
    try:
        with psycopg.connect(get_conn_str()) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP VIEW IF EXISTS {view_name} CASCADE;")

                cur.execute(f"""
                    SELECT key, '{v_int}' as source_table FROM {v_int}
                    UNION
                    SELECT key, '{v_text}' as source_table FROM {v_text}
                    ORDER BY key;
                """)
                attributes = cur.fetchall()

                if not attributes:
                    print("[V2H] No data found in vertical tables.")
                    return

                cols = [f"v{i}.val AS {attr_name}" for i, (attr_name, _) in enumerate(attributes)]
                sql = f"CREATE VIEW {view_name} AS \nSELECT base.oid, " + ", ".join(cols)
                sql += f"\nFROM (SELECT oid FROM {v_int} UNION SELECT oid FROM {v_text}) AS base"

                for i, (attr_name, source_table) in enumerate(attributes):
                    sql += f"\nLEFT JOIN {source_table} AS v{i} ON (base.oid = v{i}.oid AND v{i}.key = '{attr_name}')"
                
                sql += "\nORDER BY base.oid ASC;"

                cur.execute(sql)
                conn.commit()
                print(f"[V2H] View \"{view_name}\" successfully created with {len(attributes)} attributes.")
    except Exception as e:
        print(f"[V2H] Error: {e}")


if __name__ == "__main__":
    v2h()