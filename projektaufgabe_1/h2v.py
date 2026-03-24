import psycopg
import string
from setup import get_conn_str
from dotenv import load_dotenv

load_dotenv()

def h2v(relation: str = "H"):
    v_int = f"{relation}_V_INT"
    v_text = f"{relation}_V_TEXT"
    print(f"[H2V] Converting relation \"{relation}\" to vertical format...")
    try:
        with psycopg.connect(get_conn_str()) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE TABLE IF NOT EXISTS {v_int} (oid INTEGER, key TEXT, val INTEGER);")
                cur.execute(f"CREATE TABLE IF NOT EXISTS {v_text} (oid INTEGER, key TEXT, val TEXT);")

                cur.execute(f"TRUNCATE TABLE {v_int}, {v_text};")

                # read catalogue
                cur.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position;
                """, (relation.lower(),))

                columns = cur.fetchall()

                if not columns:
                    print(f"[H2V] Error: No columns found for table {relation}")
                    return
                
                # remove oid
                columns = [col for col in columns if col[0] != "oid"]

                int_inserts = []
                text_inserts = []

                for col_name, data_type in columns:
                    if data_type == "integer":
                        int_inserts.append(f"""
                            SELECT oid, '{col_name}' AS key, {col_name} AS val
                            FROM {relation}
                            WHERE {col_name} IS NOT NULL
                        """)
                    elif data_type == "text":
                        text_inserts.append(f"""
                            SELECT oid, '{col_name}' AS key, {col_name} AS val
                            FROM {relation}
                            WHERE {col_name} IS NOT NULL
                        """)

                if int_inserts:
                    sql_int = f"INSERT INTO {v_int} " + " UNION ALL ".join(int_inserts) + ";"
                    cur.execute(sql_int)

                if text_inserts:
                    sql_text = f"INSERT INTO {v_text} " + " UNION ALL ".join(text_inserts) + ";"
                    cur.execute(sql_text)

                conn.commit()

                print(f"[H2V] Successfully created {v_int} and {v_text} from \"{relation}\".")

    except Exception as e:
        print(f"[H2V] Error: {e}")


if __name__ == "__main__":
    h2v()