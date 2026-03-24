import psycopg
import string
from toy_setup import get_conn_str
from dotenv import load_dotenv

load_dotenv()

def h2v(relation: str):
    print(f"[H2V] Converting relation \"{relation}\" to vertical format...")
    try:
        with psycopg.connect(get_conn_str()) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS V_INT;")
                cur.execute("DROP TABLE IF EXISTS V_TEXT;")

                # create new tables
                cur.execute("""
                    CREATE TABLE V_INT (
                        oid INTEGER,
                        key TEXT,
                        val INTEGER
                    );
                """)
                cur.execute("""
                    CREATE TABLE V_TEXT (
                        oid INTEGER,
                        key TEXT,
                        val TEXT
                    );
                """)

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
                    sql_int = "INSERT INTO V_INT " + " UNION ALL ".join(int_inserts) + ";"
                    cur.execute(sql_int)

                if text_inserts:
                    sql_text = "INSERT INTO V_TEXT " + " UNION ALL ".join(text_inserts) + ";"
                    cur.execute(sql_text)

                conn.commit()

                print(f"[H2V] Successfully created V_INT and V_TEXT from \"{relation}\".")

    except Exception as e:
        print(f"[H2V] Error: {e}")


if __name__ == "__main__":
    h2v("H")