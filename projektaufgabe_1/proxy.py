import re
import psycopg
from setup import get_conn_str

def extract_table_name(sql_statement: str) -> str:
    match = re.search(r'(?i)\b(FROM|INTO|UPDATE)\b\s+([a-zA-Z0-9_]+)', sql_statement)
    if match:
        return match.group(2)
    return None

def execute(sql_statement: str) -> int:
    target = extract_table_name(sql_statement)
    if not target:
        print(f"[PROXY] Error: Could not identify target relation.")
        return -1   
    print(f"[PROXY] Executing command on relation \"{target}\"")
    v_int = f"{target}_V_INT"
    v_text = f"{target}_V_TEXT"
    v_view = f"{target}_VIEW"

    clean_sql = sql_statement.strip()
    print(f"[PROXY] Parsing command type...")
    cmd = clean_sql.split()[0].upper()

    try:
        with psycopg.connect(get_conn_str()) as conn:
            with conn.cursor() as cur:
                if cmd == "SELECT":
                    print(f"[PROXY] Redirecting SELECT statement...")
                    redirected_sql = re.sub(rf'\b{target}\b', v_view, clean_sql, flags=re.IGNORECASE)
                    cur.execute(redirected_sql)
                    return cur.fetchall()
                
                elif cmd == "INSERT":
                    print(f"[PROXY] Redirecting INSERT statement...")
                    pattern = r'(?i)INSERT\s+INTO\s+\w+\s*\((.*?)\)\s*VALUES\s*\((.*?)\)'
                    match = re.search(pattern, clean_sql)
                    if match:
                        cols = [c.strip() for c in match.group(1).split(',')]
                        vals = [v.strip().strip("'") for v in match.group(2).split(',')]
                        data = dict(zip(cols, vals))
                        oid = data.pop('oid')
                        for key, val in data.items():
                            _upsert_vertical(cur, v_int, v_text, oid, key, val)
                        conn.commit()
                        return f"OID {oid} inserted/updated vertically."

                elif cmd == "UPDATE":
                    print(f"[PROXY] Redirecting UPDATE statement...")
                    pattern = r'(?i)UPDATE\s+\w+\s+SET\s+([a-zA-Z0-9_]+)\s*=\s*(.*?)\s+WHERE\s+oid\s*=\s*(\d+)'
                    match = re.search(pattern, clean_sql)
                    if match:
                        key, val, oid = match.groups()
                        val = val.strip().strip("'")
                        _upsert_vertical(cur, v_int, v_text, oid, key, val)
                        conn.commit()
                        return f"Attribute {key} for OID {oid} updated vertically."
                    
                elif cmd == "DELETE":
                    print(f"[PROXY] Redirecting DELETE statement...")
                    match = re.search(r'(?i)WHERE\s+oid\s*=\s*(\d+)', clean_sql)
                    if match:
                        oid = match.group(1)
                        cur.execute(f"DELETE FROM {v_int} WHERE oid = %s", (oid,))
                        cur.execute(f"DELETE FROM {v_text} WHERE oid = %s", (oid,))
                        conn.commit()
                        return f"OID {oid} removed from vertical storage."

    except Exception as e:
        return f"[PROXY] Error: {e}"

def _upsert_vertical(cur, v_int, v_text, oid, key, val):
    cur.execute(f"DELETE FROM {v_int} WHERE oid = %s AND key = %s", (oid, key))
    cur.execute(f"DELETE FROM {v_text} WHERE oid = %s AND key = %s", (oid, key))
    
    try:
        int_val = int(val)
        cur.execute(f"INSERT INTO {v_int} (oid, key, val) VALUES (%s, %s, %s)", (oid, key, int_val))
    except ValueError:
        cur.execute(f"INSERT INTO {v_text} (oid, key, val) VALUES (%s, %s, %s)", (oid, key, val))

if __name__ == "__main__":
    #print(execute("SELECT * FROM H WHERE oid = 0"))
    #print(execute("INSERT INTO H (oid, attr1, attr2) VALUES (101, 100, 'Initial')"))
    #print(execute("UPDATE H SET attr1 = '999' WHERE oid = 101;"))
    print(execute("DELETE FROM H WHERE oid = 101;"))
