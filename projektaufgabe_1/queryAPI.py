import psycopg
from setup import get_conn_str

class queryAPI:
    def __init__(self):
        self.conn = psycopg.connect(get_conn_str())

    def close(self):
        self.conn.close()

    def setup_functions(self):
        with self.conn.cursor() as cur:
            # Query i: SELECT * WHERE oid = ?
            cur.execute("""
            CREATE OR REPLACE FUNCTION q_i(p_oid INT)
            RETURNS TABLE (
                key TEXT,
                val TEXT
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT key, val::TEXT
                FROM H_V_TEXT
                WHERE oid = p_oid

                UNION ALL

                SELECT key, val::TEXT
                FROM H_V_INT
                WHERE oid = p_oid;
            END;
            $$ LANGUAGE plpgsql;
            """)

            # Query ii: SELECT oid WHERE ai = ?
            cur.execute("""
            CREATE OR REPLACE FUNCTION q_ii(p_key TEXT, p_val INT)
            RETURNS TABLE (
                oid INT
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT oid
                FROM H_V_INT
                WHERE key = p_key AND val = p_val;
            END;
            $$ LANGUAGE plpgsql;
            """)

        self.conn.commit()
        print("[API] Functions created.")


    def q_i(self, oid: int):
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM q_i(%s);", (oid,))
            return cur.fetchall()

    def q_ii(self, key: str, val: int):
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM q_ii(%s, %s);", (key, val))
            return cur.fetchall()
        
    def explain_q_i(self, oid: int):
        with self.conn.cursor() as cur:
            cur.execute("EXPLAIN ANALYZE SELECT * FROM q_i(%s);", (oid,))
            return [row[0] for row in cur.fetchall()]

    def explain_q_ii(self, key: str, val: int):
        with self.conn.cursor() as cur:
            cur.execute("EXPLAIN ANALYZE SELECT * FROM q_ii(%s, %s);", (key, val))
            return [row[0] for row in cur.fetchall()]