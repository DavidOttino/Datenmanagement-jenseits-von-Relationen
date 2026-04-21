import psycopg
from setup import get_conn_str

class queryAPI:
    def __init__(self):
        self.conn = psycopg.connect(get_conn_str())

    def close(self):
        self.conn.close()

    def setup_functions(self, num_attributes, relation):
        attr_list = [f"attr{i}" for i in range(1, num_attributes + 1)]
        v_int = f"{relation}_V_INT"
        v_text = f"{relation}_V_TEXT"

        returns_columns = ", ".join([f"{a} TEXT" for a in attr_list])

        select_parts = []
        for a in attr_list:
            part = f"""
            COALESCE(
                (SELECT val::TEXT FROM {v_int} WHERE oid = p_oid AND key = '{a}'),
                (SELECT val FROM {v_text} WHERE oid = p_oid AND key = '{a}')
            )"""
            select_parts.append(part)

        q_i_sql = f"""
        CREATE OR REPLACE FUNCTION q_i(p_oid INT)
        RETURNS TABLE (oid INT, {returns_columns}) AS $$
        BEGIN
            RETURN QUERY
            SELECT 
                p_oid,
                {", ".join(select_parts)};
        END;
        $$ LANGUAGE plpgsql;
        """

        q_ii_sql = f"""
        CREATE OR REPLACE FUNCTION q_ii(p_key TEXT, p_val INT)
        RETURNS TABLE (oid INT) AS $$
        BEGIN
            RETURN QUERY
            SELECT v.oid FROM {v_int} v
            WHERE v.key = p_key AND v.val = p_val;
        END;
        $$ LANGUAGE plpgsql;
        """

        with self.conn.cursor() as cur:
            cur.execute(q_i_sql)
            cur.execute(q_ii_sql)
        self.conn.commit()
        print(f"[API] Functions created for {num_attributes} attributes.")

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