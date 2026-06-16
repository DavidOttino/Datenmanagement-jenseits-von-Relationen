import psycopg
from generator import generate

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS A (
                i INT NOT NULL,
                j INT NOT NULL,
                val INT NOT NULL
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS B (
                i INT NOT NULL,
                j INT NOT NULL,
                val INT NOT NULL
            );
        """)
    conn.commit()

def create_vector_tables(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS A_vec (i INT, row_vec DOUBLE PRECISION[]);")
        cur.execute("CREATE TABLE IF NOT EXISTS B_vec (j INT, col_vec DOUBLE PRECISION[]);")
    conn.commit()

def reset_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
                    DROP TABLE IF EXISTS A, B;
                    """)

def create_sparse_tables(A, B):
    table_A = [(i + 1, j + 1, A[i][j])
               for i in range(len(A))
               for j in range(len(A[0]))
               if A[i][j] != 0]

    table_B = [(i + 1, j + 1, B[i][j])
               for i in range(len(B))
               for j in range(len(B[0]))
               if B[i][j] != 0]

    return table_A, table_B

def create_functions(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE OR REPLACE FUNCTION dot_product_sql(vec1 DOUBLE PRECISION[], vec2 DOUBLE PRECISION[]) 
            RETURNS DOUBLE PRECISION AS $$
                SELECT COALESCE(SUM(v1 * v2), 0.0)
                FROM unnest(vec1, vec2) AS t(v1, v2);
            $$ LANGUAGE SQL;
        """)

        cur.execute("""
            CREATE OR REPLACE FUNCTION dot_product_c(DOUBLE PRECISION[], DOUBLE PRECISION[])
            RETURNS DOUBLE PRECISION
            AS '/var/lib/postgresql/extension_libs/dot_product.so', 'dot_product_c'
            LANGUAGE C IMMUTABLE STRICT;
        """)
    conn.commit()

def insert(conn, table_A, table_B):
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO A (i, j, val) VALUES (%s, %s, %s)",
            table_A
        )
        cur.executemany(
            "INSERT INTO B (i, j, val) VALUES (%s, %s, %s)",
            table_B
        )
    conn.commit()

def insert_vector_data(conn, A, B):
    rows_A = [(i + 1, row) for i, row in enumerate(A)]
    
    l = len(B)
    n = len(B[0])
    cols_B = []
    for j in range(n):
        column = [B[i][j] for i in range(l)]
        cols_B.append((j + 1, column))

    with conn.cursor() as cur:
        cur.executemany("INSERT INTO A_vec (i, row_vec) VALUES (%s, %s)", rows_A)
        cur.executemany("INSERT INTO B_vec (j, col_vec) VALUES (%s, %s)", cols_B)
    conn.commit()
