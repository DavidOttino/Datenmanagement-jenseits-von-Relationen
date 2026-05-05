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
