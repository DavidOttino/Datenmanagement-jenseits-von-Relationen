def ansatz0(A, B):
    m = len(A)
    l = len(A[0])
    n = len(B[0])

    C = [[0.0 for _ in range(n)] for _ in range(m)]

    for i in range(m):
        for j in range(n):
            for k in range(l):
                C[i][j] += A[i][k] * B[k][j]

    return C

def ansatz1(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT A.i, B.j, SUM(A.val * B.val) AS val
            FROM A, B
            WHERE A.j = B.i
            GROUP BY A.i, B.j
            ORDER BY A.I ASC
        """)

        result = cur.fetchall()

    return result

def ansatz2_slow(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT A_vec.i, B_vec.j, dot_product_sql(A_vec.row_vec, B_vec.col_vec)
            FROM A_vec, B_vec
            ORDER BY A_vec.i, B_vec.j;
        """)
        return cur.fetchall()

def ansatz2_fast(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT A_vec.i, B_vec.j, dot_product_c(A_vec.row_vec, B_vec.col_vec)
            FROM A_vec, B_vec
            ORDER BY A_vec.i, B_vec.j;
        """)
        return cur.fetchall()

def ansatz1_toy(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT a.row, b.col, SUM(a.value * b.value)
            FROM toy_a a
            JOIN toy_b b ON a.col = b.row
            GROUP BY a.row, b.col
            ORDER BY a.row, b.col;
        """)

        result = cur.fetchall()

    return result

def ansatz2_slow_toy(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT a.row, b.col, dot_product_sql(a.vec, b.vec)
            FROM toy_a_vec a, toy_b_vec b
            ORDER BY a.row, b.col;
        """)
        return cur.fetchall()

def ansatz2_fast_toy(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT a.row, b.col, dot_product_c(a.vec, b.vec)
            FROM toy_a_vec a, toy_b_vec b
            ORDER BY a.row, b.col;
        """)
        return cur.fetchall()