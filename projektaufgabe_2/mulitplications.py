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