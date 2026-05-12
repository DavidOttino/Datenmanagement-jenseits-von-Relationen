import db_comm as db
import os
import psycopg
import multiplications as mult
from dotenv import load_dotenv

load_dotenv()

def get_conn_str():
    return (
        f"dbname={os.getenv('DB_NAME','e_commerce')} "
        f"user={os.getenv('DB_USER','')} "
        f"password={os.getenv('DB_PASS','')} "
        f"host={os.getenv('DB_HOST','localhost')} "
        f"port={os.getenv('DB_PORT','5432')}"
    )

def create_toy():
    # Matrix A: 3x4
    A = [
        [1, 0, 2, 0],
        [0, 3, 0, 4],
        [5, 0, 0, 6]
    ]
    # Matrix B: 4x3
    B = [
        [7, 0, 8],
        [0, 9, 0],
        [1, 0, 2],
        [0, 3, 0]
    ]
    return A, B

def main():
    conn = psycopg.connect(get_conn_str())

    with conn.cursor() as cur:
        # Create Sparse Toy Tables
        cur.execute("DROP TABLE IF EXISTS toy_a; DROP TABLE IF EXISTS toy_b;")
        cur.execute("CREATE TABLE toy_a (row INT, col INT, value FLOAT);")
        cur.execute("CREATE TABLE toy_b (row INT, col INT, value FLOAT);")
        
        # Create Vector Toy Tables
        cur.execute("DROP TABLE IF EXISTS toy_a_vec; DROP TABLE IF EXISTS toy_b_vec;")
        cur.execute("CREATE TABLE toy_a_vec (row INT, vec FLOAT[]);")
        cur.execute("CREATE TABLE toy_b_vec (col INT, vec FLOAT[]);")
    conn.commit()

    A, B = create_toy()

    with conn.cursor() as cur:
        for r, row_vals in enumerate(A):
            for c, val in enumerate(row_vals):
                if val != 0:
                    cur.execute("INSERT INTO toy_a VALUES (%s, %s, %s)", (r, c, val))
        
        for r, row_vals in enumerate(B):
            for c, val in enumerate(row_vals):
                if val != 0:
                    cur.execute("INSERT INTO toy_b VALUES (%s, %s, %s)", (r, c, val))
    
    with conn.cursor() as cur:
        for r, row_vals in enumerate(A):
            cur.execute("INSERT INTO toy_a_vec VALUES (%s, %s)", (r, row_vals))
        for c in range(len(B[0])): # Transpose for B_vec: group by column
            col_vals = [B[r][c] for r in range(len(B))]
            cur.execute("INSERT INTO toy_b_vec VALUES (%s, %s)", (c, col_vals))
    conn.commit()

    print("--- Verification Toy Example (toy_a / toy_b) ---")

    print("\nAnsatz 0:")
    C_ref = mult.ansatz0(A, B)
    for row in C_ref:
        print(row)
    
    print("\nAnsatz 1:")
    C_ref = mult.ansatz1_toy(conn)
    for row in C_ref:
        print(row)

    print("\nAnsatz 2 - slow:")
    C_ref = mult.ansatz2_slow_toy(conn)
    for row in C_ref:
        print(row)

    print("\nAnsatz 2 - fast:")
    C_ref = mult.ansatz2_fast_toy(conn)
    for row in C_ref:
        print(row)

    conn.close()

if __name__ == "__main__":
    main()