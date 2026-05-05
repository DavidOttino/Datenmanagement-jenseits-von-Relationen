import db_comm as db
import os
import psycopg
import mulitplications as mult
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
    A = [
        [1, 0, 2, 0],
        [0, 3, 0, 4],
        [5, 0, 0, 6]
    ]

    B = [
        [7, 0, 8],
        [0, 9, 0],
        [1, 0, 2],
        [0, 3, 0]
    ]

    return A, B

def main():
    conn = psycopg.connect(get_conn_str())

    db.reset_db(conn)
    db.create_tables(conn)

    A, B = create_toy()
    table_A, table_B = db.create_sparse_tables(A, B)

    print(table_A)
    print(table_B)

    db.insert(conn, table_A, table_B)

    #Ansatz 0
    C = mult.ansatz0(A, B)
    print(C)

    #Ansatz 1
    result = mult.ansatz1(conn)

    for row in result:
        print(row)

    conn.close()

if __name__ == "__main__":
    main()