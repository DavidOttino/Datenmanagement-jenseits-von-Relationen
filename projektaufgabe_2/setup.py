import db_comm as db
import generator as gen
import os
import psycopg
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

def main():
    conn = psycopg.connect(get_conn_str())

    db.reset_db(conn)
    db.create_tables(conn)

    A, B = gen.generate(l=10, sparsity=0)
    table_A, table_B = db.create_sparse_tables(A, B)

    db.insert(conn, table_A, table_B)

    conn.close()

if __name__ == "__main__":
    main()