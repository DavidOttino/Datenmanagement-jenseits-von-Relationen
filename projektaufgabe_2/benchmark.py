import time
import psycopg
import os
import generator as gen
import db_comm as db
import multiplications as mult
import matplotlib.pyplot as plt
from dotenv import load_dotenv

load_dotenv()

def get_conn_str():
    return (f"dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} "
            f"password={os.getenv('DB_PASS')} host={os.getenv('DB_HOST')} "
            f"port={os.getenv('DB_PORT', '5432')}")

def run_benchmark():
    # Konfiguration: Zweierpotenzen für L und verschiedene Sparsity-Werte [cite: 69, 72]
    sizes = [2**3, 2**4, 2**5, 2**6] 
    sparsities = [0.2, 0.5, 0.8]
    
    results = {0: [], 1: [], 2: [], 3: []}

    with psycopg.connect(get_conn_str()) as conn:
        for l in sizes:
            for s in sparsities:
                print(f"Benchmarking: L={l}, Sparsity={s}")
                
                # Cleanup und Setup beider Phasen
                with conn.cursor() as cur:
                    cur.execute("DROP TABLE IF EXISTS A, B, A_vec, B_vec;")
                db.create_tables(conn)
                db.create_vector_tables(conn)
                db.create_functions(conn)
                
                # Daten generieren
                A_raw, B_raw = gen.generate(l, s)
                
                # Import Ansatz 1
                t_A, t_B = db.create_sparse_tables(A_raw, B_raw)
                db.insert(conn, t_A, t_B)
                
                # Import Ansatz 2
                db.insert_vector_data(conn, A_raw, B_raw)

                # Messung Ansatz 0 (Python)
                start = time.time()
                mult.ansatz0(A_raw, B_raw)
                results[0].append((l, s, time.time() - start))

                # Messung Ansatz 1 (SQL Sparse)
                start = time.time()
                mult.ansatz1(conn)
                results[1].append((l, s, time.time() - start))

                # Zeitmessung Ansatz 2 (Langsam - SQL)
                start = time.time()
                mult.ansatz2_slow(conn)
                results[2].append((l, s, time.time() - start))

                # Zeitmessung Ansatz 2 (Schnell - C)
                start = time.time()
                mult.ansatz2_fast(conn)
                results[3].append((l, s, time.time() - start))

    plot_results(results, sparsities)

def plot_results(results, sparsities):
    for s in sparsities:
        plt.figure(figsize=(10, 6))
        labels = ["Ansatz 0 (Client)", "Ansatz 1 (Sparse SQL)", "Ansatz 2 (Vector SQL)", "Ansatz 2 (Vector C)"]
        for i in range(4):
            data = [r for r in results[i] if r[1] == s]
            plt.plot([r[0] for r in data], [r[2] for r in data], label=labels[i], marker='o')
        
        plt.title(f"Matrixmultiplikation Performance (Sparsity {s})")
        plt.xlabel("Matrixgröße L")
        plt.ylabel("Zeit in Sekunden")
        plt.legend()
        plt.grid(True)
        plt.show()

if __name__ == "__main__":
    run_benchmark()