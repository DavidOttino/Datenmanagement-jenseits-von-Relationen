import os
import psycopg
import random
import string
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

def toy_setup():
    print("[SETUP] Starting toy_setup...")
    try:
        with psycopg.connect(get_conn_str()) as conn:
            with conn.cursor() as cur:
                # --- Teil a ---
                cur.execute("DROP TABLE IF EXISTS H_toy CASCADE;")
                cur.execute("CREATE TABLE H_toy (oid INT, a1 TEXT, a2 TEXT, a3 INT);")
                print("[SETUP] Created table H_toy")

                data_h = [
                    (1, 'a', 'b', None),
                    (2, None, 'c', 2),
                    (3, None, None, 3),
                    (4, None, None, None)
                ]

                cur.executemany("INSERT INTO H_toy VALUES (%s, %s, %s, %s)", data_h)
                print("[SETUP] Populated table H_toy")

                # --- Teil b ---
                cur.execute("DROP TABLE IF EXISTS V_toy CASCADE;")
                cur.execute("CREATE TABLE V_toy (oid INT, key TEXT, val TEXT);")
                print("[SETUP] Created table V_toy")

                data_v = [
                    (1, 'a1', 'a'), (1, 'a2', 'b'),
                    (2, 'a2', 'c'), (2, 'a3', '2'),
                    (3, 'a3', '3'),
                    (4, None, None)
                ]

                cur.executemany("INSERT INTO V_toy VALUES (%s, %s, %s)", data_v)
                print("[SETUP] Populated table V_toy")

                # --- Teil c ---
                cur.execute("""
                CREATE OR REPLACE VIEW h2v_toy AS
                select
                    o.oid,
                    a1.val AS a1,
                    a2.val AS a2,
                    a3.val::INT AS a3
                from
                    (select DISTINCT oid from V_toy) o
                left join
                    (select oid, val from V_toy where key='a1') a1
                    ON o.oid = a1.oid
                left join
                    (select oid, val from V_toy where key='a2') a2
                    ON o.oid = a2.oid
                left join
                    (select oid, val from V_toy where key='a3') a3
                    ON o.oid = a3.oid
                order by oid asc;
                """)
                print("[SETUP] Created view h2v_toy")

                # --- Teil d ---
                cur.execute("DROP TABLE IF EXISTS V_toy_str CASCADE;")
                cur.execute("""
                CREATE TABLE V_toy_str (
                    oid INT,
                    key TEXT,
                    val TEXT
                );
                """)

                cur.execute("DROP TABLE IF EXISTS V_toy_int CASCADE;")
                cur.execute("""
                CREATE TABLE V_toy_int (
                    oid INT,
                    key TEXT,
                    val INT
                );
                """)

                cur.execute("""
                INSERT INTO V_toy_str
                select oid, key, val
                from V_toy
                where key IN ('a1','a2') or key is null;
                """)

                cur.execute("""
                INSERT INTO V_toy_int
                select oid, key, val::INT
                from V_toy
                where key='a3';
                """)

                cur.execute("""
                CREATE OR REPLACE VIEW V_toy_all AS
                select oid, key, val::TEXT AS val
                from V_toy_str
                union all
                select oid, key, val::TEXT
                from V_toy_int
                order by oid asc;
                """)

                print("[SETUP] Created partitions and view V_toy_all")

    except Exception as e:
        print(f"[SETUP] Error: {e}")
    print("[SETUP] Completed toy_setup!")

def generate(num_tuples: int, sparsity: float, num_attributes: int):
    print("[GENERATE] Starting data generation...")
    
    # most possible attributes in Postgres is 1.600
    if num_attributes > 1599:
        print("[GENERATE] Error: Unable to generate H with more than 1.600 attributes!")
        return
    
    try:
        with psycopg.connect(get_conn_str()) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS H CASCADE;")
                columns = ["oid INTEGER"]
                for i in range(num_attributes):
                    if i % 2 == 0:
                        columns.append(f"attr{i+1} INTEGER")
                    else:
                        columns.append(f"attr{i+1} TEXT")
                columns_sql = ", ".join(columns)

                cur.execute(f"CREATE TABLE H ({columns_sql});")

                print(f"[GENERATE] Preparing {num_attributes} independent value pools...")
                unique_per_col = (num_tuples // 5) + 1
                all_pools = []
                for i in range(num_attributes):
                    if i % 2 == 0:
                        pool = list(range(1, unique_per_col + 1)) * 5
                    else:
                        pool = [''.join(random.choices(string.ascii_uppercase, k=3)) for _ in range(unique_per_col)] * 5
                    
                    random.shuffle(pool)
                    all_pools.append(iter(pool))
                
                print(f"[GENERATE] Streaming {num_tuples} rows to Postgres...")
                with cur.copy("COPY H FROM STDIN") as copy:
                    for t in range(num_tuples):
                        row = [t]
                        for i in range(num_attributes):
                            if random.random() < sparsity:
                                row.append(None)
                                next(all_pools[i]) 
                                continue
                            row.append(next(all_pools[i]))
                        
                        copy.write_row(row)

                print(f"[GENERATE] Generating views...")
                cur.execute("""
                CREATE OR REPLACE VIEW view_sparsity AS
                select
                    (select count(*) from H where attr1 is NULL)::numeric / count(*) as sparsity
                from H;
                """)

                cur.execute("""
                CREATE OR REPLACE VIEW view_max_attr1 AS 
                select attr1, count(*) AS count_per_value
                from H
                where attr1 is not null
                group by attr1
                having count(*) > 5;
                """)

        print(f"[GENERATE] Completed data generation: {num_tuples} tuples with {num_attributes} attributes.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    toy_setup()
    generate(100000, 0.2, 1599)
