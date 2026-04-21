import argparse
import csv
import random
import time
from dataclasses import dataclass

import psycopg
from dotenv import load_dotenv

from h2v import h2v
from proxy import execute as proxy_execute
from queryAPI import queryAPI as QueryAPI
from setup import generate, get_conn_str
from v2h import v2h

load_dotenv()


DATASET_SIZES = (2_000, 4_000, 8_000)
ATTRIBUTE_COUNTS = (5, 10, 15)
SPARSITY_VALUES = tuple(1 - (2 ** -i) for i in range(2, 6))
BENCHMARK_SECONDS = 3
RANDOM_SEED = 42
BASE_RELATION = "H"
OUTPUT_CSV = "projektaufgabe_1/benchmark_results.csv"
VERTICAL_BACKENDS = ("proxy", "api")


@dataclass(frozen=True)
class BenchmarkConfig:
    dataset_sizes: tuple[int, ...] = DATASET_SIZES
    attribute_counts: tuple[int, ...] = ATTRIBUTE_COUNTS
    sparsity_values: tuple[float, ...] = SPARSITY_VALUES
    duration_seconds: int = BENCHMARK_SECONDS
    random_seed: int = RANDOM_SEED
    relation: str = BASE_RELATION
    output_csv: str = OUTPUT_CSV
    vertical_backend: str = "proxy"

    def __post_init__(self):
        if self.vertical_backend not in VERTICAL_BACKENDS:
            supported = ", ".join(VERTICAL_BACKENDS)
            raise ValueError(
                f"Unsupported vertical_backend {self.vertical_backend!r}. "
                f"Use one of: {supported}"
            )


def attribute_names(num_attributes: int) -> list[str]:
    return [f"attr{i}" for i in range(1, num_attributes + 1)]


def sql_literal(value) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def prepare_horizontal_indexes(cur, relation: str, num_attributes: int) -> None:
    relation_id = relation.lower()
    cur.execute(f"DROP INDEX IF EXISTS idx_{relation_id}_oid;")
    cur.execute(f"CREATE INDEX idx_{relation_id}_oid ON {relation} (oid);")

    for attr in attribute_names(num_attributes):
        cur.execute(f"DROP INDEX IF EXISTS idx_{relation_id}_{attr};")
        cur.execute(f"CREATE INDEX idx_{relation_id}_{attr} ON {relation} ({attr});")


def prepare_vertical_indexes(cur, relation: str) -> None:
    relation_id = relation.lower()
    v_int = f"{relation}_V_INT"
    v_text = f"{relation}_V_TEXT"

    cur.execute(f"DROP INDEX IF EXISTS idx_{relation_id}_v_int_oid_key;")
    cur.execute(f"DROP INDEX IF EXISTS idx_{relation_id}_v_int_key_val_oid;")
    cur.execute(f"DROP INDEX IF EXISTS idx_{relation_id}_v_text_oid_key;")
    cur.execute(f"DROP INDEX IF EXISTS idx_{relation_id}_v_text_key_val_oid;")

    cur.execute(f"CREATE INDEX idx_{relation_id}_v_int_oid_key ON {v_int} (oid, key);")
    cur.execute(f"CREATE INDEX idx_{relation_id}_v_int_key_val_oid ON {v_int} (key, val, oid);")
    cur.execute(f"CREATE INDEX idx_{relation_id}_v_text_oid_key ON {v_text} (oid, key);")
    cur.execute(f"CREATE INDEX idx_{relation_id}_v_text_key_val_oid ON {v_text} (key, val, oid);")


def fetch_existing_oids(cur, relation: str) -> list[int]:
    cur.execute(f"SELECT oid FROM {relation} ORDER BY oid;")
    return [row[0] for row in cur.fetchall()]


def fetch_attribute_domains(cur, relation: str, num_attributes: int) -> dict[str, list]:
    domains = {}

    for attr in attribute_names(num_attributes):
        cur.execute(f"SELECT DISTINCT {attr} FROM {relation} WHERE {attr} IS NOT NULL ORDER BY 1;")
        domains[attr] = [row[0] for row in cur.fetchall()]

    return domains


def relation_size(cur, relation_name: str) -> int:
    cur.execute("SELECT pg_total_relation_size(to_regclass(%s));", (relation_name,))
    size = cur.fetchone()[0]
    return size or 0


def measure_storage(cur, relation: str) -> tuple[int, int]:
    horizontal_size = relation_size(cur, relation)
    vertical_size = relation_size(cur, f"{relation}_V_INT") + relation_size(cur, f"{relation}_V_TEXT")
    return horizontal_size, vertical_size


def benchmark_point_lookup_horizontal(
    cur,
    relation: str,
    oids: list[int],
    duration_seconds: int,
    rng: random.Random,
) -> float:
    deadline = time.perf_counter() + duration_seconds
    query_count = 0

    while time.perf_counter() < deadline:
        cur.execute(f"SELECT * FROM {relation} WHERE oid = %s;", (rng.choice(oids),))
        cur.fetchall()
        query_count += 1

    return query_count / duration_seconds


def benchmark_value_lookup_horizontal(
    cur,
    relation: str,
    domains: dict[str, list],
    duration_seconds: int,
    rng: random.Random,
) -> float:
    active_attributes = [attr for attr, values in domains.items() if values]
    if not active_attributes:
        return 0.0

    deadline = time.perf_counter() + duration_seconds
    query_count = 0

    while time.perf_counter() < deadline:
        attr = rng.choice(active_attributes)
        cur.execute(
            f"SELECT oid FROM {relation} WHERE {attr} = %s;",
            (rng.choice(domains[attr]),),
        )
        cur.fetchall()
        query_count += 1

    return query_count / duration_seconds


def execute_proxy_select(sql_statement: str):
    result = proxy_execute(sql_statement)
    if isinstance(result, str) and result.startswith("[PROXY] Error"):
        raise RuntimeError(result)
    if result == -1:
        raise RuntimeError(f"[PROXY] Invalid SQL statement: {sql_statement}")
    return result


def benchmark_point_lookup_vertical(
    relation: str,
    oids: list[int],
    duration_seconds: int,
    rng: random.Random,
) -> float:
    deadline = time.perf_counter() + duration_seconds
    query_count = 0

    while time.perf_counter() < deadline:
        oid = rng.choice(oids)
        execute_proxy_select(f"SELECT * FROM {relation} WHERE oid = {oid};")
        query_count += 1

    return query_count / duration_seconds


def benchmark_value_lookup_vertical(
    relation: str,
    domains: dict[str, list],
    duration_seconds: int,
    rng: random.Random,
) -> float:
    active_attributes = [attr for attr, values in domains.items() if values]
    if not active_attributes:
        return 0.0

    deadline = time.perf_counter() + duration_seconds
    query_count = 0

    while time.perf_counter() < deadline:
        attr = rng.choice(active_attributes)
        value = rng.choice(domains[attr])
        sql = f"SELECT oid FROM {relation} WHERE {attr} = {sql_literal(value)};"
        execute_proxy_select(sql)
        query_count += 1

    return query_count / duration_seconds


def benchmark_point_lookup_vertical_api(
    api: QueryAPI,
    oids: list[int],
    duration_seconds: int,
    rng: random.Random,
) -> float:
    deadline = time.perf_counter() + duration_seconds
    query_count = 0

    while time.perf_counter() < deadline:
        api.q_i(rng.choice(oids))
        query_count += 1

    return query_count / duration_seconds


def benchmark_value_lookup_vertical_api(
    api: QueryAPI,
    domains: dict[str, list],
    duration_seconds: int,
    rng: random.Random,
) -> float:
    active_attributes = [attr for attr, values in domains.items() if values]
    if not active_attributes:
        return 0.0

    deadline = time.perf_counter() + duration_seconds
    query_count = 0

    while time.perf_counter() < deadline:
        attr = rng.choice(active_attributes)
        api.q_ii(attr, rng.choice(domains[attr]))
        query_count += 1

    return query_count / duration_seconds


def benchmark_point_lookup_vertical_backend(
    backend: str,
    relation: str,
    oids: list[int],
    duration_seconds: int,
    rng: random.Random,
    api: QueryAPI | None = None,
) -> float:
    if backend == "api":
        if api is None:
            raise ValueError("API backend requires a queryAPI instance.")
        return benchmark_point_lookup_vertical_api(api, oids, duration_seconds, rng)

    return benchmark_point_lookup_vertical(relation, oids, duration_seconds, rng)


def benchmark_value_lookup_vertical_backend(
    backend: str,
    relation: str,
    domains: dict[str, list],
    duration_seconds: int,
    rng: random.Random,
    api: QueryAPI | None = None,
) -> float:
    if backend == "api":
        if api is None:
            raise ValueError("API backend requires a queryAPI instance.")
        return benchmark_value_lookup_vertical_api(api, domains, duration_seconds, rng)

    return benchmark_value_lookup_vertical(relation, domains, duration_seconds, rng)


def write_results_csv(results: list[dict], output_path: str) -> None:
    if not results:
        return

    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)


def prepare_vertical_representation(relation: str) -> None:
    h2v(relation)
    v2h(relation)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run horizontal vs. vertical benchmarks.")
    parser.add_argument(
        "--vertical-backend",
        choices=VERTICAL_BACKENDS,
        default="proxy",
        help="Vertical query execution path to benchmark. Defaults to proxy.",
    )
    parser.add_argument(
        "--output-csv",
        default=OUTPUT_CSV,
        help="CSV file to write benchmark results to.",
    )
    return parser.parse_args()


def run_benchmark(config: BenchmarkConfig = BenchmarkConfig()) -> list[dict]:
    rng = random.Random(config.random_seed)
    results = []

    for num_tuples in config.dataset_sizes:
        for num_attributes in config.attribute_counts:
            for sparsity in config.sparsity_values:
                print(
                    f"[BENCHMARK] Running H={num_tuples}, A={num_attributes}, "
                    f"S={sparsity:.4f}, vertical_backend={config.vertical_backend}"
                )
                generate(num_tuples, sparsity, num_attributes)
                prepare_vertical_representation(config.relation)

                with psycopg.connect(get_conn_str()) as conn:
                    with conn.cursor() as cur:
                        prepare_horizontal_indexes(cur, config.relation, num_attributes)
                        prepare_vertical_indexes(cur, config.relation)
                        conn.commit()

                        oids = fetch_existing_oids(cur, config.relation)
                        domains = fetch_attribute_domains(cur, config.relation, num_attributes)
                        horizontal_size, vertical_size = measure_storage(cur, config.relation)

                        h_qps_oid = benchmark_point_lookup_horizontal(
                            cur, config.relation, oids, config.duration_seconds, rng
                        )
                        h_qps_attr = benchmark_value_lookup_horizontal(
                            cur, config.relation, domains, config.duration_seconds, rng
                        )

                        api = None
                        try:
                            if config.vertical_backend == "api":
                                api = QueryAPI()
                                api.setup_functions(num_attributes, config.relation)

                            v_qps_oid = benchmark_point_lookup_vertical_backend(
                                config.vertical_backend,
                                config.relation,
                                oids,
                                config.duration_seconds,
                                rng,
                                api=api,
                            )
                            v_qps_attr = benchmark_value_lookup_vertical_backend(
                                config.vertical_backend,
                                config.relation,
                                domains,
                                config.duration_seconds,
                                rng,
                                api=api,
                            )
                        finally:
                            if api is not None:
                                api.close()

                        results.append(
                            {
                                "vertical_backend": config.vertical_backend,
                                "num_tuples": num_tuples,
                                "num_attributes": num_attributes,
                                "sparsity": round(sparsity, 4),
                                "duration_seconds": config.duration_seconds,
                                "storage_h_bytes": horizontal_size,
                                "storage_v_bytes": vertical_size,
                                "qps_h_oid_lookup": round(h_qps_oid, 2),
                                "qps_h_value_lookup": round(h_qps_attr, 2),
                                "qps_v_oid_lookup": round(v_qps_oid, 2),
                                "qps_v_value_lookup": round(v_qps_attr, 2),
                            }
                        )

    write_results_csv(results, config.output_csv)
    print(f"[BENCHMARK] Wrote {len(results)} benchmark rows to {config.output_csv}")
    return results


if __name__ == "__main__":
    args = parse_args()
    run_benchmark(
        BenchmarkConfig(
            output_csv=args.output_csv,
            vertical_backend=args.vertical_backend,
        )
    )
