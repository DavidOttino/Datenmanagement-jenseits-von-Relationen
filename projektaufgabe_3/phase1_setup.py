from __future__ import annotations

from pathlib import Path

try:
    from .connection import get_connection
    from .db_storage import create_phase1_tables, reset_phase1_tables, save_edge_model
    from .edge_axes import ancestor, descendant, find_one, following_sibling, preceding_sibling
    from .edge_model import EdgeModelBuilder, print_edge_tree
except ImportError:
    from connection import get_connection
    from db_storage import create_phase1_tables, reset_phase1_tables, save_edge_model
    from edge_axes import ancestor, descendant, find_one, following_sibling, preceding_sibling
    from edge_model import EdgeModelBuilder, print_edge_tree


def main() -> None:
    source = Path(__file__).with_name("toy_example.txt")
    root = EdgeModelBuilder().from_file(source)

    print("[PARSER] Transformed toy example:")
    print_edge_tree(root)

    with get_connection() as conn:
        reset_phase1_tables(conn)
        create_phase1_tables(conn)
        save_edge_model(conn, root)

        print()
        print("[DB] Saved EDGE model:")
        print_table_counts(conn)

        print()
        print("[AXES] Toy correctness checks:")
        print_required_axis_outputs(conn)


def print_table_counts(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM node;")
        node_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM edge;")
        edge_count = cur.fetchone()[0]
    print(f"node rows: {node_count}")
    print(f"edge rows: {edge_count}")


def print_required_axis_outputs(conn) -> None:
    author = find_one(conn, type="author", content="Daniel Ulrich Schmitt")
    vldb_2023 = find_one(conn, s_id="vldb_2023")
    schmitt = find_one(conn, s_id="SchmittKAMM23")
    schaler = find_one(conn, s_id="SchalerHS23")

    print_nodes('ancestor("Daniel Ulrich Schmitt")', ancestor(conn, author[0]))
    print_nodes("descendant(vldb_2023)", descendant(conn, vldb_2023[0]))
    print_nodes("following-sibling(SchmittKAMM23)", following_sibling(conn, schmitt[0]))
    print_nodes("preceding-sibling(SchmittKAMM23)", preceding_sibling(conn, schmitt[0]))
    print_nodes("following-sibling(SchalerHS23)", following_sibling(conn, schaler[0]))
    print_nodes("preceding-sibling(SchalerHS23)", preceding_sibling(conn, schaler[0]))


def print_nodes(label: str, rows: list[tuple[int, str | None, str, str | None]]) -> None:
    print(label)
    if not rows:
        print("  <empty>")
        return
    for id_, s_id, type_, content in rows:
        print(f"  id={id_}, s_id={s_id}, type={type_}, content={content}")


if __name__ == "__main__":
    main()
