from __future__ import annotations

try:
    from .edge_model import EdgeNode
except ImportError:
    from edge_model import EdgeNode


def reset_phase1_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS edge;")
        cur.execute("DROP TABLE IF EXISTS node;")
    conn.commit()


def create_phase1_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS node (
                id INT PRIMARY KEY,
                s_id TEXT,
                type TEXT NOT NULL,
                content TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS edge (
                from_id INT NOT NULL REFERENCES node(id),
                to_id INT NOT NULL REFERENCES node(id),
                PRIMARY KEY (from_id, to_id)
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_node_s_id ON node(s_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_node_type ON node(type);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_node_content ON node(content);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_edge_from ON edge(from_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_edge_to ON edge(to_id);")
    conn.commit()


def save_edge_model(conn, root: EdgeNode) -> None:
    nodes = root.walk()
    edges = root.edges()

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO node (id, s_id, type, content)
            VALUES (%s, %s, %s, %s);
            """,
            [(node.id, node.s_id, node.type, node.content) for node in nodes],
        )
        cur.executemany(
            """
            INSERT INTO edge (from_id, to_id)
            VALUES (%s, %s);
            """,
            edges,
        )
    conn.commit()
