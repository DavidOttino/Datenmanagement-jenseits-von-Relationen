import psycopg
from pathlib import Path
from connection import get_connection
from edge_model import EdgeModelBuilder, EdgeNode

def create_xpath_accelerator_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS attribute;")
        cur.execute("DROP TABLE IF EXISTS content;")
        cur.execute("DROP TABLE IF EXISTS accel;")
        print("[DB] Erstelle Accelerator-Tabellen...")
        
        cur.execute(
            """
            CREATE TABLE accel (
                pre INT PRIMARY KEY,
                post INT NOT NULL,
                parent INT,
                node_id INT NOT NULL,
                height INT NOT NULL
            );
            """
        )
        
        cur.execute(
            """
            CREATE TABLE content (
                node_id INT PRIMARY KEY,
                tag TEXT NOT NULL,
                text TEXT
            );
            """
        )
        
        cur.execute(
            """
            CREATE TABLE attribute (
                node_id INT NOT NULL,
                name TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (node_id, name)
            );
            """
        )
        
        print("[DB] Erstelle Indizes für schnelle Bereichsanfragen...")
        cur.execute("CREATE INDEX idx_accel_post ON accel(post);")
        cur.execute("CREATE INDEX idx_accel_parent ON accel(parent);")
        cur.execute("CREATE INDEX idx_accel_node_id ON accel(node_id);")
        cur.execute("CREATE INDEX idx_content_tag ON content(tag);")
        cur.execute("CREATE INDEX idx_content_text ON content(text);")
        
    conn.commit()


def annotate_tree(root: EdgeNode) -> tuple[list[tuple], list[tuple], list[tuple]]:
    accel_rows = []
    content_rows = []
    attribute_rows = []
    
    pre_counter = 0
    post_counter = 0

    def dfs(node: EdgeNode, parent_pre: int | None) -> tuple[int, int]:
        nonlocal pre_counter, post_counter
        
        current_pre = pre_counter
        pre_counter += 1
        
        # Phase 3: Höhe des Baumes berechnen
        max_child_height = -1
        for child in node.children:
            child_pre, child_height = dfs(child, current_pre)
            max_child_height = max(max_child_height, child_height)
            
        current_post = post_counter
        post_counter += 1

        # Phase 3: Höhe berechnen: Blatt = 0, Innere = max(Kinderhöhen) + 1
        height = max_child_height + 1
        
        accel_rows.append((current_pre, current_post, parent_pre, node.id, height))
        content_rows.append((node.id, node.type, node.content))
        if node.s_id is not None:
            attribute_rows.append((node.id, "s_id", node.s_id))
            
        return current_pre, height

    print("[ANNOTATION] Starte Pre-/Post-Order Berechnung via DFS...")
    dfs(root, parent_pre=None)
    print(f"[ANNOTATION] Fertig. {pre_counter} Knoten annotiert.")
    
    return accel_rows, content_rows, attribute_rows


def save_accelerator_data(conn, accel_rows, content_rows, attribute_rows) -> None:
    print("[DB] Schreibe Daten in die Accelerator-Tabellen...")
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO accel (pre, post, parent, node_id, height)
            VALUES (%s, %s, %s, %s, %s);
            """,
            accel_rows
        )
        
        cur.executemany(
            """
            INSERT INTO content (node_id, tag, text)
            VALUES (%s, %s, %s);
            """,
            content_rows
        )
        
        if attribute_rows:
            cur.executemany(
                """
                INSERT INTO attribute (node_id, name, value)
                VALUES (%s, %s, %s);
                """,
                attribute_rows
            )
            
    conn.commit()
    print("[DB] Alle Daten erfolgreich gespeichert.")


def print_stats(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM accel;")
        print(f"Einträge in 'accel': {cur.fetchone()[0]}")
        cur.execute("SELECT COUNT(*) FROM content;")
        print(f"Einträge in 'content': {cur.fetchone()[0]}")
        cur.execute("SELECT COUNT(*) FROM attribute;")
        print(f"Einträge in 'attribute': {cur.fetchone()[0]}")


def main() -> None:
    base_dir = Path(__file__).parent
    small_xml = base_dir / "my_small_bib.xml"
    
    if not small_xml.exists():
        print(f"Fehler: {small_xml.name} fehlt.")
        return

    root = EdgeModelBuilder().from_file(small_xml)
    accel_rows, content_rows, attribute_rows = annotate_tree(root)
    
    with get_connection() as conn:
        create_xpath_accelerator_tables(conn)
        save_accelerator_data(conn, accel_rows, content_rows, attribute_rows)
        print_stats(conn)

if __name__ == "__main__":
    main()