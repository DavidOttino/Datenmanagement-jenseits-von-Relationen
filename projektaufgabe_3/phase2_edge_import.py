import psycopg
from pathlib import Path
from connection import get_connection
from db_storage import create_phase1_tables, reset_phase1_tables, save_edge_model
from edge_model import EdgeModelBuilder

def print_edge_statistics(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM node;")
        node_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM edge;")
        edge_count = cur.fetchone()[0]

    print(f"Anzahl der Tupel in Relation 'node': {node_count}")
    print(f"Anzahl der Tupel in Relation 'edge': {edge_count}")

def main() -> None:
    base_dir = Path(__file__).parent
    small_xml = base_dir / "my_small_bib.xml"
    
    if not small_xml.exists():
        print(f"Fehler: {small_xml.name} existiert nicht.")
        return

    print(f"[PARSER] Lese {small_xml.name} ein und transformiere in das hierarchische Format...")
    root = EdgeModelBuilder().from_file(small_xml)

    print("\n[DB] Setze Tabellen zurück...")
    with get_connection() as conn:
        reset_phase1_tables(conn)
        create_phase1_tables(conn)
        
        print("[DB] Speichere Daten in das EDGE-Modell...")
        save_edge_model(conn, root)
        print_edge_statistics(conn)

if __name__ == "__main__":
    main()