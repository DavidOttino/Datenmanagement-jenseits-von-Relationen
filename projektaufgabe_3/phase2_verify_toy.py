from pathlib import Path
from connection import get_connection
from db_storage import create_phase1_tables, reset_phase1_tables, save_edge_model
from edge_model import EdgeModelBuilder
from edge_axes import find_one, ancestor, descendant, following_sibling, preceding_sibling
from phase2_accelerator_axes import ancestor_accel, descendant_accel, following_sibling_accel, preceding_sibling_accel
from phase2_accelerator import create_xpath_accelerator_tables, annotate_tree, save_accelerator_data

def print_axis_values(title, p1_rows, p2_rows):
    p1_ids = ", ".join(sorted([str(r[0]) for r in p1_rows])) if p1_rows else "leere Menge"
    p2_ids = ", ".join(sorted([str(r[0]) for r in p2_rows])) if p2_rows else "leere Menge"
    
    print(f"--- {title} ---")
    print(f"  Phase 1 - IDs:   {p1_ids}")
    print(f"  Phase 1 - Größe: {len(p1_rows)}")
    print(f"  Phase 2 - IDs:   {p2_ids}")
    print(f"  Phase 2 - Größe: {len(p2_rows)}")
    print()

def main():
    source = Path(__file__).with_name("toy_example.txt")
    if not source.exists():
        print(f"Fehler: {source.name} not found.")
        return
    
    root = EdgeModelBuilder().from_file(source)
    accel_rows, content_rows, attribute_rows = annotate_tree(root)
    
    with get_connection() as conn:
        reset_phase1_tables(conn)
        create_phase1_tables(conn)
        save_edge_model(conn, root)
        
        create_xpath_accelerator_tables(conn)
        save_accelerator_data(conn, accel_rows, content_rows, attribute_rows)
        
        print("[DB] Daten erfolgreich geladen. Berechne Werte...\n")
        
        # ancestor: ID des Autors 'Daniel Ulrich Schmitt'
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM node WHERE content = 'Daniel Ulrich Schmitt';")
            schmitt_author_row = cur.fetchone()
            schmitt_author_id = schmitt_author_row[0] if schmitt_author_row else None

        # descendants: ID des year-Knotens unter VLDB für 2023
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM node WHERE type = 'year' AND (s_id = 'vldb_2023' OR content = '2023');")
            vldb_2023_row = cur.fetchone()
            vldb_2023_id = vldb_2023_row[0] if vldb_2023_row else 2

        # preceding/following: IDs der Publikationen
        schmitt_pub = find_one(conn, s_id="SchmittKAMM23")
        schaler_pub = find_one(conn, s_id="SchalerHS23")
        
        if not schmitt_author_id or not schmitt_pub or not schaler_pub:
            print("❌ Fehler: Testknoten konnten in der DB nicht identifiziert werden.")
            return

        schmitt_pub_id = schmitt_pub[0]
        schaler_pub_id = schaler_pub[0]

        print_axis_values("ancestor (Kontext: Autor 'Daniel Ulrich Schmitt')", 
                          ancestor(conn, schmitt_author_id), 
                          ancestor_accel(conn, schmitt_author_id))
        
        print_axis_values("descendants (Kontext: Jahr '2023' unter vldb)", 
                          descendant(conn, vldb_2023_id), 
                          descendant_accel(conn, vldb_2023_id))
        
        print_axis_values("following SchmittKAMM23", 
                          following_sibling(conn, schmitt_pub_id), 
                          following_sibling_accel(conn, schmitt_pub_id))
        
        print_axis_values("preceding SchmittKAMM23", 
                          preceding_sibling(conn, schmitt_pub_id), 
                          preceding_sibling_accel(conn, schmitt_pub_id))
        
        print_axis_values("following SchalerHS23", 
                          following_sibling(conn, schaler_pub_id), 
                          following_sibling_accel(conn, schaler_pub_id))
        
        print_axis_values("preceding SchalerHS23", 
                          preceding_sibling(conn, schaler_pub_id), 
                          preceding_sibling_accel(conn, schaler_pub_id))

if __name__ == "__main__":
    main()