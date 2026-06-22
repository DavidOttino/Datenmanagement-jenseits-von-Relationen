"""
Verifikation: Phase 1 vs Phase 3 Optimiert (Fenster-Verkleinerung)

Gibt die Ergebnisse in einer einfachen Tabellen-Form aus zum Copy-Pasten.
Ruft nur bestehende Funktionen auf, keine Duplikate!
"""

from __future__ import annotations
from pathlib import Path

try:
    from .connection import get_connection
    from .db_storage import reset_phase1_tables, create_phase1_tables, save_edge_model
    from .edge_model import EdgeModelBuilder
    from .edge_axes import ancestor, descendant, following_sibling, preceding_sibling, find_one
    from .phase2_accelerator import create_xpath_accelerator_tables, annotate_tree, save_accelerator_data
    from .phase2_accelerator_axes import ancestor_accel, descendant_accel, following_sibling_accel, preceding_sibling_accel
except ImportError:
    from connection import get_connection
    from db_storage import reset_phase1_tables, create_phase1_tables, save_edge_model
    from edge_model import EdgeModelBuilder
    from edge_axes import ancestor, descendant, following_sibling, preceding_sibling, find_one
    from phase2_accelerator import create_xpath_accelerator_tables, annotate_tree, save_accelerator_data
    from phase2_accelerator_axes import ancestor_accel, descendant_accel, following_sibling_accel, preceding_sibling_accel


def setup_phase1(conn, root):
    """Setup Phase 1: EDGE Model."""
    reset_phase1_tables(conn)
    create_phase1_tables(conn)
    save_edge_model(conn, root)


def setup_phase3_optimized(conn, root):
    """Setup Phase 3 Optimiert: nutzt bestehende Phase 2 Funktionen (mit height)."""
    create_xpath_accelerator_tables(conn)
    accel_rows, content_rows, attribute_rows = annotate_tree(root)
    save_accelerator_data(conn, accel_rows, content_rows, attribute_rows)


def format_ids(results):
    """Formatiert Node IDs für die Tabelle."""
    ids = [str(row[0]) for row in results]
    return ", ".join(ids) if ids else "—"


def print_results_table(title, queries_dict):
    """Gibt Ergebnisse in Tabellen-Form aus."""
    print(f"\n{title}")
    print("=" * 90)
    print(f"{'Achse':<35} | {'Ergebnisknoten IDs':<40} | {'Größe':<5}")
    print("-" * 90)
    
    for axis_name, results in queries_dict.items():
        ids_str = format_ids(results)
        size = len(results)
        print(f"{axis_name:<35} | {ids_str:<40} | {size:<5}")
    
    print("=" * 90)


def main():
    """Hauptfunktion."""
    base_dir = Path(__file__).parent
    toy_example = base_dir / "toy_example.txt"
    
    if not toy_example.exists():
        print(f"✗ Fehler: {toy_example.name} nicht gefunden!")
        return
    
    print("\n" + "=" * 90)
    print("VERIFIKATION: Phase 1 vs Phase 3 Optimiert (Fenster-Verkleinerung)")
    print("=" * 90)
    
    # Parse XML
    print(f"\n[PARSE] Lese {toy_example.name}...")
    root = EdgeModelBuilder().from_file(toy_example)
    
    all_nodes = root.walk()
    print(f"Gesamt-Knoten: {len(all_nodes)}")
    
    with get_connection() as conn:
        # Setup Phase 1
        print("\n[SETUP] Phase 1: EDGE Model...")
        setup_phase1(conn, root)
        
        # Setup Phase 3 Optimiert
        print("[SETUP] Phase 3 Optimiert: accel mit Height...")
        setup_phase3_optimized(conn, root)
        
        # Finde Test-Knoten
        print("\n[FIND] Suche Test-Knoten...")
        
        article_node = find_one(conn, type="article")
        print(f"  Article-Knoten: id={article_node[0]}, s_id={article_node[1]}")
        
        year_node = find_one(conn, type="year")
        print(f"  Year-Knoten: id={year_node[0]}, s_id={year_node[1]}")
        
        schmitt_node = find_one(conn, s_id="SchmittKAMM23")
        print(f"  Schmitt-Knoten: id={schmitt_node[0]}, s_id={schmitt_node[1]}")
        
        schaler_node = find_one(conn, s_id="SchalerHS23")
        print(f"  Schäler-Knoten: id={schaler_node[0]}, s_id={schaler_node[1]}")
        
        # ===== PHASE 1 QUERIES =====
        print("\n[QUERIES] Phase 1 Queries...")
        
        phase1_queries = {
            "ancestor": ancestor(conn, article_node[0]),
            "descendant": descendant(conn, year_node[0]),
            "following-sibling (SchmittKAMM23)": following_sibling(conn, schmitt_node[0]),
            "preceding-sibling (SchmittKAMM23)": preceding_sibling(conn, schmitt_node[0]),
            "following-sibling (SchalerHS23)": following_sibling(conn, schaler_node[0]),
            "preceding-sibling (SchalerHS23)": preceding_sibling(conn, schaler_node[0]),
        }
        
        # ===== PHASE 3 OPTIMIERT QUERIES (nutzt bestehende Funktionen) =====
        print("[QUERIES] Phase 3 Optimiert Queries (aus phase2_accelerator_axes.py)...")
        
        phase3_queries = {
            "ancestor": ancestor_accel(conn, article_node[0]),
            "descendant": descendant_accel(conn, year_node[0]),
            "following-sibling (SchmittKAMM23)": following_sibling_accel(conn, schmitt_node[0]),
            "preceding-sibling (SchmittKAMM23)": preceding_sibling_accel(conn, schmitt_node[0]),
            "following-sibling (SchalerHS23)": following_sibling_accel(conn, schaler_node[0]),
            "preceding-sibling (SchalerHS23)": preceding_sibling_accel(conn, schaler_node[0]),
        }
        
        # ===== VERGLEICH =====
        print("\n[VERIFIKATION] Vergleich Phase 1 vs Phase 3 Optimiert...")
        
        all_identical = True
        for axis_name in phase1_queries.keys():
            phase1_ids = sorted([row[0] for row in phase1_queries[axis_name]])
            phase3_ids = sorted([row[0] for row in phase3_queries[axis_name]])
            
            if phase1_ids == phase3_ids:
                print(f"  ✓ {axis_name}: IDENTISCH")
            else:
                print(f"  ✗ {axis_name}: UNTERSCHIED!")
                all_identical = False
        
        # ===== AUSGABE TABELLEN =====
        print_results_table("TABELLE 1: PHASE 1 (EDGE Model)", phase1_queries)
        print_results_table("TABELLE 2: PHASE 3 OPTIMIERT (Fenster-Verkleinerung)", phase3_queries)


if __name__ == "__main__":
    main()