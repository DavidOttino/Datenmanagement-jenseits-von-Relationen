"""
Verifikation: Phase 1 vs Phase 3 Single-Axis (1D Range Query)

Vergleicht die Descendant-Ergebnisse und zeigt Node-Identitäten mit pre_min/pre_max.
Nutzt nur bestehende Funktionen, keine Duplikate!
"""

from __future__ import annotations
from pathlib import Path

try:
    from .connection import get_connection
    from .db_storage import reset_phase1_tables, create_phase1_tables, save_edge_model
    from .edge_model import EdgeModelBuilder
    from .edge_axes import descendant, find_one
    from .phase3_accelerator_single_axis import (
        annotate_tree_single_axis,
        create_single_axis_accelerator_tables,
        save_single_axis_accelerator_data
    )
    from .phase3_single_axis import descendant_single_axis
except ImportError:
    from connection import get_connection
    from db_storage import reset_phase1_tables, create_phase1_tables, save_edge_model
    from edge_model import EdgeModelBuilder
    from edge_axes import descendant, find_one
    from phase3_accelerator_single_axis import (
        annotate_tree_single_axis,
        create_single_axis_accelerator_tables,
        save_single_axis_accelerator_data
    )
    from phase3_single_axis import descendant_single_axis


def setup_phase1(conn, root):
    """Setup Phase 1: EDGE Model."""
    reset_phase1_tables(conn)
    create_phase1_tables(conn)
    save_edge_model(conn, root)


def setup_phase3_single_axis(conn, root):
    """Setup Phase 3 Single-Axis: nutzt bestehende Single-Axis Funktionen."""
    annotations = annotate_tree_single_axis(root)
    
    # Extrahiere content und attribute
    content_rows = []
    attribute_rows = []
    
    def walk(node):
        content_rows.append((node.id, node.type, node.content))
        if node.s_id is not None:
            attribute_rows.append((node.id, "s_id", node.s_id))
        for child in node.children:
            walk(child)
    
    walk(root)
    
    create_single_axis_accelerator_tables(conn)
    save_single_axis_accelerator_data(conn, annotations, content_rows, attribute_rows)


def get_node_info_phase1(conn, node_id: int) -> dict:
    """Holt Node-Informationen aus Phase 1 (pre, post)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.id, a.value AS s_id, c.tag AS type, c.text AS content
            FROM node n
            LEFT JOIN attribute a ON a.node_id = n.id AND a.name = 's_id'
            LEFT JOIN content c ON c.node_id = n.id
            WHERE n.id = %s;
            """,
            (node_id,),
        )
        row = cur.fetchone()
        if row:
            return {
                "id": row[0],
                "s_id": row[1],
                "type": row[2],
                "content": row[3]
            }
        return None


def get_node_info_phase3_sa(conn, node_id: int) -> dict:
    """Holt Node-Informationen aus Phase 3 Single-Axis (pre_min, pre_max)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.pre_min, a.pre_max, n.id, att.value AS s_id, c.tag AS type, c.text AS content
            FROM accel_single a
            JOIN node n ON n.id = a.node_id
            LEFT JOIN attribute att ON att.node_id = n.id AND att.name = 's_id'
            LEFT JOIN content c ON c.node_id = n.id
            WHERE n.id = %s;
            """,
            (node_id,),
        )
        row = cur.fetchone()
        if row:
            return {
                "pre_min": row[0],
                "pre_max": row[1],
                "id": row[2],
                "s_id": row[3],
                "type": row[4],
                "content": row[5]
            }
        return None


def format_node_info_phase1(node_info: dict) -> str:
    """Formatiert Node-Information Phase 1 für Ausgabe."""
    if not node_info:
        return "N/A"
    s_id = node_info["s_id"] or "—"
    node_type = node_info["type"] or "—"
    content = node_info["content"]
    
    if content:
        if len(content) > 25:
            content = content[:22] + "..."
        return f"id={node_info['id']:<2}, s_id={s_id:<15}, type={node_type:<12}, content='{content}'"
    else:
        return f"id={node_info['id']:<2}, s_id={s_id:<15}, type={node_type:<12}"


def format_node_info_phase3_sa(node_info: dict) -> str:
    """Formatiert Node-Information Phase 3 SA mit pre_min/pre_max für Ausgabe."""
    if not node_info:
        return "N/A"
    s_id = node_info["s_id"] or "—"
    node_type = node_info["type"] or "—"
    content = node_info["content"]
    pre_min = node_info["pre_min"]
    pre_max = node_info["pre_max"]
    
    if content:
        if len(content) > 25:
            content = content[:22] + "..."
        return f"id={node_info['id']:<2}, pre_min={pre_min:<2}, pre_max={pre_max:<2}, s_id={s_id:<15}, type={node_type:<12}, content='{content}'"
    else:
        return f"id={node_info['id']:<2}, pre_min={pre_min:<2}, pre_max={pre_max:<2}, s_id={s_id:<15}, type={node_type:<12}"


def format_ids(results):
    """Formatiert Node IDs für die Tabelle."""
    ids = sorted([str(row[0]) for row in results])
    return ", ".join(ids) if ids else "—"


def print_results_table(title, phase1_results, phase3_results):
    """Gibt Ergebnisse in Tabellen-Form aus."""
    print(f"\n{title}")
    print("=" * 90)
    print(f"{'Achse':<35} | {'Ergebnisknoten IDs':<40} | {'Größe':<5}")
    print("-" * 90)
    
    ids_p1 = format_ids(phase1_results)
    ids_p3 = format_ids(phase3_results)
    
    print(f"{'Phase 1':<35} | {ids_p1:<40} | {len(phase1_results):<5}")
    print(f"{'Phase 3 Single-Axis':<35} | {ids_p3:<40} | {len(phase3_results):<5}")
    
    print("=" * 90)


def print_node_details(conn, phase1_results, phase3_results):
    """Zeigt detaillierte Node-Informationen mit pre_min/pre_max für Vergleich."""
    print("\n" + "=" * 130)
    print("NODE-IDENTITÄTEN MIT PRE_MIN/PRE_MAX (Detaillierter Vergleich)")
    print("=" * 130)
    
    phase1_ids = sorted([row[0] for row in phase1_results])
    phase3_ids = sorted([row[0] for row in phase3_results])
    
    print("\nPHASE 1 Descendant-Knoten:")
    print("-" * 130)
    for node_id in phase1_ids:
        info = get_node_info_phase1(conn, node_id)
        print(f"  {format_node_info_phase1(info)}")
    
    print("\nPHASE 3 SINGLE-AXIS Descendant-Knoten (mit pre_min/pre_max):")
    print("-" * 130)
    for node_id in phase3_ids:
        info = get_node_info_phase3_sa(conn, node_id)
        print(f"  {format_node_info_phase3_sa(info)}")
    
    # Zeige Unterschiede
    only_phase1 = set(phase1_ids) - set(phase3_ids)
    only_phase3 = set(phase3_ids) - set(phase1_ids)
    
    if only_phase1 or only_phase3:
        print("\nUNTERSCHIEDE:")
        print("-" * 130)
        
        if only_phase1:
            print("\nNur in Phase 1 (nicht in Phase 3):")
            for node_id in sorted(only_phase1):
                info = get_node_info_phase1(conn, node_id)
                print(f"  {format_node_info_phase1(info)}")
        
        if only_phase3:
            print("\nNur in Phase 3 Single-Axis (nicht in Phase 1):")
            for node_id in sorted(only_phase3):
                info = get_node_info_phase3_sa(conn, node_id)
                print(f"  {format_node_info_phase3_sa(info)}")
    else:
        print("\n✓ Keine Unterschiede - identische Ergebnisse!")
    
    print("=" * 130)


def main():
    """Hauptfunktion."""
    base_dir = Path(__file__).parent
    toy_example = base_dir / "toy_example.txt"
    
    if not toy_example.exists():
        print(f"✗ Fehler: {toy_example.name} nicht gefunden!")
        return
    
    print("\n" + "=" * 130)
    print("VERIFIKATION: Phase 1 vs Phase 3 Single-Axis (1D Range Query - nur Descendant)")
    print("=" * 130)
    
    # Parse XML
    print(f"\n[PARSE] Lese {toy_example.name}...")
    root = EdgeModelBuilder().from_file(toy_example)
    
    all_nodes = root.walk()
    print(f"Gesamt-Knoten: {len(all_nodes)}")
    
    with get_connection() as conn:
        # Setup Phase 1
        print("\n[SETUP] Phase 1: EDGE Model...")
        setup_phase1(conn, root)
        
        # Setup Phase 3 Single-Axis
        print("[SETUP] Phase 3 Single-Axis: accel_single mit 1D Query...")
        setup_phase3_single_axis(conn, root)
        
        # Finde Test-Knoten (Year-Knoten für Descendant)
        print("\n[FIND] Suche Test-Knoten...")
        
        year_node = find_one(conn, type="year")
        print(f"  Year-Knoten: id={year_node[0]}, s_id={year_node[1]}")
        
        # ===== PHASE 1 QUERY =====
        print("\n[QUERY] Phase 1: Descendant (Rekursive CTE)...")
        phase1_descendant = descendant(conn, year_node[0])
        print(f"  Ergebnis: {len(phase1_descendant)} Knoten")
        
        # ===== PHASE 3 SINGLE-AXIS QUERY =====
        print("[QUERY] Phase 3 Single-Axis: Descendant (1D Range Query)...")
        phase3_descendant = descendant_single_axis(conn, year_node[0])
        print(f"  Ergebnis: {len(phase3_descendant)} Knoten")
        
        # ===== VERGLEICH =====
        print("\n[VERIFIKATION] Vergleich Phase 1 vs Phase 3 Single-Axis...")
        
        phase1_ids = sorted([row[0] for row in phase1_descendant])
        phase3_ids = sorted([row[0] for row in phase3_descendant])
        
        if phase1_ids == phase3_ids:
            print("  ✓ IDENTISCH: Beide liefern die gleichen Ergebnisse!")
        else:
            print("  ⚠ UNTERSCHIED: Die Ergebnisse unterscheiden sich!")
            only_phase1 = set(phase1_ids) - set(phase3_ids)
            only_phase3 = set(phase3_ids) - set(phase1_ids)
            if only_phase1:
                print(f"    Nur in Phase 1: {only_phase1}")
            if only_phase3:
                print(f"    Nur in Phase 3: {only_phase3}")
        
        # ===== AUSGABE TABELLE =====
        print_results_table(
            "DESCENDANT ACHSE (Year-Knoten)",
            phase1_descendant,
            phase3_descendant
        )
        
        # ===== NODE-DETAILS MIT PRE_MIN/PRE_MAX =====
        print_node_details(conn, phase1_descendant, phase3_descendant)
        
        # ===== ZUSAMMENFASSUNG =====
        print("\n" + "=" * 130)
        if phase1_ids == phase3_ids:
            print("✓ VERIFIKATION ERFOLGREICH: Phase 1 und Phase 3 Single-Axis liefern identische Ergebnisse!")
        else:
            print("⚠ VERIFIKATION MIT UNTERSCHIEDEN: Siehe Node-Identitäten oben.")
        print("=" * 130)


if __name__ == "__main__":
    main()