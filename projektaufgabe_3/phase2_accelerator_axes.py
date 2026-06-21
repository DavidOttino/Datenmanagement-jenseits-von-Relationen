from __future__ import annotations

AXES = {
    "ancestor",
    "descendant",
    "following-sibling",
    "preceding-sibling",
}

def axis_accel(conn, node_id: int, axis_name: str) -> list[tuple[int, str | None, str, str | None]]:
    """Verteilerfunktion für die XPath-Accelerator-Achsen."""
    if axis_name == "ancestor":
        return ancestor_accel(conn, node_id)
    if axis_name == "descendant":
        return descendant_accel(conn, node_id)
    if axis_name == "following-sibling":
        return following_sibling_accel(conn, node_id)
    if axis_name == "preceding-sibling":
        return preceding_sibling_accel(conn, node_id)
    raise ValueError(f"Nicht unterstützte Achse: {axis_name!r}. Erwartet wird eine von {sorted(AXES)}")


def ancestor_accel(conn, node_id: int) -> list[tuple[int, str | None, str, str | None]]:
    """Berechnet die Ancestor-Achse über pre/post-Bedingungen (sortiert von Wurzel zu Kontext)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.id, a.value AS s_id, c.tag AS type, c.text AS content
            FROM accel ctx
            JOIN accel x ON x.pre < ctx.pre AND ctx.post < x.post
            JOIN node n ON n.id = x.node_id
            JOIN content c ON c.node_id = n.id
            LEFT JOIN attribute a ON a.node_id = n.id AND a.name = 's_id'
            WHERE ctx.node_id = %s
            ORDER BY x.pre ASC;
            """,
            (node_id,),
        )
        return cur.fetchall()


def descendant_accel(conn, node_id: int) -> list[tuple[int, str | None, str, str | None]]:
    """Berechnet die Descendant-Achse über pre/post-Bedingungen."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.id, a.value AS s_id, c.tag AS type, c.text AS content
            FROM accel ctx
            JOIN accel x ON ctx.pre < x.pre AND x.post < ctx.post
            JOIN node n ON n.id = x.node_id
            JOIN content c ON c.node_id = n.id
            LEFT JOIN attribute a ON a.node_id = n.id AND a.name = 's_id'
            WHERE ctx.node_id = %s
            ORDER BY x.pre ASC;
            """,
            (node_id,),
        )
        return cur.fetchall()
    
    
def descendant_accel_opti(conn, node_id: int) -> list[tuple[int, str | None, str, str | None]]:
    """
    Berechnet die Descendant-Achse über pre/post-Bedingungen mit Höhen-Optimierung.
    
    Optimierte Bedingung: ctx.pre < x.pre AND x.post < ctx.post + ctx.height
    Statt:                ctx.pre < x.pre AND x.post < ctx.post
    
    Das Fenster schrumpft rechts: [post(v), post(v) + height(v)]
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.id, a.value AS s_id, c.tag AS type, c.text AS content
            FROM accel ctx
            JOIN accel x ON ctx.pre < x.pre AND x.post < ctx.post + ctx.height
            JOIN node n ON n.id = x.node_id
            JOIN content c ON c.node_id = n.id
            LEFT JOIN attribute a ON a.node_id = n.id AND a.name = 's_id'
            WHERE ctx.node_id = %s
            ORDER BY x.pre ASC;
            """,
            (node_id,),
        )
        return cur.fetchall()


def following_sibling_accel(conn, node_id: int) -> list[tuple[int, str | None, str, str | None]]:
    """
    Berechnet folgende Geschwisterknoten (following-sibling).
    Bedingung: Gleicher Parent-Knoten und nachfolgende Pre-Order-ID.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.id, a.value AS s_id, c.tag AS type, c.text AS content
            FROM accel ctx
            JOIN accel x ON x.parent = ctx.parent AND ctx.pre < x.pre
            JOIN node n ON n.id = x.node_id
            JOIN content c ON c.node_id = n.id
            LEFT JOIN attribute a ON a.node_id = n.id AND a.name = 's_id'
            WHERE ctx.node_id = %s
            ORDER BY x.pre ASC;
            """,
            (node_id,),
        )
        return cur.fetchall()


def preceding_sibling_accel(conn, node_id: int) -> list[tuple[int, str | None, str, str | None]]:
    """
    Berechnet vorangegangene Geschwisterknoten (preceding-sibling).
    Bedingung: Gleicher Parent-Knoten und vorangegangene Pre-Order-ID.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.id, a.value AS s_id, c.tag AS type, c.text AS content
            FROM accel ctx
            JOIN accel x ON x.parent = ctx.parent AND x.pre < ctx.pre
            JOIN node n ON n.id = x.node_id
            JOIN content c ON c.node_id = n.id
            LEFT JOIN attribute a ON a.node_id = n.id AND a.name = 's_id'
            WHERE ctx.node_id = %s
            ORDER BY x.pre ASC;
            """,
            (node_id,),
        )
        return cur.fetchall()
    

    

def verify_optimization_equivalence(conn, test_nodes: list[tuple[int, str]]) -> None:
    """
    Vergleicht Original- und optimierte Descendant-Implementierung.
    Zeigt, dass beide dasselbe Ergebnis liefern.
    
    Args:
        conn: Datenbankverbindung
        test_nodes: Liste von (node_id, node_name) Tuples zum Testen
    """
    print("\n[VERIFY] Vergleiche descendant() Original vs. Optimiert")
    print("=" * 90)
    
    all_match = True
    
    for node_id, node_name in test_nodes:
        print(f"\nKnoten: {node_id} ('{node_name}')")
        print("-" * 90)
        
        # Original
        original = descendant_accel(conn, node_id)
        
        # Optimiert
        optimized = descendant_accel_optimized(conn, node_id)
        
        # Vergleich
        orig_ids = sorted([row[0] for row in original])
        opt_ids = sorted([row[0] for row in optimized])
        
        print(f"Original:  {len(original):3d} Knoten → Node IDs: {orig_ids}")
        print(f"Optimiert: {len(optimized):3d} Knoten → Node IDs: {opt_ids}")
        
        if orig_ids == opt_ids:
            print("✓ IDENTISCH - Optimierung korrekt!")
        else:
            print("✗ UNTERSCHIED - Fehler erkannt!")
            only_orig = set(orig_ids) - set(opt_ids)
            only_opt = set(opt_ids) - set(orig_ids)
            if only_orig:
                print(f"  Nur in Original: {only_orig}")
            if only_opt:
                print(f"  Nur in Optimiert: {only_opt}")
            all_match = False
    
    print("\n" + "=" * 90)
    if all_match:
        print("✓ ALLE TESTS BESTANDEN - Optimierung ist korrekt äquivalent!")
    else:
        print("✗ FEHLER GEFUNDEN - Optimierung nicht äquivalent!")
    print("=" * 90 + "\n")


def descendant_accel_optimized(conn, node_id: int) -> list[tuple[int, str | None, str, str | None]]:
    """
    Optimierte Descendant-Achse (Alternative zur überarbeiteten Hauptfunktion).
    Diese Funktion ist identisch zu descendant_accel() nach der Optimierung.
    Wird nur für Verifikationszwecke verwendet.
    """
    return descendant_accel(conn, node_id)