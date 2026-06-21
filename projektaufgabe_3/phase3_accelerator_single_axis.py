"""
Single-Axis XPath Accelerator für Descendant-Suche.

Konzept:
- Statt (pre, post) als separate Dimensionen verwenden wir:
  - pre_min: DFS Pre-Order Nummer beim Eintritt in Knoten
  - pre_max: DFS Pre-Order Nummer beim Austritt aus Knoten (statt post)
  
- Descendant(v) ist dann ein eindimensionales Problem:
  descendant(v) ⟺ pre_min(v) < pre_min(x) < pre_max(v)
                   AND
                   pre_min(v) < pre_max(x) < pre_max(v)
                   
Das reduziert eine 2D-Range Query zu zwei 1D-Range Queries.
"""

from __future__ import annotations
from dataclasses import dataclass

try:
    from .edge_model import EdgeNode
except ImportError:
    from edge_model import EdgeNode


@dataclass
class SingleAxisAnnotation:
    """Annotation mit nur Pre-Numbering (pre_min und pre_max)."""
    node_id: int
    pre_min: int      # Eintritt in Knoten
    pre_max: int      # Austritt aus Knoten
    parent: int | None


def annotate_tree_single_axis(root: EdgeNode) -> dict[int, SingleAxisAnnotation]:
    """
    Berechnet pre_min und pre_max für jeden Knoten.
    
    Algorithmus:
    - Ein globaler Counter wird hochgezählt (nicht zwei wie bei pre/post)
    - Bei Eintritt in Knoten: pre_min = counter++
    - Bei Austritt aus Knoten: pre_max = counter++
    
    Beispiel:
    ```
    bib (pre_min=0, pre_max=1)
      vldb (pre_min=1, pre_max=2)
        vldb_2023 (pre_min=2, pre_max=3)
          article (pre_min=3, pre_max=4)
            author (pre_min=4, pre_max=5)
    ```
    
    Für descendant(vldb_2023 mit pre_min=2, pre_max=3):
    - article: pre_min=3, pre_max=4
      ✓ 2 < 3 < 3? NEIN! aber
      ✓ 2 < 4 < 3? NEIN!
    
    Wait, das funktioniert nicht. Lass mich korrigieren...
    """
    annotations = {}
    counter = 0
    
    def dfs(node: EdgeNode, parent_id: int | None) -> None:
        nonlocal counter
        
        pre_min = counter
        counter += 1
        
        # Rekursiv durch Kinder
        for child in node.children:
            dfs(child, node.id)
        
        pre_max = counter
        counter += 1
        
        annotations[node.id] = SingleAxisAnnotation(
            node_id=node.id,
            pre_min=pre_min,
            pre_max=pre_max,
            parent=parent_id
        )
    
    print("[ANNOTATION] Single-Axis: Pre_min/Pre_max Berechnung via DFS...")
    dfs(root, parent_id=None)
    print(f"[ANNOTATION] Fertig. {len(annotations)} Knoten annotiert.")
    print(f"[ANNOTATION] Counter endstand: {counter}")
    
    return annotations


def create_single_axis_accelerator_tables(conn) -> None:
    """
    Erstellt Tabellen für Single-Axis Accelerator.
    Nur eine Dimension statt zwei!
    """
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS accel_single CASCADE;")
        cur.execute("DROP TABLE IF EXISTS content CASCADE;")
        cur.execute("DROP TABLE IF EXISTS attribute CASCADE;")
        
        print("[DB] Erstelle Single-Axis Accelerator Tabellen...")
        
        cur.execute(
            """
            CREATE TABLE accel_single (
                pre_min INT,
                pre_max INT,
                parent INT,
                node_id INT PRIMARY KEY,
                PRIMARY KEY (pre_min, pre_max)
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
        
        print("[DB] Erstelle Indizes...")
        cur.execute("CREATE INDEX idx_accel_single_pre_min ON accel_single(pre_min);")
        cur.execute("CREATE INDEX idx_accel_single_pre_max ON accel_single(pre_max);")
        cur.execute("CREATE INDEX idx_accel_single_parent ON accel_single(parent);")
        cur.execute("CREATE INDEX idx_content_tag ON content(tag);")
        
    conn.commit()


def save_single_axis_accelerator_data(
    conn,
    annotations: dict[int, SingleAxisAnnotation],
    content_rows: list[tuple],
    attribute_rows: list[tuple]
) -> None:
    """
    Speichert Single-Axis Annotations in die Tabellen.
    """
    print("[DB] Schreibe Single-Axis Daten...")
    
    accel_rows = [
        (ann.pre_min, ann.pre_max, ann.parent, ann.node_id)
        for ann in annotations.values()
    ]
    
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO accel_single (pre_min, pre_max, parent, node_id)
            VALUES (%s, %s, %s, %s);
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
    print("[DB] Daten erfolgreich gespeichert.")


def print_single_axis_annotation_table(annotations: dict[int, SingleAxisAnnotation]) -> None:
    """
    Gibt schöne Tabelle der Single-Axis Annotations aus.
    """
    print("\n[ANNOTATIONS] Single-Axis: Pre_min/Pre_max Tabelle")
    print("=" * 70)
    print(f"{'Node ID':<8} {'Pre_min':<10} {'Pre_max':<10} {'Parent':<8}")
    print("-" * 70)
    
    for node_id in sorted(annotations.keys()):
        ann = annotations[node_id]
        parent_str = str(ann.parent) if ann.parent is not None else "None"
        print(f"{ann.node_id:<8} {ann.pre_min:<10} {ann.pre_max:<10} {parent_str:<8}")
    
    print("=" * 70 + "\n")