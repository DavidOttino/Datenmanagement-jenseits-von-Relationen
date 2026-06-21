"""
Umfassender Benchmark für XPath Accelerator Varianten.

Vergleicht vier Implementierungen:
1. Phase 1: EDGE Model mit Standard-Indexen
2. Phase 2: XPath Accelerator (2D Range Query) 
3. Phase 3 Optimiert: Verkleinertes Fenster + Height
4. Phase 3 Single-Axis: Nur Descendant mit 1D Query

Tabellennamen:
- Phase 1: node, edge
- Phase 2: accel, content, attribute
- Phase 3 Opt: accel_opt, content, attribute
- Phase 3 SA: accel_single, content, attribute
"""

from __future__ import annotations
import time
import random
import gzip
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Callable
import matplotlib.pyplot as plt
import numpy as np

try:
    from .connection import get_connection
    from .db_storage import reset_phase1_tables, create_phase1_tables, save_edge_model
    from .edge_model import EdgeModelBuilder, EdgeNode, PUBLICATION_TYPES, _venue_from_key, _resolve_named_entities
    from .edge_axes import ancestor, descendant, following_sibling, preceding_sibling, find_one
    from .phase2_accelerator_axes import ancestor_accel, descendant_accel, following_sibling_accel, preceding_sibling_accel
except ImportError:
    from connection import get_connection
    from db_storage import reset_phase1_tables, create_phase1_tables, save_edge_model
    from edge_model import EdgeModelBuilder, EdgeNode, PUBLICATION_TYPES, _venue_from_key, _resolve_named_entities
    from edge_axes import ancestor, descendant, following_sibling, preceding_sibling, find_one
    from phase2_accelerator_axes import ancestor_accel, descendant_accel, following_sibling_accel, preceding_sibling_accel


# ============================================================================
# DATENGENERIERUNG: Vergrößerung von my_small_bib.xml
# ============================================================================

class DatasetGenerator:
    """
    Generiert vergrößerte Datensätze durch Hinzufügen von zusätzlichen Venues.
    """
    
    # Zusätzliche Venues (über die Standard VLDB, SIGMOD, ICDE hinaus)
    ADDITIONAL_VENUES = [
        ("conf/pods/", "pods"),
        ("conf/edbt/", "edbt"),
        ("conf/vldbj/", "vldbj"),
        ("conf/cidr/", "cidr"),
        ("journals/tods/", "tods"),
        ("journals/dapd/", "dapd"),
        ("conf/icdt/", "icdt"),
        ("conf/btw/", "btw"),
        ("conf/ssdbm/", "ssdbm"),
        ("conf/cikm/", "cikm"),
    ]
    
    @staticmethod
    def create_enlarged_dataset(
        source_path: Path,
        output_path: Path,
        multiplier: int
    ) -> None:
        """
        Erstellt einen vergrößerten Datensatz durch Replizierung mit zusätzlichen Venues.
        
        Args:
            source_path: my_small_bib.xml Pfad
            output_path: Zielpath für vergrößerten Datensatz
            multiplier: 1=original, 2=2x, 4=4x, etc.
        """
        print(f"[GENERATOR] Erstelle {multiplier}x vergrößerten Datensatz...")
        
        # Lese Quell-XML
        with open(source_path, 'r', encoding='utf-8') as f:
            source_xml = f.read()
        
        # Parse nur die <publication> Elemente
        root = ET.fromstring(source_xml)
        publications = list(root)
        
        print(f"[GENERATOR] Original-Publikationen: {len(publications)}")
        
        # Generiere neue XML mit Replizierung
        new_root = ET.Element("bib")
        
        # Originalveröffentlichungen
        for pub in publications:
            new_root.append(pub)
        
        # Repliziere mit neuen Venues
        num_venues_to_add = multiplier - 1
        venues_to_use = DatasetGenerator.ADDITIONAL_VENUES[:num_venues_to_add]
        
        for venue_key_prefix, venue_name in venues_to_use:
            for i, original_pub in enumerate(publications):
                # Clone Publikation
                cloned = ET.Element(original_pub.tag)
                cloned.attrib['mdate'] = original_pub.attrib.get('mdate', '')
                
                # Modifiziere Key
                original_key = original_pub.attrib.get('key', '')
                base_name = original_key.rsplit('/', 1)[-1] if '/' in original_key else original_key
                new_key = f"{venue_key_prefix}{base_name[:-2]}{i:03d}"
                cloned.attrib['key'] = new_key
                
                # Kopiere alle child elements
                for child in original_pub:
                    cloned.append(ET.fromstring(ET.tostring(child)))
                
                new_root.append(cloned)
        
        # Schreibe neue XML
        tree = ET.ElementTree(new_root)
        tree.write(output_path, encoding='utf-8', xml_declaration=True)
        
        # Verifizierung
        with open(output_path, 'r', encoding='utf-8') as f:
            new_xml = f.read()
        new_root_verify = ET.fromstring(new_xml)
        
        print(f"[GENERATOR] Neue Publikationen: {len(list(new_root_verify))}")
        print(f"[GENERATOR] Dateigrößen-Verhältnis: {len(new_xml) / len(source_xml):.2f}x")


# ============================================================================
# PHASE 1: EDGE Model mit Standard-Indexen
# ============================================================================

def setup_phase1(conn, root: EdgeNode) -> None:
    """
    Setup Phase 1: Speichere EDGE Model in node/edge Tabellen.
    """
    reset_phase1_tables(conn)
    create_phase1_tables(conn)
    save_edge_model(conn, root)


# ============================================================================
# PHASE 2: XPath Accelerator mit 2D Range Query
# ============================================================================

def setup_phase2(conn, root: EdgeNode) -> None:
    """
    Setup Phase 2: Erstelle accel Tabelle mit pre/post/parent/node_id.
    Tabellennamen: accel, content, attribute
    """
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS attribute CASCADE;")
        cur.execute("DROP TABLE IF EXISTS content CASCADE;")
        cur.execute("DROP TABLE IF EXISTS accel CASCADE;")
    
    # Annotiere Tree mit pre/post
    accel_rows = []
    content_rows = []
    attribute_rows = []
    
    pre_counter = 0
    post_counter = 0
    
    def dfs(node: EdgeNode, parent_pre: int | None) -> int:
        nonlocal pre_counter, post_counter
        
        current_pre = pre_counter
        pre_counter += 1
        
        for child in node.children:
            dfs(child, current_pre)
        
        current_post = post_counter
        post_counter += 1
        
        accel_rows.append((current_pre, current_post, parent_pre, node.id))
        content_rows.append((node.id, node.type, node.content))
        if node.s_id is not None:
            attribute_rows.append((node.id, "s_id", node.s_id))
        
        return current_post
    
    dfs(root, parent_pre=None)
    
    # Erstelle Tabellen
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE accel (
                pre INT PRIMARY KEY,
                post INT NOT NULL,
                parent INT,
                node_id INT NOT NULL
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
        
        # Indizes für Phase 2 (2D Range Query)
        cur.execute("CREATE INDEX idx_accel_pre_post ON accel(pre, post);")
        cur.execute("CREATE INDEX idx_accel_parent ON accel(parent);")
        
        cur.executemany(
            """
            INSERT INTO accel (pre, post, parent, node_id)
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


# ============================================================================
# PHASE 3 OPTIMIERT: Verkleinertes Fenster mit Height
# ============================================================================

def setup_phase3_optimized(conn, root: EdgeNode) -> None:
    """
    Setup Phase 3 Optimiert: Fenster mit height-Information verkleinern.
    Tabellennamen: accel_opt, content, attribute
    """
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS attribute CASCADE;")
        cur.execute("DROP TABLE IF EXISTS content CASCADE;")
        cur.execute("DROP TABLE IF EXISTS accel_opt CASCADE;")
    
    # Annotiere mit pre/post/height
    accel_rows = []
    content_rows = []
    attribute_rows = []
    
    pre_counter = 0
    post_counter = 0
    
    def dfs(node: EdgeNode, parent_pre: int | None) -> tuple[int, int]:
        nonlocal pre_counter, post_counter
        
        current_pre = pre_counter
        pre_counter += 1
        
        max_child_height = -1
        for child in node.children:
            _, child_height = dfs(child, current_pre)
            max_child_height = max(max_child_height, child_height)
        
        current_post = post_counter
        post_counter += 1
        
        height = max_child_height + 1
        
        accel_rows.append((current_pre, current_post, parent_pre, node.id, height))
        content_rows.append((node.id, node.type, node.content))
        if node.s_id is not None:
            attribute_rows.append((node.id, "s_id", node.s_id))
        
        return current_pre, height
    
    dfs(root, parent_pre=None)
    
    # Erstelle Tabellen
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE accel_opt (
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
        
        # Indizes für Phase 3 (2D Range Query mit Höhe-Pruning)
        cur.execute("CREATE INDEX idx_accel_opt_pre_post ON accel_opt(pre, post);")
        cur.execute("CREATE INDEX idx_accel_opt_parent ON accel_opt(parent);")
        
        cur.executemany(
            """
            INSERT INTO accel_opt (pre, post, parent, node_id, height)
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


def descendant_phase3_optimized(conn, node_id: int) -> list:
    """Phase 3 Optimiert: Descendant mit verkleinertem Fenster."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.id, a.value AS s_id, c.tag AS type, c.text AS content
            FROM accel_opt ctx
            JOIN accel_opt x ON ctx.pre < x.pre AND x.post < ctx.post + ctx.height
            JOIN node n ON n.id = x.node_id
            JOIN content c ON c.node_id = n.id
            LEFT JOIN attribute a ON a.node_id = n.id AND a.name = 's_id'
            WHERE ctx.node_id = %s
            ORDER BY x.pre ASC;
            """,
            (node_id,),
        )
        return cur.fetchall()


# ============================================================================
# PHASE 3 SINGLE-AXIS: Nur Descendant mit 1D Query
# ============================================================================

def setup_phase3_single_axis(conn, root: EdgeNode) -> None:
    """
    Setup Phase 3 Single-Axis: Descendant mit 1D pre_min/pre_max Query.
    Tabellennamen: accel_single, content, attribute
    """
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS attribute CASCADE;")
        cur.execute("DROP TABLE IF EXISTS content CASCADE;")
        cur.execute("DROP TABLE IF EXISTS accel_single CASCADE;")
    
    # Annotiere mit pre_min/pre_max
    accel_rows = []
    content_rows = []
    attribute_rows = []
    
    counter = 0
    
    def dfs(node: EdgeNode, parent_id: int | None) -> None:
        nonlocal counter
        
        pre_min = counter
        counter += 1
        
        for child in node.children:
            dfs(child, node.id)
        
        pre_max = counter
        counter += 1
        
        accel_rows.append((pre_min, pre_max, parent_id, node.id))
        content_rows.append((node.id, node.type, node.content))
        if node.s_id is not None:
            attribute_rows.append((node.id, "s_id", node.s_id))
    
    dfs(root, parent_id=None)
    
    # Erstelle Tabellen mit Clustered B+-Tree (PRIMARY KEY auf pre_min)
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE accel_single (
                pre_min INT PRIMARY KEY,
                pre_max INT NOT NULL,
                parent INT,
                node_id INT NOT NULL
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


def descendant_phase3_single_axis(conn, node_id: int) -> list:
    """Phase 3 Single-Axis: Descendant mit 1D Range Query."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.id, a.value AS s_id, c.tag AS type, c.text AS content
            FROM accel_single ctx
            JOIN accel_single x ON (
                ctx.pre_min < x.pre_min AND x.pre_min < ctx.pre_max
                AND
                ctx.pre_min < x.pre_max AND x.pre_max < ctx.pre_max
            )
            JOIN node n ON n.id = x.node_id
            JOIN content c ON c.node_id = n.id
            LEFT JOIN attribute a ON a.node_id = n.id AND a.name = 's_id'
            WHERE ctx.node_id = %s
            ORDER BY x.pre_min ASC;
            """,
            (node_id,),
        )
        return cur.fetchall()


# ============================================================================
# BENCHMARK RUNNER
# ============================================================================

class BenchmarkRunner:
    """
    Führt Benchmarks für alle vier Varianten durch.
    """
    
    def __init__(self):
        self.results = {
            "phase1": {"ancestor": [], "descendant": [], "sibling": []},
            "phase2": {"ancestor": [], "descendant": [], "sibling": []},
            "phase3_opt": {"ancestor": [], "descendant": [], "sibling": []},
            "phase3_sa": {"descendant": []},  # Nur descendant
        }
        self.data_sizes = []
    
    def run_full_benchmark(self, base_xml_path: Path, multipliers: list[int] = None) -> None:
        """
        Führt kompletten Benchmark für verschiedene Datengrößen durch.
        
        Args:
            base_xml_path: Pfad zu my_small_bib.xml
            multipliers: Liste von Multiplikationen (z.B. [1, 2, 4, 8])
        """
        if multipliers is None:
            multipliers = [1, 2, 4, 8, 16]
        
        print("\n" + "=" * 100)
        print("PHASE 3 BENCHMARK: XPath Accelerator Varianten")
        print("=" * 100)
        
        for multiplier in multipliers:
            print(f"\n[BENCHMARK] Multiplier: {multiplier}x")
            print("-" * 100)
            
            # Generiere Datensatz
            enlarged_path = base_xml_path.parent / f"toy_example_{multiplier}x.xml"
            if multiplier == 1:
                enlarged_path = base_xml_path
            else:
                DatasetGenerator.create_enlarged_dataset(base_xml_path, enlarged_path, multiplier)
            
            # Parse XML
            print(f"[PARSE] Lese {enlarged_path.name}...")
            root = EdgeModelBuilder().from_file(enlarged_path)
            
            # Zähle Knoten
            all_nodes = root.walk()
            article_nodes = [n for n in all_nodes if n.type == "article"]
            year_nodes = [n for n in all_nodes if n.type == "year"]
            
            print(f"  Gesamt-Knoten: {len(all_nodes)}")
            print(f"  Article-Knoten: {len(article_nodes)}")
            print(f"  Year-Knoten: {len(year_nodes)}")
            
            self.data_sizes.append(len(all_nodes))
            
            if not article_nodes or not year_nodes:
                print("  ✗ Nicht genug Knoten für Benchmark!")
                continue
            
            # Benchmark für alle Varianten
            with get_connection() as conn:
                # Phase 1
                print("[SETUP] Phase 1: EDGE Model...")
                setup_phase1(conn, root)
                self._benchmark_phase1(conn, article_nodes, year_nodes, multiplier)
                
                # Phase 2
                print("[SETUP] Phase 2: 2D Range Query...")
                setup_phase2(conn, root)
                self._benchmark_phase2(conn, article_nodes, year_nodes, multiplier)
                
                # Phase 3 Optimiert
                print("[SETUP] Phase 3 Optimiert: Fenster-Verkleinerung...")
                setup_phase3_optimized(conn, root)
                self._benchmark_phase3_optimized(conn, article_nodes, year_nodes, multiplier)
                
                # Phase 3 Single-Axis
                print("[SETUP] Phase 3 Single-Axis: 1D Query...")
                setup_phase3_single_axis(conn, root)
                self._benchmark_phase3_single_axis(conn, year_nodes, multiplier)
    
    def _benchmark_phase1(self, conn, article_nodes: list, year_nodes: list, multiplier: int) -> None:
        """Benchmark Phase 1 (EDGE Model)."""
        print("[BENCHMARK] Phase 1: EDGE Model mit edge/node Tabellen")
        
        # Ancestor (Article)
        test_article = random.choice(article_nodes)
        start = time.time()
        for _ in range(5):
            ancestor(conn, test_article.id)
        elapsed = (time.time() - start) / 5
        self.results["phase1"]["ancestor"].append(elapsed)
        print(f"  Ancestor: {elapsed*1000:.3f} ms")
        
        # Descendant (Year)
        test_year = random.choice(year_nodes)
        start = time.time()
        for _ in range(5):
            descendant(conn, test_year.id)
        elapsed = (time.time() - start) / 5
        self.results["phase1"]["descendant"].append(elapsed)
        print(f"  Descendant: {elapsed*1000:.3f} ms")
        
        # Sibling (Article)
        start = time.time()
        for _ in range(5):
            if random.random() < 0.5:
                following_sibling(conn, test_article.id)
            else:
                preceding_sibling(conn, test_article.id)
        elapsed = (time.time() - start) / 5
        self.results["phase1"]["sibling"].append(elapsed)
        print(f"  Sibling: {elapsed*1000:.3f} ms")
    
    def _benchmark_phase2(self, conn, article_nodes: list, year_nodes: list, multiplier: int) -> None:
        """Benchmark Phase 2 (2D Range Query mit accel Tabelle)."""
        print("[BENCHMARK] Phase 2: XPath Accelerator (2D Range Query)")
        
        # Ancestor (Article)
        test_article = random.choice(article_nodes)
        start = time.time()
        for _ in range(5):
            ancestor_accel(conn, test_article.id)
        elapsed = (time.time() - start) / 5
        self.results["phase2"]["ancestor"].append(elapsed)
        print(f"  Ancestor: {elapsed*1000:.3f} ms")
        
        # Descendant (Year)
        test_year = random.choice(year_nodes)
        start = time.time()
        for _ in range(5):
            descendant_accel(conn, test_year.id)
        elapsed = (time.time() - start) / 5
        self.results["phase2"]["descendant"].append(elapsed)
        print(f"  Descendant: {elapsed*1000:.3f} ms")
        
        # Sibling (Article)
        start = time.time()
        for _ in range(5):
            if random.random() < 0.5:
                following_sibling_accel(conn, test_article.id)
            else:
                preceding_sibling_accel(conn, test_article.id)
        elapsed = (time.time() - start) / 5
        self.results["phase2"]["sibling"].append(elapsed)
        print(f"  Sibling: {elapsed*1000:.3f} ms")
    
    def _benchmark_phase3_optimized(self, conn, article_nodes: list, year_nodes: list, multiplier: int) -> None:
        """Benchmark Phase 3 Optimiert (accel_opt Tabelle mit Fenster-Verkleinerung)."""
        print("[BENCHMARK] Phase 3 Optimiert: Verkleinertes Fenster + Height")
        
        # Ancestor (Article) - nutzt accel_opt, aber keine Optimierung
        test_article = random.choice(article_nodes)
        start = time.time()
        for _ in range(5):
            ancestor_accel(conn, test_article.id)  # Ancestor ändert sich nicht
        elapsed = (time.time() - start) / 5
        self.results["phase3_opt"]["ancestor"].append(elapsed)
        print(f"  Ancestor: {elapsed*1000:.3f} ms")
        
        # Descendant (Year) - nutzt optimierte Query
        test_year = random.choice(year_nodes)
        start = time.time()
        for _ in range(5):
            descendant_phase3_optimized(conn, test_year.id)
        elapsed = (time.time() - start) / 5
        self.results["phase3_opt"]["descendant"].append(elapsed)
        print(f"  Descendant: {elapsed*1000:.3f} ms")
        
        # Sibling (Article) - nutzt accel_opt, aber keine Optimierung
        start = time.time()
        for _ in range(5):
            if random.random() < 0.5:
                following_sibling_accel(conn, test_article.id)
            else:
                preceding_sibling_accel(conn, test_article.id)
        elapsed = (time.time() - start) / 5
        self.results["phase3_opt"]["sibling"].append(elapsed)
        print(f"  Sibling: {elapsed*1000:.3f} ms")
    
    def _benchmark_phase3_single_axis(self, conn, year_nodes: list, multiplier: int) -> None:
        """Benchmark Phase 3 Single-Axis (accel_single Tabelle mit 1D Query)."""
        print("[BENCHMARK] Phase 3 Single-Axis: 1D Range Query (nur Descendant)")
        
        # Descendant (Year) - nutzt 1D Query
        test_year = random.choice(year_nodes)
        start = time.time()
        for _ in range(5):
            descendant_phase3_single_axis(conn, test_year.id)
        elapsed = (time.time() - start) / 5
        self.results["phase3_sa"]["descendant"].append(elapsed)
        print(f"  Descendant: {elapsed*1000:.3f} ms")
    
    def plot_results(self, output_dir: Path = None) -> None:
        """
        Visualisiert die Benchmark-Ergebnisse.
        """
        if output_dir is None:
            output_dir = Path.cwd()
        
        output_dir.mkdir(exist_ok=True)
        
        print("\n" + "=" * 100)
        print("VISUALIZATION")
        print("=" * 100)
        
        # Figure mit Subplots
        fig, axes = plt.subplots(1, 3, figsize=(18, 5))
        fig.suptitle("XPath Accelerator Benchmark: Phase 1 vs Phase 2 vs Phase 3", fontsize=16, fontweight='bold')
        
        # Daten vorbereiten
        data_sizes_mb = [size / 1000 for size in self.data_sizes]
        
        # --- ANCESTOR ---
        ax = axes[0]
        ax.plot(data_sizes_mb, [t*1000 for t in self.results["phase1"]["ancestor"]], 
                marker='o', label='Phase 1: EDGE Model', linewidth=2)
        ax.plot(data_sizes_mb, [t*1000 for t in self.results["phase2"]["ancestor"]], 
                marker='s', label='Phase 2: 2D Range Query', linewidth=2)
        ax.plot(data_sizes_mb, [t*1000 for t in self.results["phase3_opt"]["ancestor"]], 
                marker='^', label='Phase 3 Opt: Fenster-Verkl.', linewidth=2)
        ax.set_xlabel('Datengröße (approx. Knoten)', fontsize=11)
        ax.set_ylabel('Zeit (ms)', fontsize=11)
        ax.set_title('Ancestor Achse\n(Article-Knoten)', fontsize=12, fontweight='bold')
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.set_yscale('log')
        
        # --- DESCENDANT ---
        ax = axes[1]
        ax.plot(data_sizes_mb, [t*1000 for t in self.results["phase1"]["descendant"]], 
                marker='o', label='Phase 1: EDGE Model', linewidth=2)
        ax.plot(data_sizes_mb, [t*1000 for t in self.results["phase2"]["descendant"]], 
                marker='s', label='Phase 2: 2D Range Query', linewidth=2)
        ax.plot(data_sizes_mb, [t*1000 for t in self.results["phase3_opt"]["descendant"]], 
                marker='^', label='Phase 3 Opt: Fenster-Verkl.', linewidth=2)
        ax.plot(data_sizes_mb, [t*1000 for t in self.results["phase3_sa"]["descendant"]], 
                marker='D', label='Phase 3 SA: 1D Query', linewidth=2)
        ax.set_xlabel('Datengröße (approx. Knoten)', fontsize=11)
        ax.set_ylabel('Zeit (ms)', fontsize=11)
        ax.set_title('Descendant Achse\n(Year-Knoten)', fontsize=12, fontweight='bold')
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.set_yscale('log')
        
        # --- SIBLING ---
        ax = axes[2]
        ax.plot(data_sizes_mb, [t*1000 for t in self.results["phase1"]["sibling"]], 
                marker='o', label='Phase 1: EDGE Model', linewidth=2)
        ax.plot(data_sizes_mb, [t*1000 for t in self.results["phase2"]["sibling"]], 
                marker='s', label='Phase 2: 2D Range Query', linewidth=2)
        ax.plot(data_sizes_mb, [t*1000 for t in self.results["phase3_opt"]["sibling"]], 
                marker='^', label='Phase 3 Opt: Fenster-Verkl.', linewidth=2)
        ax.set_xlabel('Datengröße (approx. Knoten)', fontsize=11)
        ax.set_ylabel('Zeit (ms)', fontsize=11)
        ax.set_title('Sibling Achsen\n(Following/Preceding)', fontsize=12, fontweight='bold')
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.set_yscale('log')
        
        plt.tight_layout()
        
        # Speichere
        output_file = output_dir / "benchmark_results.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"\n✓ Benchmark-Plot gespeichert: {output_file}")
        
        # Zusätzlicher Plot: Speedup vs Phase 1
        fig, axes = plt.subplots(1, 3, figsize=(18, 5))
        fig.suptitle("Speedup gegenüber Phase 1 (EDGE Model)", fontsize=16, fontweight='bold')
        
        # --- ANCESTOR Speedup ---
        ax = axes[0]
        phase1_times = np.array(self.results["phase1"]["ancestor"])
        speedup_phase2 = phase1_times / np.array(self.results["phase2"]["ancestor"])
        speedup_phase3 = phase1_times / np.array(self.results["phase3_opt"]["ancestor"])
        ax.plot(data_sizes_mb, speedup_phase2, marker='s', label='Phase 2 Speedup', linewidth=2)
        ax.plot(data_sizes_mb, speedup_phase3, marker='^', label='Phase 3 Opt Speedup', linewidth=2)
        ax.axhline(y=1, color='red', linestyle='--', alpha=0.5, label='Baseline (Phase 1)')
        ax.set_xlabel('Datengröße', fontsize=11)
        ax.set_ylabel('Speedup Factor', fontsize=11)
        ax.set_title('Ancestor Speedup', fontsize=12, fontweight='bold')
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        
        # --- DESCENDANT Speedup ---
        ax = axes[1]
        phase1_times = np.array(self.results["phase1"]["descendant"])
        speedup_phase2 = phase1_times / np.array(self.results["phase2"]["descendant"])
        speedup_phase3_opt = phase1_times / np.array(self.results["phase3_opt"]["descendant"])
        speedup_phase3_sa = phase1_times / np.array(self.results["phase3_sa"]["descendant"])
        ax.plot(data_sizes_mb, speedup_phase2, marker='s', label='Phase 2 Speedup', linewidth=2)
        ax.plot(data_sizes_mb, speedup_phase3_opt, marker='^', label='Phase 3 Opt Speedup', linewidth=2)
        ax.plot(data_sizes_mb, speedup_phase3_sa, marker='D', label='Phase 3 SA Speedup', linewidth=2)
        ax.axhline(y=1, color='red', linestyle='--', alpha=0.5, label='Baseline (Phase 1)')
        ax.set_xlabel('Datengröße', fontsize=11)
        ax.set_ylabel('Speedup Factor', fontsize=11)
        ax.set_title('Descendant Speedup', fontsize=12, fontweight='bold')
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        
        # --- SIBLING Speedup ---
        ax = axes[2]
        phase1_times = np.array(self.results["phase1"]["sibling"])
        speedup_phase2 = phase1_times / np.array(self.results["phase2"]["sibling"])
        speedup_phase3 = phase1_times / np.array(self.results["phase3_opt"]["sibling"])
        ax.plot(data_sizes_mb, speedup_phase2, marker='s', label='Phase 2 Speedup', linewidth=2)
        ax.plot(data_sizes_mb, speedup_phase3, marker='^', label='Phase 3 Opt Speedup', linewidth=2)
        ax.axhline(y=1, color='red', linestyle='--', alpha=0.5, label='Baseline (Phase 1)')
        ax.set_xlabel('Datengröße', fontsize=11)
        ax.set_ylabel('Speedup Factor', fontsize=11)
        ax.set_title('Sibling Speedup', fontsize=12, fontweight='bold')
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        output_file = output_dir / "benchmark_speedup.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Speedup-Plot gespeichert: {output_file}")
        
        plt.show()
    
    def print_summary(self) -> None:
        """Gibt eine Text-Zusammenfassung aus."""
        print("\n" + "=" * 100)
        print("BENCHMARK SUMMARY")
        print("=" * 100)
        
        print("\nAncestor Achse (Article-Knoten):")
        print(f"{'Multiplier':<12} {'Phase 1':<12} {'Phase 2':<12} {'Phase 3 Opt':<12}")
        print("-" * 50)
        for i, mult in enumerate([1, 2, 4, 8, 16]):
            if i < len(self.results["phase1"]["ancestor"]):
                print(f"{mult}x{'':<9} {self.results['phase1']['ancestor'][i]*1000:>10.3f} ms "
                      f"{self.results['phase2']['ancestor'][i]*1000:>10.3f} ms "
                      f"{self.results['phase3_opt']['ancestor'][i]*1000:>10.3f} ms")
        
        print("\nDescendant Achse (Year-Knoten):")
        print(f"{'Multiplier':<12} {'Phase 1':<12} {'Phase 2':<12} {'Phase 3 Opt':<12} {'Phase 3 SA':<12}")
        print("-" * 62)
        for i, mult in enumerate([1, 2, 4, 8, 16]):
            if i < len(self.results["phase1"]["descendant"]):
                print(f"{mult}x{'':<9} {self.results['phase1']['descendant'][i]*1000:>10.3f} ms "
                      f"{self.results['phase2']['descendant'][i]*1000:>10.3f} ms "
                      f"{self.results['phase3_opt']['descendant'][i]*1000:>10.3f} ms "
                      f"{self.results['phase3_sa']['descendant'][i]*1000:>10.3f} ms")
        
        print("\nSibling Achsen (Article-Knoten):")
        print(f"{'Multiplier':<12} {'Phase 1':<12} {'Phase 2':<12} {'Phase 3 Opt':<12}")
        print("-" * 50)
        for i, mult in enumerate([1, 2, 4, 8, 16]):
            if i < len(self.results["phase1"]["sibling"]):
                print(f"{mult}x{'':<9} {self.results['phase1']['sibling'][i]*1000:>10.3f} ms "
                      f"{self.results['phase2']['sibling'][i]*1000:>10.3f} ms "
                      f"{self.results['phase3_opt']['sibling'][i]*1000:>10.3f} ms")
        
        print("\n" + "=" * 100)


# ============================================================================
# MAIN
# ============================================================================

def main():
    """
    Hauptfunktion: Führt den vollständigen Benchmark durch.
    """
    base_dir = Path(__file__).parent
    my_small_bib = base_dir / "my_small_bib.xml"
    
    if not my_small_bib.exists():
        print(f"✗ Fehler: {my_small_bib.name} nicht gefunden!")
        print(f"  Erstelle zuerst mit: python phase2_parser.py")
        return
    
    # Benchmark ausführen
    runner = BenchmarkRunner()
    runner.run_full_benchmark(my_small_bib, multipliers=[1, 2, 4, 8, 16])
    
    # Ergebnisse ausgeben
    runner.print_summary()
    runner.plot_results(output_dir=base_dir)


if __name__ == "__main__":
    main()