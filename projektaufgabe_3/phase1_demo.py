from __future__ import annotations

from pathlib import Path

try:
    from .edge_model import EdgeModelBuilder, print_edge_tree
except ImportError:
    from edge_model import EdgeModelBuilder, print_edge_tree


def main() -> None:
    source = Path(__file__).with_name("toy_example.txt")
    root = EdgeModelBuilder().from_file(source)

    print("Phase 1 EDGE model parser demo")
    print("==============================")
    print_edge_tree(root)


if __name__ == "__main__":
    main()
