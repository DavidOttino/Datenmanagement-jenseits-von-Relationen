from __future__ import annotations

from pathlib import Path
import unittest

from projektaufgabe_3.edge_model import EdgeModelBuilder


class EdgeModelBuilderTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = EdgeModelBuilder().from_file(Path(__file__).with_name("toy_example.txt"))
        cls.nodes = cls.root.walk()
        cls.by_s_id = {node.s_id: node for node in cls.nodes if node.s_id is not None}

    def test_toy_example_is_grouped_by_venue_then_year(self) -> None:
        self.assertEqual(self.root.s_id, "bib")
        self.assertEqual([child.s_id for child in self.root.children], ["vldb", "sigmod"])
        self.assertEqual([child.s_id for child in self.by_s_id["vldb"].children], ["vldb_2023"])
        self.assertEqual(
            [child.s_id for child in self.by_s_id["sigmod"].children],
            ["sigmod_2022", "sigmod_2023"],
        )

    def test_publications_are_under_their_transformed_year_nodes(self) -> None:
        self.assertEqual(
            [child.s_id for child in self.by_s_id["vldb_2023"].children],
            ["SchmittKAMM23", "SchalerHS23"],
        )
        self.assertEqual(
            [child.s_id for child in self.by_s_id["sigmod_2022"].children],
            ["HutterAK0L22"],
        )
        self.assertEqual(
            [child.s_id for child in self.by_s_id["sigmod_2023"].children],
            ["ThielKAHMS23"],
        )

    def test_ignored_attributes_do_not_create_nodes(self) -> None:
        node_types = [node.type for node in self.nodes]

        self.assertNotIn("mdate", node_types)
        self.assertNotIn("orcid", node_types)

    def test_edge_model_rows_are_generated(self) -> None:
        self.assertEqual(len(self.root.edges()), len(self.nodes) - 1)
        self.assertEqual(self.root.edges()[0], (0, 1))


if __name__ == "__main__":
    unittest.main()
