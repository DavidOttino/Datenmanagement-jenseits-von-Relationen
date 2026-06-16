from __future__ import annotations

from dataclasses import dataclass, field
from html.entities import name2codepoint
from pathlib import Path
import re
import xml.etree.ElementTree as ET


PUBLICATION_TYPES = {
    "article",
    "inproceedings",
    "proceedings",
    "book",
    "incollection",
    "phdthesis",
    "mastersthesis",
    "www",
}


@dataclass
class EdgeNode:
    id: int
    s_id: str | None
    type: str
    content: str | None = None
    children: list["EdgeNode"] = field(default_factory=list)

    def walk(self) -> list["EdgeNode"]:
        nodes = [self]
        for child in self.children:
            nodes.extend(child.walk())
        return nodes

    def edges(self) -> list[tuple[int, int]]:
        result: list[tuple[int, int]] = []
        for child in self.children:
            result.append((self.id, child.id))
            result.extend(child.edges())
        return result


class EdgeModelBuilder:
    def __init__(self) -> None:
        self._next_id = 0

    def from_file(self, path: str | Path) -> EdgeNode:
        xml = Path(path).read_text(encoding="utf-8")
        root = ET.fromstring(_resolve_named_entities(xml))
        return self.from_xml_root(root)

    def from_xml_root(self, xml_root: ET.Element) -> EdgeNode:
        edge_root = self._node("bib", "bib")
        venue_nodes: dict[str, EdgeNode] = {}
        year_nodes: dict[tuple[str, str], EdgeNode] = {}

        for publication in list(xml_root):
            publication_type = _strip_namespace(publication.tag)
            if publication_type not in PUBLICATION_TYPES:
                continue

            key = publication.attrib.get("key", "")
            venue = _venue_from_key(key)
            if venue is None:
                continue

            year = _child_text(publication, "year")
            if year is None:
                continue

            venue_node = venue_nodes.get(venue)
            if venue_node is None:
                venue_node = self._node(venue, "venue")
                venue_nodes[venue] = venue_node
                edge_root.children.append(venue_node)

            year_key = (venue, year)
            year_node = year_nodes.get(year_key)
            if year_node is None:
                year_node = self._node(f"{venue}_{year}", "year")
                year_nodes[year_key] = year_node
                venue_node.children.append(year_node)

            publication_node = self._publication_node(publication, publication_type, key)
            year_node.children.append(publication_node)

        self._renumber(edge_root)
        return edge_root

    def _publication_node(
        self,
        publication: ET.Element,
        publication_type: str,
        key: str,
    ) -> EdgeNode:
        publication_node = self._node(_publication_s_id(key), publication_type)
        for child in list(publication):
            child_type = _strip_namespace(child.tag)
            content = (child.text or "").strip() or None
            publication_node.children.append(self._node(None, child_type, content))
        return publication_node

    def _node(self, s_id: str | None, type: str, content: str | None = None) -> EdgeNode:
        node = EdgeNode(id=self._next_id, s_id=s_id, type=type, content=content)
        self._next_id += 1
        return node

    def _renumber(self, root: EdgeNode) -> None:
        self._next_id = 0

        def visit(node: EdgeNode) -> None:
            node.id = self._next_id
            self._next_id += 1
            for child in node.children:
                visit(child)

        visit(root)


def print_edge_tree(root: EdgeNode) -> None:
    def print_node(node: EdgeNode, depth: int) -> None:
        indent = "  " * depth
        s_id = node.s_id if node.s_id is not None else "NULL"
        content = node.content if node.content is not None else "NULL"
        print(f"{indent}{node.id}: s_id={s_id}, type={node.type}, content={content}")
        for child in node.children:
            print_node(child, depth + 1)

    print_node(root, 0)


def _publication_s_id(key: str) -> str:
    return key.rsplit("/", 1)[-1] if key else ""


def _venue_from_key(key: str) -> str | None:
    if key.startswith(("journals/pvldb/", "conf/vldb/")):
        return "vldb"
    if key.startswith(("journals/pacmmod/", "conf/sigmod/")):
        return "sigmod"
    if key.startswith("conf/icde/"):
        return "icde"
    return None


def _child_text(element: ET.Element, tag: str) -> str | None:
    for child in list(element):
        if _strip_namespace(child.tag) == tag:
            text = (child.text or "").strip()
            return text or None
    return None


def _strip_namespace(tag: str) -> str:
    if "}" not in tag:
        return tag
    return tag.rsplit("}", 1)[1]


def _resolve_named_entities(xml: str) -> str:
    xml_builtin_entities = {"amp", "lt", "gt", "apos", "quot"}

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name in xml_builtin_entities:
            return match.group(0)
        codepoint = name2codepoint.get(name)
        if codepoint is None:
            return match.group(0)
        return chr(codepoint)

    return re.sub(r"&([A-Za-z][A-Za-z0-9]+);", replace, xml)
