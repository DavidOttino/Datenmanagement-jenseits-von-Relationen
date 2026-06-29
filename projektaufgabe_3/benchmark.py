from __future__ import annotations

import copy
import io
import math
import random
import statistics
import xml.etree.ElementTree as ET
from contextlib import redirect_stdout
from pathlib import Path
from time import perf_counter_ns
from typing import Any, Callable

from connection import get_connection
from db_storage import create_phase1_tables, reset_phase1_tables, save_edge_model
from edge_axes import ancestor, descendant, following_sibling, preceding_sibling
from edge_model import EdgeModelBuilder, EdgeNode, PUBLICATION_TYPES, _resolve_named_entities
from phase2_accelerator import create_xpath_accelerator_tables, annotate_tree, save_accelerator_data
from phase2_accelerator_axes import (
	ancestor_accel,
	descendant_accel,
	descendant_accel_opti,
	following_sibling_accel,
	preceding_sibling_accel,
)
from phase3_accelerator_single_axis import (
	annotate_tree_single_axis,
	create_single_axis_accelerator_tables,
	save_single_axis_accelerator_data,
)
from phase3_single_axis import descendant_single_axis


DEFAULT_FACTORS = (1, 1)
DEFAULT_REPEATS = 5
DEFAULT_SEED = 26


def main() -> None:
	dataset_factors = normalize_factors(list(DEFAULT_FACTORS))
	base_path = Path(__file__).with_name("my_small_bib.xml")
	rng = random.Random(DEFAULT_SEED)

	if not base_path.exists():
		raise FileNotFoundError(f"Datensatz nicht gefunden: {base_path}")

	dataset_files = ensure_scaled_dataset_files(base_path, dataset_factors)
	approaches = build_approaches()
	results: list[dict[str, Any]] = []

	with get_connection() as conn:
		for factor in dataset_factors:
			dataset_name = f"my_small_bib_{factor}x"
			edge_root = EdgeModelBuilder().from_file(dataset_files[factor])
			context = select_context_nodes(edge_root)

			print()
			print(f"[DATASET] {dataset_name}: {len(edge_root.walk())} Knoten im EDGE-Baum")
			print("[DATASET] sibling axes: following-sibling, preceding-sibling")

			for approach in approaches:
				print(f"[SETUP] {approach['name']} auf {dataset_name}")
				run_quiet(approach["setup"], conn, edge_root)

				if approach["ancestor"] is not None:
					results.append(
						benchmark_query(
							dataset_name,
							approach["name"],
							"ancestor",
							lambda: approach["ancestor"](conn, context["ancestor_node_id"]),
							DEFAULT_REPEATS,
						)
					)

				if approach["descendant"] is not None:
					results.append(
						benchmark_query(
							dataset_name,
							approach["name"],
							"descendant",
							lambda: approach["descendant"](conn, context["descendant_node_id"]),
							DEFAULT_REPEATS,
						)
					)

				if approach["sibling"] is not None:
					for axis_name in ("following-sibling", "preceding-sibling"):
						context_key = axis_name.replace("-", "_") + "_node_id"
						results.append(
							benchmark_query(
								dataset_name,
								approach["name"],
								axis_name,
								lambda axis_name=axis_name, context_key=context_key: approach["sibling"](
									conn,
									context[context_key],
									axis_name,
								),
								DEFAULT_REPEATS,
							)
						)

	print_results(results)
	write_charts(results, Path(__file__).with_name("benchmark_charts"))


def normalize_factors(factors: list[int]) -> list[int]:
	normalized: list[int] = []
	for factor in factors:
		if factor < 1 or (factor & (factor - 1)) != 0:
			raise ValueError(f"Ungültiger Skalierungsfaktor: {factor}. Erlaubt sind 1, 2, 4, 8, ...")
		if factor not in normalized:
			normalized.append(factor)
	return normalized


def ensure_scaled_dataset_files(base_path: Path, factors: list[int]) -> dict[int, Path]:
	cache_dir = base_path.with_name("benchmark_datasets")
	cache_dir.mkdir(exist_ok=True)

	dataset_files: dict[int, Path] = {}
	for factor in factors:
		target = cache_dir / f"my_small_bib_{factor}x.xml"
		needs_refresh = not target.exists() or target.stat().st_mtime < base_path.stat().st_mtime

		if needs_refresh:
			write_scaled_dataset_file(base_path, target, factor)
			print(f"[CACHE] erzeugt: {target.name}")
		else:
			print(f"[CACHE] wiederverwendet: {target.name}")

		dataset_files[factor] = target

	return dataset_files


def write_scaled_dataset_file(base_path: Path, target_path: Path, factor: int) -> None:
	xml = base_path.read_text(encoding="utf-8")
	base_root = ET.fromstring(_resolve_named_entities(xml))
	base_children = [copy.deepcopy(child) for child in list(base_root)]

	if not base_children:
		raise ValueError("my_small_bib.xml enthält keine Publikationen.")

	scaled_root = ET.Element(base_root.tag, attrib=dict(base_root.attrib))
	for _ in range(factor):
		for child in base_children:
			scaled_root.append(copy.deepcopy(child))

	ET.ElementTree(scaled_root).write(target_path, encoding="utf-8", xml_declaration=True)


def select_context_nodes(root: EdgeNode) -> dict[str, int]:
	year_nodes: list[EdgeNode] = []
	article_nodes: list[EdgeNode] = []
	publication_nodes: list[EdgeNode] = []
	parent_by_id: dict[int, EdgeNode] = {}

	def visit(node: EdgeNode) -> None:
		if node.type == "year":
			year_nodes.append(node)
		if node.type == "article":
			article_nodes.append(node)
		if node.type in PUBLICATION_TYPES:
			publication_nodes.append(node)
		for child in node.children:
			parent_by_id[child.id] = node
			visit(child)

	visit(root)

	if not year_nodes:
		raise ValueError("Kein year-Knoten im Datensatz gefunden.")

	ancestor_node = article_nodes[0] if article_nodes else publication_nodes[0]
	descendant_node = year_nodes[0]

	sibling_candidates = article_nodes if article_nodes else publication_nodes
	following_sibling_node = pick_sibling_candidate(
		sibling_candidates,
		parent_by_id,
		"following-sibling",
	)
	preceding_sibling_node = pick_sibling_candidate(
		sibling_candidates,
		parent_by_id,
		"preceding-sibling",
	)

	return {
		"ancestor_node_id": ancestor_node.id,
		"descendant_node_id": descendant_node.id,
		"following_sibling_node_id": following_sibling_node.id,
		"preceding_sibling_node_id": preceding_sibling_node.id,
	}


def pick_sibling_candidate(
	candidates: list[EdgeNode],
	parent_by_id: dict[int, EdgeNode],
	axis_name: str,
) -> EdgeNode:
	for candidate in candidates:
		parent = parent_by_id.get(candidate.id)
		if parent is None:
			continue
		siblings = parent.children
		position = siblings.index(candidate)
		if axis_name == "following-sibling" and position < len(siblings) - 1:
			return candidate
		if axis_name == "preceding-sibling" and position > 0:
			return candidate
	raise ValueError(f"Kein geeigneter Knoten für die Achse {axis_name} gefunden.")


def build_approaches() -> list[dict[str, Any]]:
	return [
		{
			"name": "phase1_edge",
			"setup": setup_phase1_edge,
			"ancestor": ancestor,
			"descendant": descendant,
			"sibling": query_phase1_sibling,
		},
		{
			"name": "phase2_accel",
			"setup": setup_phase2_accel,
			"ancestor": ancestor_accel,
			"descendant": descendant_accel,
			"sibling": query_phase2_sibling,
		},
		{
			"name": "phase3_window",
			"setup": setup_phase3_window,
			"ancestor": ancestor_accel,
			"descendant": descendant_accel_opti,
			"sibling": query_phase2_sibling,
		},
		{
			"name": "phase3_single_axis",
			"setup": setup_phase3_single_axis,
			"ancestor": None,
			"descendant": descendant_single_axis,
			"sibling": None,
		},
	]


def benchmark_query(
	dataset: str,
	approach: str,
	axis: str,
	query: Callable[[], list[tuple[int, str | None, str, str | None]]],
	repeats: int,
) -> dict[str, Any]:
	if repeats < 1:
		raise ValueError("repeats muss >= 1 sein.")

	durations_ms: list[float] = []
	result_size = -1
	for _ in range(repeats):
		start_ns = perf_counter_ns()
		rows = query()
		end_ns = perf_counter_ns()
		result_size = len(rows)
		durations_ms.append((end_ns - start_ns) / 1_000_000)

	row = {
		"dataset": dataset,
		"approach": approach,
		"axis": axis,
		"repeats": repeats,
		"result_size": result_size,
		"avg_ms": statistics.fmean(durations_ms),
		"min_ms": min(durations_ms),
		"max_ms": max(durations_ms),
		"stdev_ms": statistics.stdev(durations_ms) if len(durations_ms) > 1 else 0.0,
	}
	print(
		f"[BENCH] {dataset:<15} {approach:<18} {axis:<18} avg={row['avg_ms']:>8.3f} ms rows={row['result_size']}"
	)
	return row


def run_quiet(func: Callable[..., None], *args) -> None:
	with redirect_stdout(io.StringIO()):
		func(*args)


def setup_phase1_edge(conn, root: EdgeNode) -> None:
	reset_phase1_tables(conn)
	create_phase1_tables(conn)
	save_edge_model(conn, root)
	with conn.cursor() as cur:
		cur.execute("CLUSTER edge USING idx_edge_from;")
		cur.execute("ANALYZE node;")
		cur.execute("ANALYZE edge;")
	conn.commit()


def setup_phase2_accel(conn, root: EdgeNode) -> None:
	setup_phase1_edge(conn, root)
	accel_rows, content_rows, attribute_rows = quiet_annotate_tree(root)
	create_xpath_accelerator_tables(conn)
	save_accelerator_data(conn, accel_rows, content_rows, attribute_rows)
	apply_accel_indexes(conn, include_height_range=False)


def setup_phase3_window(conn, root: EdgeNode) -> None:
	setup_phase1_edge(conn, root)
	accel_rows, content_rows, attribute_rows = quiet_annotate_tree(root)
	create_xpath_accelerator_tables(conn)
	save_accelerator_data(conn, accel_rows, content_rows, attribute_rows)
	apply_accel_indexes(conn, include_height_range=True)


def setup_phase3_single_axis(conn, root: EdgeNode) -> None:
	setup_phase1_edge(conn, root)
	annotations, content_rows, attribute_rows = build_single_axis_payload(root)
	create_single_axis_accelerator_tables(conn)
	save_single_axis_accelerator_data(conn, annotations, content_rows, attribute_rows)
	with conn.cursor() as cur:
		cur.execute("CREATE INDEX IF NOT EXISTS idx_accel_single_node_id ON accel_single(node_id);")
		cur.execute("CLUSTER accel_single USING accel_single_pkey;")
		cur.execute("ANALYZE accel_single;")
		cur.execute("ANALYZE content;")
		cur.execute("ANALYZE attribute;")
	conn.commit()


def quiet_annotate_tree(root: EdgeNode) -> tuple[list[tuple], list[tuple], list[tuple]]:
	with redirect_stdout(io.StringIO()):
		return annotate_tree(root)


def build_single_axis_payload(
	root: EdgeNode,
) -> tuple[dict[int, object], list[tuple[int, str, str | None]], list[tuple[int, str, str]]]:
	with redirect_stdout(io.StringIO()):
		annotations = annotate_tree_single_axis(root)

	content_rows: list[tuple[int, str, str | None]] = []
	attribute_rows: list[tuple[int, str, str]] = []
	for node in root.walk():
		content_rows.append((node.id, node.type, node.content))
		if node.s_id is not None:
			attribute_rows.append((node.id, "s_id", node.s_id))
	return annotations, content_rows, attribute_rows


def apply_accel_indexes(conn, *, include_height_range: bool) -> None:
	with conn.cursor() as cur:
		cur.execute("CREATE INDEX IF NOT EXISTS idx_accel_pre ON accel(pre);")
		cur.execute("CLUSTER accel USING accel_pkey;")
		cur.execute(
			"CREATE INDEX IF NOT EXISTS idx_accel_point_gist ON accel USING GIST (point(pre, post));"
		)
		if include_height_range:
			cur.execute(
				"CREATE INDEX IF NOT EXISTS idx_accel_height_point_gist ON accel USING GIST (point(pre, post + height));"
			)
		cur.execute("ANALYZE accel;")
		cur.execute("ANALYZE content;")
		cur.execute("ANALYZE attribute;")
	conn.commit()


def print_results(rows: list[dict[str, Any]]) -> None:
	print()
	print("Benchmark-Ergebnisse")
	print("=" * 116)
	print(
		f"{'Dataset':<16} {'Ansatz':<20} {'Achse':<20} {'Rows':>8} {'Avg ms':>10} {'Min ms':>10} {'Max ms':>10} {'StdDev':>10}"
	)
	print("-" * 116)
	for row in rows:
		print(
			f"{row['dataset']:<16} {row['approach']:<20} {row['axis']:<20} {row['result_size']:>8} {row['avg_ms']:>10.3f} {row['min_ms']:>10.3f} {row['max_ms']:>10.3f} {row['stdev_ms']:>10.3f}"
		)


def write_charts(rows: list[dict[str, Any]], output_dir: Path) -> None:
	if not rows:
		return

	try:
		import matplotlib.pyplot as plt
	except ImportError:
		print("[CHART] matplotlib nicht installiert. Installiere mit: pip install matplotlib")
		return

	output_dir.mkdir(exist_ok=True)
	axes = sorted({str(row["axis"]) for row in rows})
	approaches = [
		"phase1_edge",
		"phase2_accel",
		"phase3_window",
		"phase3_single_axis",
	]
	palette = {
		"phase1_edge": "#1f77b4",
		"phase2_accel": "#2ca02c",
		"phase3_window": "#ff7f0e",
		"phase3_single_axis": "#d62728",
	}

	for axis_name in axes:
		axis_rows = [row for row in rows if str(row["axis"]) == axis_name]
		datasets = sorted({str(row["dataset"]) for row in axis_rows}, key=_dataset_sort_key)
		if not datasets:
			continue

		# Eindeutiges Lookup: verhindert falsche 0.0-Werte durch Fallback-Logik.
		row_map = {
			(str(row["dataset"]), str(row["approach"])): float(row["avg_ms"])
			for row in axis_rows
		}

		file_name = f"benchmark_{sanitize_filename(axis_name)}.png"
		file_path = output_dir / file_name

		x_positions = list(range(len(datasets)))
		fig, ax = plt.subplots(figsize=(max(8, len(datasets) * 2.2), 5.2))
		bar_width = 0.18

		for idx, approach in enumerate(approaches):
			offset = (idx - (len(approaches) - 1) / 2) * bar_width
			values = [row_map.get((dataset, approach), float("nan")) for dataset in datasets]

			bar_positions = [x + offset for x in x_positions]
			bars = ax.bar(
				bar_positions,
				values,
				width=bar_width,
				label=approach,
				color=palette.get(approach, "#777777"),
			)

			for rect, value in zip(bars, values):
				if math.isnan(value):
					continue
				ax.text(
					rect.get_x() + rect.get_width() / 2,
					rect.get_height(),
					f"{value:.3f}",
					ha="center",
					va="bottom",
					fontsize=8,
				)

		ax.set_title(f"Benchmark {axis_name} (Avg ms)")
		ax.set_ylabel("Zeit in ms")
		ax.set_xticks(x_positions)
		ax.set_xticklabels(datasets, rotation=20, ha="right")
		ax.grid(axis="y", alpha=0.25)
		ax.legend(fontsize=9)
		fig.tight_layout()
		fig.savefig(file_path, dpi=150)
		plt.close(fig)
		print(f"[CHART] erzeugt: {file_path.name}")


def sanitize_filename(name: str) -> str:
	allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
	cleaned = [ch if ch in allowed else "_" for ch in name]
	return "".join(cleaned).strip("_") or "chart"


def _dataset_sort_key(name: str) -> int:
	if "_" in name and name.endswith("x"):
		factor = name.rsplit("_", 1)[-1][:-1]
		if factor.isdigit():
			return int(factor)
	return 10**9


def query_phase1_sibling_for_axis(
	conn,
	node_id: int,
	axis_name: str,
) -> list[tuple[int, str | None, str, str | None]]:
	if axis_name == "following-sibling":
		return following_sibling(conn, node_id)
	return preceding_sibling(conn, node_id)


def query_phase2_sibling_for_axis(
	conn,
	node_id: int,
	axis_name: str,
) -> list[tuple[int, str | None, str, str | None]]:
	if axis_name == "following-sibling":
		return following_sibling_accel(conn, node_id)
	return preceding_sibling_accel(conn, node_id)


def query_phase1_sibling(
	conn,
	node_id: int,
	axis_name: str,
) -> list[tuple[int, str | None, str, str | None]]:
	return query_phase1_sibling_for_axis(conn, node_id, axis_name)


def query_phase2_sibling(
	conn,
	node_id: int,
	axis_name: str,
) -> list[tuple[int, str | None, str, str | None]]:
	return query_phase2_sibling_for_axis(conn, node_id, axis_name)


if __name__ == "__main__":
	main()
