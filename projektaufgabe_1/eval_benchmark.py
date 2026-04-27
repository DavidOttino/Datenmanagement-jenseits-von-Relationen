import argparse
import csv
import math
import re
from collections import defaultdict
from html import escape
from pathlib import Path


DEFAULT_INPUT = Path(__file__).with_name("benchmark_results.csv")
DEFAULT_OUTPUT = Path(__file__).with_name("benchmark_results.svg")

NUMERIC_FIELDS = {
    "num_tuples": int,
    "num_attributes": int,
    "sparsity": float,
    "duration_seconds": int,
    "storage_h_bytes": int,
    "storage_v_bytes": int,
    "qps_h_oid_lookup": float,
    "qps_h_value_lookup": float,
    "qps_v_oid_lookup": float,
    "qps_v_value_lookup": float,
}

COLORS = ("#1b9e77", "#7570b3", "#d95f02", "#e7298a")
WIDTH = 1400
HEIGHT = 2460
PADDING = 48
PLOT_COUNT = 11


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create an SVG dashboard from benchmark result CSV data."
    )
    parser.add_argument(
        "input_csv",
        nargs="*",
        type=Path,
        default=[DEFAULT_INPUT],
        help=f"CSV file(s) to read. Defaults to {DEFAULT_INPUT}",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"SVG file to write. Defaults to {DEFAULT_OUTPUT}",
    )
    parser.add_argument(
        "--individual-plots",
        nargs="?",
        const=Path(__file__).with_name("benchmark_plots"),
        type=Path,
        metavar="DIR",
        help=(
            "Also write each plot as a separate SVG. "
            "Optionally pass an output directory."
        ),
    )
    return parser.parse_args()


def infer_backend(path: Path, row: dict) -> str:
    backend = row.get("vertical_backend", "").strip()
    if backend:
        return backend

    filename = path.stem.lower()
    if "api" in filename:
        return "api"
    if "proxy" in filename:
        return "proxy"
    return "proxy"


def read_result_file(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Result CSV not found: {path}")

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = []

        for row in reader:
            parsed_row = {
                field: converter(row[field])
                for field, converter in NUMERIC_FIELDS.items()
            }
            parsed_row["vertical_backend"] = infer_backend(path, row)
            rows.append(parsed_row)

    if not rows:
        raise ValueError(f"Result CSV is empty: {path}")

    return rows


def read_results(paths: list[Path]) -> list[dict]:
    rows = []

    for path in paths:
        rows.extend(read_result_file(path))

    if not rows:
        raise ValueError("No benchmark rows found.")

    return rows


def average(values: list[float]) -> float:
    return sum(values) / len(values)


def group_average(rows: list[dict], x_field: str, y_fields: tuple[str, ...]) -> dict:
    grouped = defaultdict(lambda: defaultdict(list))

    for row in rows:
        for y_field in y_fields:
            grouped[row[x_field]][y_field].append(row[y_field])

    return {
        x_value: {
            y_field: average(values)
            for y_field, values in grouped_values.items()
        }
        for x_value, grouped_values in grouped.items()
    }


def bytes_to_mib(value: float) -> float:
    return value / (1024 * 1024)


def format_number(value: float) -> str:
    if abs(value) >= 100:
        return f"{value:.0f}"
    if abs(value) >= 10:
        return f"{value:.1f}"
    return f"{value:.2f}"


def scale(
    value: float,
    source_min: float,
    source_max: float,
    target_min: float,
    target_max: float,
) -> float:
    if source_min == source_max:
        return (target_min + target_max) / 2
    return target_min + ((value - source_min) / (source_max - source_min)) * (
        target_max - target_min
    )


def svg_text(
    x: float,
    y: float,
    text: str,
    size: int = 14,
    anchor: str = "middle",
    weight: str = "400",
    rotate: bool = False,
) -> str:
    transform = f' transform="rotate(-90 {x:.1f} {y:.1f})"' if rotate else ""
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" '
        f'font-family="Arial, sans-serif" font-weight="{weight}" '
        f'text-anchor="{anchor}" fill="#222"{transform}>{escape(text)}</text>'
    )


def line_chart(
    x: float,
    y: float,
    width: float,
    height: float,
    grouped: dict,
    y_fields: tuple[str, ...],
    labels: tuple[str, ...],
    title: str,
    ylabel: str,
    xlabel: str,
    transform=lambda value: value,
    y_scale: str = "linear",
) -> list[str]:
    elements = []
    x_values = sorted(grouped)
    series_values = [
        [
            transform(grouped[x_value][y_field])
            if y_field in grouped[x_value]
            else None
            for x_value in x_values
        ]
        for y_field in y_fields
    ]
    all_y_values = [
        value
        for values in series_values
        for value in values
        if value is not None
    ]

    if not all_y_values:
        raise ValueError(f"Cannot render chart without values: {title}")

    if y_scale == "log":
        positive_values = [value for value in all_y_values if value > 0]
        if not positive_values:
            raise ValueError(f"Cannot render log-scale chart without positive values: {title}")
        y_min = 10 ** math.floor(math.log10(min(positive_values)))
        y_max = 10 ** math.ceil(math.log10(max(positive_values)))
        tick_values = []
        tick = y_min
        while tick <= y_max:
            tick_values.append(tick)
            tick *= 10
    else:
        y_min = min(0, min(all_y_values))
        y_max = max(all_y_values)
        y_max = y_max * 1.08 if y_max else 1
        tick_values = [
            y_min + (y_max - y_min) * tick / 4
            for tick in range(5)
        ]

    def y_position(value: float) -> float:
        if y_scale == "log":
            value = max(value, y_min)
            return scale(
                math.log10(value),
                math.log10(y_min),
                math.log10(y_max),
                plot_bottom,
                plot_top,
            )
        return scale(value, y_min, y_max, plot_bottom, plot_top)

    plot_left = x + 76
    plot_right = x + width - 28
    plot_top = y + 58
    plot_bottom = y + height - 78

    elements.append(
        f'<rect x="{x}" y="{y}" width="{width}" height="{height}" '
        'rx="4" fill="#fff" stroke="#ddd"/>'
    )
    elements.append(svg_text(x + width / 2, y + 30, title, size=18, weight="700"))
    elements.append(svg_text(x + 20, y + height / 2, ylabel, rotate=True))
    elements.append(svg_text(x + width / 2, y + height - 40, xlabel))

    for tick_value in tick_values:
        tick_y = y_position(tick_value)
        elements.append(
            f'<line x1="{plot_left}" y1="{tick_y:.1f}" '
            f'x2="{plot_right}" y2="{tick_y:.1f}" stroke="#e8e8e8"/>'
        )
        elements.append(
            svg_text(
                plot_left - 8,
                tick_y + 5,
                format_number(tick_value),
                size=12,
                anchor="end",
            )
        )

    elements.append(
        f'<line x1="{plot_left}" y1="{plot_bottom}" '
        f'x2="{plot_right}" y2="{plot_bottom}" stroke="#333"/>'
    )
    elements.append(
        f'<line x1="{plot_left}" y1="{plot_top}" '
        f'x2="{plot_left}" y2="{plot_bottom}" stroke="#333"/>'
    )

    for x_value in x_values:
        point_x = scale(x_value, min(x_values), max(x_values), plot_left, plot_right)
        elements.append(
            f'<line x1="{point_x:.1f}" y1="{plot_bottom}" '
            f'x2="{point_x:.1f}" y2="{plot_bottom + 5}" stroke="#333"/>'
        )
        elements.append(svg_text(point_x, plot_bottom + 22, format_number(x_value), size=12))

    for series_index, (values, label) in enumerate(zip(series_values, labels)):
        color = COLORS[series_index % len(COLORS)]
        points = []

        current_segment = []

        for x_value, y_value in zip(x_values, values):
            if y_value is None:
                if len(current_segment) > 1:
                    elements.append(
                        f'<polyline fill="none" stroke="{color}" stroke-width="3" '
                        f'points="{" ".join(current_segment)}"/>'
                    )
                current_segment = []
                continue

            point_x = scale(x_value, min(x_values), max(x_values), plot_left, plot_right)
            point_y = y_position(y_value)
            point = f"{point_x:.1f},{point_y:.1f}"
            points.append(point)
            current_segment.append(point)

        if len(current_segment) > 1:
            elements.append(
                f'<polyline fill="none" stroke="{color}" stroke-width="3" '
                f'points="{" ".join(current_segment)}"/>'
            )

        for point in points:
            point_x, point_y = point.split(",")
            elements.append(
                f'<circle cx="{point_x}" cy="{point_y}" r="4" fill="{color}"/>'
            )

        legend_x = plot_left + series_index * 128
        legend_y = y + height - 18
        elements.append(
            f'<line x1="{legend_x}" y1="{legend_y}" '
            f'x2="{legend_x + 22}" y2="{legend_y}" '
            f'stroke="{color}" stroke-width="3"/>'
        )
        elements.append(svg_text(legend_x + 30, legend_y + 5, label, size=12, anchor="start"))

    return elements


def scatter_chart(
    x: float,
    y: float,
    width: float,
    height: float,
    series: tuple[tuple[str, list[tuple[float, float, str, str]]], ...],
    title: str,
    ylabel: str,
    xlabel: str,
    diagonal_reference: bool = False,
) -> list[str]:
    elements = []
    all_points = [point for _, points in series for point in points]

    if not all_points:
        raise ValueError(f"Cannot render chart without values: {title}")

    x_values = [point[0] for point in all_points]
    y_values = [point[1] for point in all_points]
    x_min = min(0, min(x_values))
    x_max = max(x_values) * 1.08 if max(x_values) else 1
    y_min = min(0, min(y_values))
    y_max = max(y_values) * 1.08 if max(y_values) else 1
    x_ticks = [x_min + (x_max - x_min) * tick / 4 for tick in range(5)]
    y_ticks = [y_min + (y_max - y_min) * tick / 4 for tick in range(5)]

    plot_left = x + 76
    plot_right = x + width - 28
    plot_top = y + 58
    plot_bottom = y + height - 78

    elements.append(
        f'<rect x="{x}" y="{y}" width="{width}" height="{height}" '
        'rx="4" fill="#fff" stroke="#ddd"/>'
    )
    elements.append(svg_text(x + width / 2, y + 30, title, size=18, weight="700"))
    elements.append(svg_text(x + 20, y + height / 2, ylabel, rotate=True))
    elements.append(svg_text(x + width / 2, y + height - 40, xlabel))

    for tick_value in y_ticks:
        tick_y = scale(tick_value, y_min, y_max, plot_bottom, plot_top)
        elements.append(
            f'<line x1="{plot_left}" y1="{tick_y:.1f}" '
            f'x2="{plot_right}" y2="{tick_y:.1f}" stroke="#e8e8e8"/>'
        )
        elements.append(
            svg_text(plot_left - 8, tick_y + 5, format_number(tick_value), size=12, anchor="end")
        )

    for tick_value in x_ticks:
        tick_x = scale(tick_value, x_min, x_max, plot_left, plot_right)
        elements.append(
            f'<line x1="{tick_x:.1f}" y1="{plot_bottom}" '
            f'x2="{tick_x:.1f}" y2="{plot_bottom + 5}" stroke="#333"/>'
        )
        elements.append(svg_text(tick_x, plot_bottom + 22, format_number(tick_value), size=12))

    elements.append(
        f'<line x1="{plot_left}" y1="{plot_bottom}" '
        f'x2="{plot_right}" y2="{plot_bottom}" stroke="#333"/>'
    )
    elements.append(
        f'<line x1="{plot_left}" y1="{plot_top}" '
        f'x2="{plot_left}" y2="{plot_bottom}" stroke="#333"/>'
    )

    if diagonal_reference:
        reference_min = max(x_min, y_min)
        reference_max = min(x_max, y_max)
        if reference_min < reference_max:
            reference_x1 = scale(reference_min, x_min, x_max, plot_left, plot_right)
            reference_y1 = scale(reference_min, y_min, y_max, plot_bottom, plot_top)
            reference_x2 = scale(reference_max, x_min, x_max, plot_left, plot_right)
            reference_y2 = scale(reference_max, y_min, y_max, plot_bottom, plot_top)
            elements.append(
                f'<line x1="{reference_x1:.1f}" y1="{reference_y1:.1f}" '
                f'x2="{reference_x2:.1f}" y2="{reference_y2:.1f}" '
                'stroke="#777" stroke-width="2" stroke-dasharray="6 5">'
                '<title>Reference line: speed ratio = storage ratio</title></line>'
            )

    for series_index, (label, points) in enumerate(series):
        color = COLORS[series_index % len(COLORS)]

        for point_x_value, point_y_value, point_label, marker in points:
            point_x = scale(point_x_value, x_min, x_max, plot_left, plot_right)
            point_y = scale(point_y_value, y_min, y_max, plot_bottom, plot_top)
            if marker == "value":
                points_attr = (
                    f"{point_x:.1f},{point_y - 5:.1f} "
                    f"{point_x - 5:.1f},{point_y + 5:.1f} "
                    f"{point_x + 5:.1f},{point_y + 5:.1f}"
                )
                elements.append(
                    f'<polygon points="{points_attr}" fill="{color}">'
                    f'<title>{escape(point_label)}</title></polygon>'
                )
            else:
                elements.append(
                    f'<circle cx="{point_x:.1f}" cy="{point_y:.1f}" r="4.5" '
                    f'fill="{color}"><title>{escape(point_label)}</title></circle>'
                )

        legend_x = plot_left + series_index * 128
        legend_y = y + height - 18
        elements.append(
            f'<circle cx="{legend_x + 10}" cy="{legend_y}" r="4.5" fill="{color}"/>'
        )
        elements.append(svg_text(legend_x + 24, legend_y + 5, label, size=12, anchor="start"))

    marker_legend_x = plot_right - 150
    marker_legend_y = y + 54
    elements.append(
        f'<circle cx="{marker_legend_x}" cy="{marker_legend_y}" r="4.5" fill="#555"/>'
    )
    elements.append(svg_text(marker_legend_x + 12, marker_legend_y + 5, "oid", size=12, anchor="start"))
    elements.append(
        f'<polygon points="{marker_legend_x:.1f},{marker_legend_y + 20:.1f} '
        f'{marker_legend_x - 5:.1f},{marker_legend_y + 30:.1f} '
        f'{marker_legend_x + 5:.1f},{marker_legend_y + 30:.1f}" fill="#555"/>'
    )
    elements.append(
        svg_text(marker_legend_x + 12, marker_legend_y + 30, "value", size=12, anchor="start")
    )

    return elements


def storage_ratio_chart(
    x: float,
    y: float,
    width: float,
    height: float,
    rows: list[dict],
) -> list[str]:
    grouped = group_average(
        rows,
        "sparsity",
        ("storage_h_bytes", "storage_v_bytes"),
    )
    ratios = {
        x_value: {
            "ratio": grouped[x_value]["storage_v_bytes"]
            / grouped[x_value]["storage_h_bytes"]
        }
        for x_value in sorted(grouped)
    }
    return line_chart(
        x,
        y,
        width,
        height,
        ratios,
        ("ratio",),
        ("Vertical / horizontal",),
        "Vertical / Horizontal Storage Ratio",
        "Ratio",
        "Sparsity",
    )


def storage_speed_tradeoff_chart(
    x: float,
    y: float,
    width: float,
    height: float,
    rows: list[dict],
) -> list[str]:
    api_rows = [row for row in rows if row["vertical_backend"] == "api"]
    if not api_rows:
        raise ValueError("Cannot render API storage/speed tradeoff without API rows.")

    grouped = defaultdict(
        lambda: {
            "storage_ratio": [],
            "oid_speed_ratio": [],
            "value_speed_ratio": [],
        }
    )

    for row in api_rows:
        key = row["sparsity"]
        grouped[key]["storage_ratio"].append(row["storage_v_bytes"] / row["storage_h_bytes"])
        grouped[key]["oid_speed_ratio"].append(
            row["qps_v_oid_lookup"] / row["qps_h_oid_lookup"]
        )
        grouped[key]["value_speed_ratio"].append(
            row["qps_v_value_lookup"] / row["qps_h_value_lookup"]
        )

    series_values = defaultdict(list)

    for sparsity in sorted(grouped):
        values = grouped[sparsity]
        storage_ratio = average(values["storage_ratio"])
        oid_speed_ratio = average(values["oid_speed_ratio"])
        value_speed_ratio = average(values["value_speed_ratio"])
        sparsity_label = f"Sparsity {format_number(sparsity)}"
        series_values[sparsity_label].append(
            (
                storage_ratio,
                oid_speed_ratio,
                f"API oid, sparsity {format_number(sparsity)}",
                "oid",
            )
        )
        series_values[sparsity_label].append(
            (
                storage_ratio,
                value_speed_ratio,
                f"API value, sparsity {format_number(sparsity)}",
                "value",
            )
        )

    return scatter_chart(
        x,
        y,
        width,
        height,
        tuple(series_values.items()),
        "API Storage / Speed Tradeoff",
        "API vertical / horizontal speed",
        "Vertical / horizontal storage",
        diagonal_reference=True,
    )


def qps_by_dimension_chart(
    x: float,
    y: float,
    width: float,
    height: float,
    rows: list[dict],
    x_field: str,
    title: str,
    y_scale: str = "linear",
) -> list[str]:
    grouped = group_average(
        rows,
        x_field,
        (
            "qps_h_oid_lookup",
            "qps_h_value_lookup",
            "qps_v_oid_lookup",
            "qps_v_value_lookup",
        ),
    )
    return line_chart(
        x,
        y,
        width,
        height,
        grouped,
        (
            "qps_h_oid_lookup",
            "qps_h_value_lookup",
            "qps_v_oid_lookup",
            "qps_v_value_lookup",
        ),
        ("H oid", "H value", "V oid", "V value"),
        f"{title} ({y_scale.title()})",
        "Queries per second",
        x_field.replace("_", " ").title(),
        y_scale=y_scale,
    )


def available_backends(rows: list[dict]) -> list[str]:
    preferred_order = {"proxy": 0, "api": 1}
    return sorted(
        {row["vertical_backend"] for row in rows},
        key=lambda backend: (preferred_order.get(backend, 99), backend),
    )


def vertical_backend_qps_by_dimension(
    rows: list[dict],
    x_field: str,
) -> tuple[dict, tuple[str, ...], tuple[str, ...]]:
    grouped_values = defaultdict(lambda: defaultdict(list))

    for row in rows:
        backend = row["vertical_backend"]
        grouped_values[row[x_field]][f"{backend}_v_oid"].append(row["qps_v_oid_lookup"])
        grouped_values[row[x_field]][f"{backend}_v_value"].append(row["qps_v_value_lookup"])

    grouped = {
        x_value: {
            field: average(values)
            for field, values in fields.items()
        }
        for x_value, fields in grouped_values.items()
    }

    fields = []
    labels = []
    for backend in available_backends(rows):
        fields.extend((f"{backend}_v_oid", f"{backend}_v_value"))
        labels.extend((f"{backend} V oid", f"{backend} V value"))

    return grouped, tuple(fields), tuple(labels)


def vertical_backend_comparison_chart(
    x: float,
    y: float,
    width: float,
    height: float,
    rows: list[dict],
    x_field: str,
    title: str,
    y_scale: str = "linear",
) -> list[str]:
    grouped, fields, labels = vertical_backend_qps_by_dimension(rows, x_field)
    return line_chart(
        x,
        y,
        width,
        height,
        grouped,
        fields,
        labels,
        f"{title} ({y_scale.title()})",
        "Vertical queries per second",
        x_field.replace("_", " ").title(),
        y_scale=y_scale,
    )


def available_backends(rows: list[dict]) -> list[str]:
    preferred_order = {"proxy": 0, "api": 1}
    return sorted(
        {row["vertical_backend"] for row in rows},
        key=lambda backend: (preferred_order.get(backend, 99), backend),
    )


def vertical_backend_qps_by_dimension(
    rows: list[dict],
    x_field: str,
) -> tuple[dict, tuple[str, ...], tuple[str, ...]]:
    grouped_values = defaultdict(lambda: defaultdict(list))

    for row in rows:
        backend = row["vertical_backend"]
        grouped_values[row[x_field]][f"{backend}_v_oid"].append(row["qps_v_oid_lookup"])
        grouped_values[row[x_field]][f"{backend}_v_value"].append(row["qps_v_value_lookup"])

    grouped = {
        x_value: {
            field: average(values)
            for field, values in fields.items()
        }
        for x_value, fields in grouped_values.items()
    }

    fields = []
    labels = []
    for backend in available_backends(rows):
        fields.extend((f"{backend}_v_oid", f"{backend}_v_value"))
        labels.extend((f"{backend} V oid", f"{backend} V value"))

    return grouped, tuple(fields), tuple(labels)


def vertical_backend_comparison_chart(
    x: float,
    y: float,
    width: float,
    height: float,
    rows: list[dict],
    x_field: str,
    title: str,
    y_scale: str = "linear",
) -> list[str]:
    grouped, fields, labels = vertical_backend_qps_by_dimension(rows, x_field)
    return line_chart(
        x,
        y,
        width,
        height,
        grouped,
        fields,
        labels,
        f"{title} ({y_scale.title()})",
        "Vertical queries per second",
        x_field.replace("_", " ").title(),
        y_scale=y_scale,
    )


def plot_filename(title: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    return f"{slug}.svg"


def svg_document(width: float, height: float, elements: list[str]) -> str:
    return "\n".join(
        [
            (
                f'<svg xmlns="http://www.w3.org/2000/svg" width="{width:.0f}" '
                f'height="{height:.0f}" viewBox="0 0 {width:.0f} {height:.0f}">'
            ),
            '<rect width="100%" height="100%" fill="#f7f7f5"/>',
            *elements,
            "</svg>",
        ]
    )


def build_plot_elements(
    rows: list[dict],
    positions: list[tuple[float, float]],
    chart_width: float,
    chart_height: float,
) -> list[tuple[str, list[str]]]:
    storage_by_sparsity = group_average(
        rows,
        "sparsity",
        ("storage_h_bytes", "storage_v_bytes"),
    )
    qps_by_sparsity = group_average(
        rows,
        "sparsity",
        (
            "qps_h_oid_lookup",
            "qps_h_value_lookup",
            "qps_v_oid_lookup",
            "qps_v_value_lookup",
        ),
    )

    plot_builders = [
        (
            "Average Storage by Sparsity",
            lambda x, y: line_chart(
                x,
                y,
                chart_width,
                chart_height,
                storage_by_sparsity,
                ("storage_h_bytes", "storage_v_bytes"),
                ("Horizontal", "Vertical"),
                "Average Storage by Sparsity",
                "Storage (MiB)",
                "Sparsity",
                transform=bytes_to_mib,
            ),
        ),
        (
            "Vertical / Horizontal Storage Ratio",
            lambda x, y: storage_ratio_chart(x, y, chart_width, chart_height, rows),
        ),
        (
            "Average Query Throughput by Sparsity (Log)",
            lambda x, y: line_chart(
                x,
                y,
                chart_width,
                chart_height,
                qps_by_sparsity,
                (
                    "qps_h_oid_lookup",
                    "qps_h_value_lookup",
                    "qps_v_oid_lookup",
                    "qps_v_value_lookup",
                ),
                ("H oid", "H value", "V oid", "V value"),
                "Average Query Throughput by Sparsity (Log)",
                "Queries per second",
                "Sparsity",
                y_scale="log",
            ),
        ),
        (
            "Average Query Throughput by Tuple Count (Log)",
            lambda x, y: qps_by_dimension_chart(
                x,
                y,
                chart_width,
                chart_height,
                rows,
                "num_tuples",
                "Average Query Throughput by Tuple Count",
                y_scale="log",
            ),
        ),
        (
            "Average Query Throughput by Sparsity (Linear)",
            lambda x, y: line_chart(
                x,
                y,
                chart_width,
                chart_height,
                qps_by_sparsity,
                (
                    "qps_h_oid_lookup",
                    "qps_h_value_lookup",
                    "qps_v_oid_lookup",
                    "qps_v_value_lookup",
                ),
                ("H oid", "H value", "V oid", "V value"),
                "Average Query Throughput by Sparsity (Linear)",
                "Queries per second",
                "Sparsity",
            ),
        ),
        (
            "Average Query Throughput by Tuple Count (Linear)",
            lambda x, y: qps_by_dimension_chart(
                x,
                y,
                chart_width,
                chart_height,
                rows,
                "num_tuples",
                "Average Query Throughput by Tuple Count",
                y_scale="linear",
            ),
        ),
        (
            "Vertical Backend Throughput by Sparsity (Log)",
            lambda x, y: vertical_backend_comparison_chart(
                x,
                y,
                chart_width,
                chart_height,
                rows,
                "sparsity",
                "Vertical Backend Throughput by Sparsity",
                y_scale="log",
            ),
        ),
        (
            "Vertical Backend Throughput by Tuple Count (Log)",
            lambda x, y: vertical_backend_comparison_chart(
                x,
                y,
                chart_width,
                chart_height,
                rows,
                "num_tuples",
                "Vertical Backend Throughput by Tuple Count",
                y_scale="log",
            ),
        ),
        (
            "Vertical Backend Throughput by Sparsity (Linear)",
            lambda x, y: vertical_backend_comparison_chart(
                x,
                y,
                chart_width,
                chart_height,
                rows,
                "sparsity",
                "Vertical Backend Throughput by Sparsity",
                y_scale="linear",
            ),
        ),
        (
            "Vertical Backend Throughput by Tuple Count (Linear)",
            lambda x, y: vertical_backend_comparison_chart(
                x,
                y,
                chart_width,
                chart_height,
                rows,
                "num_tuples",
                "Vertical Backend Throughput by Tuple Count",
                y_scale="linear",
            ),
        ),
        (
            "API Storage / Speed Tradeoff",
            lambda x, y: storage_speed_tradeoff_chart(x, y, chart_width, chart_height, rows),
        ),
    ]

    if len(positions) != len(plot_builders):
        raise ValueError("Expected one position for each benchmark plot.")

    return [
        (title, build_plot(*position))
        for (title, build_plot), position in zip(plot_builders, positions)
    ]


def create_dashboard(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    row_count = math.ceil(PLOT_COUNT / 2)
    chart_width = (WIDTH - PADDING * 3) / 2
    chart_height = (HEIGHT - PADDING * (row_count + 1) - 36) / row_count
    first_row = PADDING + 36
    left = PADDING
    right = PADDING * 2 + chart_width
    positions = []

    for plot_index in range(PLOT_COUNT):
        row = plot_index // 2
        column = plot_index % 2
        positions.append(
            (
                left if column == 0 else right,
                first_row + row * (chart_height + PADDING),
            )
        )

    elements = [
        svg_text(WIDTH / 2, 38, "Benchmark Results", size=28, weight="700"),
    ]

    for _, plot_elements in build_plot_elements(rows, positions, chart_width, chart_height):
        elements.extend(plot_elements)

    output_path.write_text(svg_document(WIDTH, HEIGHT, elements), encoding="utf-8")


def create_individual_plots(rows: list[dict], output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    chart_width = 760
    chart_height = 420
    plots = build_plot_elements(rows, [(0, 0)] * PLOT_COUNT, chart_width, chart_height)
    paths = []

    for title, elements in plots:
        path = output_dir / plot_filename(title)
        path.write_text(svg_document(chart_width, chart_height, elements), encoding="utf-8")
        paths.append(path)

    return paths


def main() -> None:
    args = parse_args()
    rows = read_results(args.input_csv)
    create_dashboard(rows, args.output)
    print(f"Wrote benchmark chart to {args.output}")
    if args.individual_plots is not None:
        paths = create_individual_plots(rows, args.individual_plots)
        print(f"Wrote {len(paths)} individual benchmark plots to {args.individual_plots}")


if __name__ == "__main__":
    main()
