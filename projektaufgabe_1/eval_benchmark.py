import argparse
import csv
import math
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
HEIGHT = 1260
PADDING = 48


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create an SVG dashboard from benchmark result CSV data."
    )
    parser.add_argument(
        "input_csv",
        nargs="?",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"CSV file to read. Defaults to {DEFAULT_INPUT}",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"SVG file to write. Defaults to {DEFAULT_OUTPUT}",
    )
    return parser.parse_args()


def read_results(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Result CSV not found: {path}")

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = []

        for row in reader:
            rows.append(
                {
                    field: converter(row[field])
                    for field, converter in NUMERIC_FIELDS.items()
                }
            )

    if not rows:
        raise ValueError(f"Result CSV is empty: {path}")

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
        [transform(grouped[x_value][y_field]) for x_value in x_values]
        for y_field in y_fields
    ]
    all_y_values = [value for values in series_values for value in values]

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

        for x_value, y_value in zip(x_values, values):
            point_x = scale(x_value, min(x_values), max(x_values), plot_left, plot_right)
            point_y = y_position(y_value)
            points.append(f"{point_x:.1f},{point_y:.1f}")

        elements.append(
            f'<polyline fill="none" stroke="{color}" stroke-width="3" '
            f'points="{" ".join(points)}"/>'
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


def create_dashboard(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    chart_width = (WIDTH - PADDING * 3) / 2
    chart_height = (HEIGHT - PADDING * 4 - 36) / 3
    row_1 = PADDING + 36
    row_2 = row_1 + chart_height + PADDING
    row_3 = row_2 + chart_height + PADDING
    left = PADDING
    right = PADDING * 2 + chart_width

    elements = [
        (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" '
            f'height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">'
        ),
        '<rect width="100%" height="100%" fill="#f7f7f5"/>',
        svg_text(WIDTH / 2, 38, "Benchmark Results", size=28, weight="700"),
    ]

    storage_by_sparsity = group_average(
        rows,
        "sparsity",
        ("storage_h_bytes", "storage_v_bytes"),
    )
    elements.extend(
        line_chart(
            left,
            row_1,
            chart_width,
            chart_height,
            storage_by_sparsity,
            ("storage_h_bytes", "storage_v_bytes"),
            ("Horizontal", "Vertical"),
            "Average Storage by Sparsity",
            "Storage (MiB)",
            "Sparsity",
            transform=bytes_to_mib,
        )
    )

    elements.extend(storage_ratio_chart(right, row_1, chart_width, chart_height, rows))

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
    elements.extend(
        line_chart(
            left,
            row_2,
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
        )
    )

    elements.extend(
        qps_by_dimension_chart(
            right,
            row_2,
            chart_width,
            chart_height,
            rows,
            "num_tuples",
            "Average Query Throughput by Tuple Count",
            y_scale="log",
        )
    )

    elements.extend(
        line_chart(
            left,
            row_3,
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
        )
    )

    elements.extend(
        qps_by_dimension_chart(
            right,
            row_3,
            chart_width,
            chart_height,
            rows,
            "num_tuples",
            "Average Query Throughput by Tuple Count",
            y_scale="linear",
        )
    )

    elements.append("</svg>")
    output_path.write_text("\n".join(elements), encoding="utf-8")


def main() -> None:
    args = parse_args()
    rows = read_results(args.input_csv)
    create_dashboard(rows, args.output)
    print(f"Wrote benchmark chart to {args.output}")


if __name__ == "__main__":
    main()
