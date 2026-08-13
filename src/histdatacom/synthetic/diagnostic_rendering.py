"""Deterministic optional static rendering for reconstruction diagnostics."""

from __future__ import annotations

import hashlib
import re
from io import BytesIO
from pathlib import Path
from textwrap import fill
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from matplotlib.axes import Axes

from histdatacom.synthetic.diagnostics import (
    DIAGNOSTIC_RENDERER_CONTRACT_VERSION,
    DIAGNOSTIC_RENDERER_NAME,
    DiagnosticChartBundleV1,
    DiagnosticChartDataV1,
    DiagnosticMark,
    DiagnosticRenderedArtifactV1,
    DiagnosticRendererConfigV1,
    DiagnosticStatus,
    _write_once,
)

_COLORS = (
    "#176B87",
    "#DA7B24",
    "#4C956C",
    "#8E5EA2",
    "#C44E52",
    "#4C72B0",
    "#8172B2",
    "#CCB974",
    "#64B5CD",
    "#6C6C6C",
)


def render_diagnostic_bundle(
    bundle: DiagnosticChartBundleV1,
    config: DiagnosticRendererConfigV1,
    *,
    output_directory: str | Path,
) -> tuple[DiagnosticRenderedArtifactV1, ...]:
    """Render every chart and format under one deterministic configuration."""
    try:
        import matplotlib

        matplotlib.use("Agg", force=True)
        from matplotlib import pyplot as plt
    except (
        ModuleNotFoundError
    ) as err:  # pragma: no cover - environment-specific
        raise ModuleNotFoundError(
            "static reconstruction diagnostics require histdatacom[viz]"
        ) from err

    if not isinstance(bundle, DiagnosticChartBundleV1):
        raise TypeError("diagnostic renderer requires a v1 bundle")
    if not isinstance(config, DiagnosticRendererConfigV1):
        raise TypeError("diagnostic renderer requires a v1 configuration")
    root = Path(output_directory).expanduser().resolve()
    render_root = root / "rendered"
    render_root.mkdir(parents=True, exist_ok=True)
    receipts: list[DiagnosticRenderedArtifactV1] = []
    rc = {
        "axes.grid": True,
        "axes.spines.right": False,
        "axes.spines.top": False,
        "figure.facecolor": "white",
        "font.family": config.font_family,
        "font.size": 9.0,
        "savefig.facecolor": "white",
        "svg.hashsalt": config.hash_salt,
    }
    with matplotlib.rc_context(rc=rc):
        for chart in bundle.charts:
            for output_format in config.formats:
                figure, axis = plt.subplots(
                    figsize=(
                        config.width_px / config.dpi,
                        config.height_px / config.dpi,
                    ),
                    dpi=config.dpi,
                )
                try:
                    figure.subplots_adjust(
                        left=0.155,
                        right=0.985,
                        top=0.94,
                        bottom=0.44,
                    )
                    _draw_chart(axis, chart)
                    payload = BytesIO()
                    metadata = (
                        {
                            "Date": None,
                            "Creator": (
                                f"histdatacom/{DIAGNOSTIC_RENDERER_CONTRACT_VERSION}"
                            ),
                        }
                        if output_format.value == "svg"
                        else {
                            "Software": (
                                f"histdatacom/{DIAGNOSTIC_RENDERER_CONTRACT_VERSION}"
                            )
                        }
                    )
                    figure.savefig(
                        payload,
                        format=output_format.value,
                        dpi=config.dpi,
                        metadata=metadata,
                    )
                finally:
                    plt.close(figure)
                content = payload.getvalue()
                digest = hashlib.sha256(content).hexdigest()
                filename = (
                    f"{chart.family.value}-{chart.chart_id.rsplit(':', 1)[-1][:16]}."
                    f"{output_format.value}"
                )
                target = render_root / filename
                _write_once(target, content)
                receipts.append(
                    DiagnosticRenderedArtifactV1(
                        chart_id=chart.chart_id,
                        family=chart.family,
                        format=output_format,
                        relative_path=f"rendered/{filename}",
                        size_bytes=len(content),
                        sha256=digest,
                        renderer_name=DIAGNOSTIC_RENDERER_NAME,
                        renderer_version=str(matplotlib.__version__),
                        renderer_contract_version=(
                            DIAGNOSTIC_RENDERER_CONTRACT_VERSION
                        ),
                        renderer_config_id=config.config_id,
                    )
                )
    return tuple(receipts)


def _draw_chart(axis: Axes, chart: DiagnosticChartDataV1) -> None:
    axis.set_title(chart.title, loc="left", fontweight="bold")
    axis.set_xlabel(chart.x_label)
    y_label = (
        f"{chart.y_label} ({chart.y_unit})" if chart.y_unit else chart.y_label
    )
    axis.set_ylabel(y_label, fontsize=7.5 if len(y_label) > 60 else 9.0)
    if not chart.points:
        axis.grid(False)
        axis.set_xticks([])
        axis.set_yticks([])
        reason = ", ".join(chart.reason_codes)
        axis.text(
            0.5,
            0.55,
            chart.status.value.replace("_", " ").upper(),
            ha="center",
            va="center",
            transform=axis.transAxes,
            fontsize=14,
            fontweight="bold",
            color="#6C6C6C",
        )
        axis.text(
            0.5,
            0.43,
            reason,
            ha="center",
            va="center",
            transform=axis.transAxes,
            fontsize=9,
            wrap=True,
        )
    else:
        _draw_points(axis, chart)
        if chart.status is not DiagnosticStatus.AVAILABLE:
            axis.text(
                0.99,
                0.99,
                chart.status.value.replace("_", " ").upper(),
                ha="right",
                va="top",
                transform=axis.transAxes,
                fontsize=8,
                fontweight="bold",
                color="#A13D2D",
                bbox={"facecolor": "#FFF2E6", "edgecolor": "#A13D2D"},
            )
    axis.figure.text(
        0.05,
        0.025,
        fill(chart.caption, width=145),
        ha="left",
        va="bottom",
        fontsize=8,
        color="#444444",
    )


def _draw_points(axis: Axes, chart: DiagnosticChartDataV1) -> None:
    x_labels = _ordered_x_labels({point.x for point in chart.points})
    positions = {label: float(index) for index, label in enumerate(x_labels)}
    series_names = sorted({point.series for point in chart.points})
    width = min(0.8 / max(1, len(series_names)), 0.32)
    for index, series in enumerate(series_names):
        selected = sorted(
            (point for point in chart.points if point.series == series),
            key=lambda point: (positions[point.x], point.datum_id),
        )
        offset = (index - (len(series_names) - 1) / 2.0) * width
        x_values = [positions[point.x] for point in selected]
        y_values = [float(point.y) for point in selected if point.y is not None]
        if len(y_values) != len(selected):
            raise ValueError("renderable diagnostic point lacks y")
        color = _COLORS[index % len(_COLORS)]
        if chart.mark is DiagnosticMark.BAR:
            axis.bar(
                [value + offset for value in x_values],
                y_values,
                width=width,
                label=series,
                color=color,
            )
        elif chart.mark is DiagnosticMark.SCATTER:
            axis.scatter(x_values, y_values, label=series, color=color, s=24)
        elif chart.mark is DiagnosticMark.INTERVAL:
            lower = [
                (
                    max(0.0, value - float(point.lower))
                    if point.lower is not None
                    else 0.0
                )
                for value, point in zip(y_values, selected, strict=True)
            ]
            upper = [
                (
                    max(0.0, float(point.upper) - value)
                    if point.upper is not None
                    else 0.0
                )
                for value, point in zip(y_values, selected, strict=True)
            ]
            axis.errorbar(
                x_values,
                y_values,
                yerr=[lower, upper],
                fmt="o",
                capsize=3,
                label=series,
                color=color,
            )
        else:
            axis.plot(x_values, y_values, marker="o", label=series, color=color)
    axis.set_xticks(list(positions.values()), x_labels, rotation=35, ha="right")
    if len(series_names) > 1:
        axis.legend(loc="best", fontsize=7)


def _ordered_x_labels(values: set[str]) -> list[str]:
    try:
        numeric = [(float(value), value) for value in values]
    except ValueError:
        return sorted(values, key=_natural_label_key)
    return [value for _, value in sorted(numeric)]


def _natural_label_key(value: str) -> tuple[tuple[int, str | float], ...]:
    return tuple(
        (1, float(part)) if part.replace(".", "", 1).isdigit() else (0, part)
        for part in re.split(r"(\d+(?:\.\d+)?)", value.lower())
        if part
    )


__all__ = ["render_diagnostic_bundle"]
