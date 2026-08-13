"""Publication-safe reconstruction evidence and diagnostic projections.

The base-install layer reads only bounded retained machine evidence.  It never
reads tick rows, refits a model, reruns reconstruction, or treats a visual as a
promotion gate.  Optional static rendering is imported lazily from
``histdatacom.synthetic.diagnostic_rendering``.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path, PurePosixPath
from statistics import median
from typing import Any, cast

from histdatacom.orchestration.reconstruction import verify_artifact_ref
from histdatacom.publication_safety import publish_safe_path
from histdatacom.reconstruction_experiment import (
    ReconstructionExperimentManifestV1,
    read_reconstruction_experiment,
    verify_reconstruction_experiment,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.benchmark_corpus import (
    BenchmarkWindowMetricObservationV1,
    BenchmarkWindowMetricTraceV1,
    ReverseDegradationBenchmarkCorpusV1,
    read_benchmark_window_metric_trace,
    read_reverse_degradation_benchmark_corpus,
)
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.proposal_engines import (
    ProposalPortfolioEvaluationV1,
    read_proposal_portfolio_evaluation,
)
from histdatacom.synthetic.qualification import (
    PoweredQualificationDossierV1,
    QualificationStatus,
    read_powered_qualification_dossier,
    verify_powered_qualification_dossier,
)

DIAGNOSTIC_SOURCE_SCHEMA_VERSION = (
    "histdatacom.reconstruction-diagnostic-source.v1"
)
DIAGNOSTIC_CHART_DATUM_SCHEMA_VERSION = (
    "histdatacom.reconstruction-diagnostic-chart-datum.v1"
)
DIAGNOSTIC_CHART_DATA_SCHEMA_VERSION = (
    "histdatacom.reconstruction-diagnostic-chart-data.v1"
)
DIAGNOSTIC_CHART_BUNDLE_SCHEMA_VERSION = (
    "histdatacom.reconstruction-diagnostic-chart-bundle.v1"
)
DIAGNOSTIC_RENDERER_CONFIG_SCHEMA_VERSION = (
    "histdatacom.reconstruction-diagnostic-renderer-config.v1"
)
DIAGNOSTIC_PUBLICATION_SPEC_SCHEMA_VERSION = (
    "histdatacom.reconstruction-diagnostic-publication-spec.v1"
)
DIAGNOSTIC_RENDERED_ARTIFACT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-diagnostic-rendered-artifact.v1"
)
DIAGNOSTIC_PUBLICATION_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-diagnostic-publication.v1"
)

CURRENT_DIAGNOSTIC_PROVIDER_ID = "histdata.com"
CURRENT_DIAGNOSTIC_SOURCE_FORMAT = "ascii"
CURRENT_DIAGNOSTIC_TIMEFRAME = "T"
DIAGNOSTIC_RENDERER_NAME = "matplotlib-agg"
DIAGNOSTIC_RENDERER_CONTRACT_VERSION = "1.0.0"
DIAGNOSTIC_SCIENTIFIC_NONCLAIM = (
    "Chart data are bounded views of retained evidence. They do not recover "
    "historical truth, replace machine gates, or select a model."
)

DEFAULT_DIAGNOSTIC_MAX_POINTS_PER_CHART = 1024
MAX_DIAGNOSTIC_POINTS_PER_CHART = 4096
MAX_DIAGNOSTIC_CHARTS = 24
MAX_DIAGNOSTIC_SOURCES = 64
MAX_DIAGNOSTIC_SOURCE_IDS_PER_CHART = 16
MAX_DIAGNOSTIC_SOURCE_IDS_PER_DATUM = 8
MAX_DIAGNOSTIC_STRATA = 12
MAX_DIAGNOSTIC_TEXT = 512
MAX_DIAGNOSTIC_REASON_CODES = 32
MAX_DIAGNOSTIC_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_DIAGNOSTIC_BUNDLE_BYTES = 16 * 1024 * 1024
MAX_DIAGNOSTIC_RENDERED_BYTES = 32 * 1024 * 1024
MAX_DIAGNOSTIC_RENDER_WIDTH = 2400
MAX_DIAGNOSTIC_RENDER_HEIGHT = 1600
MAX_DIAGNOSTIC_RENDER_DPI = 300

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SUBJECT_ID_FIELDS = (
    "dossier_id",
    "trace_id",
    "experiment_id",
    "evaluation_id",
    "manifest_id",
    "publication_id",
    "report_id",
    "campaign_id",
    "definition_id",
    "audit_id",
    "plan_id",
    "execution_id",
)
_LOCAL_PATH_MARKERS = (
    "/Users/",
    "/home/",
    "/private/",
    "/tmp/",
    "/var/folders/",
    "C:\\Users\\",
    "file:///",
)
_SECRET_MARKERS = (
    "api_key",
    "apikey",
    "access_token",
    "refresh_token",
    "client_secret",
    "password",
    "private_key",
)
_SUPPORTED_OPTIONAL_SCHEMAS = frozenset(
    {
        "histdatacom.reconstruction-product.v2",
        "histdatacom.derived-bar-product.v1",
        "histdatacom.strategy-sensitivity-report.v1",
        "histdatacom.reconstruction-plan-execution-manifest.v1",
        "histdatacom.reconstruction-information-manifest.v1",
        "histdatacom.reconstruction-information-audit-report.v1",
        "histdatacom.reconstruction-certification-campaign-spec.v1",
        "histdatacom.reconstruction-certification-campaign-result.v1",
        "histdatacom.certification-reconstruction-product-manifest-report.v1",
        "histdatacom.certification-derived-bar-manifest-report.v1",
        "histdatacom.certification-strategy-sensitivity-report-report.v1",
    }
)


class DiagnosticFamily(str, Enum):
    """Stable required reconstruction diagnostic families."""

    FEED_EPOCH_OBSERVATION = "feed_epoch_observation"
    QUALITY_CONSTRAINT_TIMELINE = "quality_constraint_timeline"
    OBSERVATION_OPERATOR_RECONSTRUCTION = "observation_operator_reconstruction"
    POINT_PROCESS_RESIDUAL = "point_process_residual"
    MARK_REFUSAL_CALIBRATION = "mark_refusal_calibration"
    PROPER_SCORE_POWER = "proper_score_power"
    ENGINE_PORTFOLIO_DISTRIBUTION = "engine_portfolio_distribution"
    CARVING_DECISION_FLOW = "carving_decision_flow"
    CROSS_SERIES_RECONCILIATION = "cross_series_reconciliation"
    PROTECTED_SPLIT_LEAKAGE = "protected_split_leakage"
    PRODUCT_ORIGIN_LINEAGE = "product_origin_lineage"
    BAR_STRATEGY_SENSITIVITY = "bar_strategy_sensitivity"


REQUIRED_DIAGNOSTIC_FAMILIES = tuple(DiagnosticFamily)


class DiagnosticStatus(str, Enum):
    """Truthful availability states for chart data and renderings."""

    AVAILABLE = "available"
    LIMITED = "limited"
    UNDERPOWERED = "underpowered"
    REFUSED = "refused"
    MISSING_CONTEXT = "missing_context"
    UNAVAILABLE = "unavailable"
    EMPTY = "empty"


class DiagnosticMark(str, Enum):
    """Small renderer-independent chart grammar."""

    LINE = "line"
    BAR = "bar"
    SCATTER = "scatter"
    INTERVAL = "interval"


class DiagnosticRenderFormat(str, Enum):
    """Supported static publication formats."""

    SVG = "svg"
    PNG = "png"


@dataclass(frozen=True, slots=True)
class DiagnosticSourceV1:
    """Publication-safe identity for one verified retained source artifact."""

    kind: str
    subject_schema_version: str
    subject_id: str
    relative_locator: str
    size_bytes: int
    sha256: str
    source_id: str = ""
    schema_version: str = DIAGNOSTIC_SOURCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            DIAGNOSTIC_SOURCE_SCHEMA_VERSION,
            "diagnostic source",
        )
        for name in ("kind", "subject_schema_version", "subject_id"):
            object.__setattr__(
                self, name, _safe_text(getattr(self, name), name=name)
            )
        locator = _safe_relative_locator(self.relative_locator)
        object.__setattr__(self, "relative_locator", locator)
        object.__setattr__(
            self,
            "size_bytes",
            _bounded_int(
                self.size_bytes, 0, MAX_DIAGNOSTIC_ARTIFACT_BYTES, "source size"
            ),
        )
        object.__setattr__(self, "sha256", _sha256(self.sha256))
        expected = _stable_id(
            "reconstruction-diagnostic-source", self.payload()
        )
        if self.source_id and self.source_id != expected:
            raise ValueError("diagnostic source identity differs")
        object.__setattr__(self, "source_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "kind": self.kind,
            "subject_schema_version": self.subject_schema_version,
            "subject_id": self.subject_id,
            "relative_locator": self.relative_locator,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "provider_id": CURRENT_DIAGNOSTIC_PROVIDER_ID,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "source_id": self.source_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DiagnosticSourceV1:
        if data.get("provider_id") != CURRENT_DIAGNOSTIC_PROVIDER_ID:
            raise ValueError("diagnostic source provider differs")
        return cls(
            kind=str(data.get("kind", "")),
            subject_schema_version=str(data.get("subject_schema_version", "")),
            subject_id=str(data.get("subject_id", "")),
            relative_locator=str(data.get("relative_locator", "")),
            size_bytes=_strict_int(data.get("size_bytes"), "source size"),
            sha256=str(data.get("sha256", "")),
            source_id=str(data.get("source_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DiagnosticChartDatumV1:
    """One bounded scalar observation ready for a static chart."""

    x: str
    series: str
    source_ids: tuple[str, ...]
    y: float | None = None
    lower: float | None = None
    upper: float | None = None
    strata: Mapping[str, str] = field(default_factory=dict)
    annotation: str = ""
    datum_id: str = ""
    schema_version: str = DIAGNOSTIC_CHART_DATUM_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            DIAGNOSTIC_CHART_DATUM_SCHEMA_VERSION,
            "diagnostic chart datum",
        )
        object.__setattr__(self, "x", _safe_text(self.x, name="x"))
        object.__setattr__(
            self, "series", _safe_text(self.series, name="series")
        )
        source_ids = _safe_text_tuple(
            self.source_ids,
            "source_ids",
            maximum=MAX_DIAGNOSTIC_SOURCE_IDS_PER_DATUM,
            allow_empty=False,
        )
        object.__setattr__(self, "source_ids", source_ids)
        y = _optional_finite(self.y, "y")
        lower = _optional_finite(self.lower, "lower")
        upper = _optional_finite(self.upper, "upper")
        if (lower is None) != (upper is None):
            raise ValueError("diagnostic interval requires both bounds")
        if lower is not None and upper is not None and lower > upper:
            raise ValueError("diagnostic interval bounds are reversed")
        if (
            y is not None
            and lower is not None
            and not lower <= y <= cast(float, upper)
        ):
            raise ValueError("diagnostic value is outside interval bounds")
        if y is None and lower is None:
            raise ValueError("diagnostic datum has no numeric value")
        object.__setattr__(self, "y", y)
        object.__setattr__(self, "lower", lower)
        object.__setattr__(self, "upper", upper)
        strata = _safe_strata(self.strata or {})
        object.__setattr__(self, "strata", strata)
        annotation = _safe_text(
            self.annotation,
            name="annotation",
            allow_empty=True,
        )
        object.__setattr__(self, "annotation", annotation)
        expected = _stable_id("reconstruction-diagnostic-datum", self.payload())
        if self.datum_id and self.datum_id != expected:
            raise ValueError("diagnostic datum identity differs")
        object.__setattr__(self, "datum_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "x": self.x,
            "y": self.y,
            "lower": self.lower,
            "upper": self.upper,
            "series": self.series,
            "strata": dict(self.strata),
            "annotation": self.annotation,
            "source_ids": list(self.source_ids),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "datum_id": self.datum_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DiagnosticChartDatumV1:
        return cls(
            x=str(data.get("x", "")),
            y=_optional_number(data.get("y"), "y"),
            lower=_optional_number(data.get("lower"), "lower"),
            upper=_optional_number(data.get("upper"), "upper"),
            series=str(data.get("series", "")),
            strata={
                str(key): str(value)
                for key, value in _mapping(data.get("strata"), "strata").items()
            },
            annotation=str(data.get("annotation", "")),
            source_ids=_string_tuple(data.get("source_ids"), "source_ids"),
            datum_id=str(data.get("datum_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DiagnosticChartDataV1:
    """One bounded, renderer-independent diagnostic chart document."""

    family: DiagnosticFamily
    view_id: str
    title: str
    caption: str
    mark: DiagnosticMark
    x_label: str
    y_label: str
    y_unit: str
    status: DiagnosticStatus
    reason_codes: tuple[str, ...]
    source_ids: tuple[str, ...]
    points: tuple[DiagnosticChartDatumV1, ...]
    original_point_count: int
    chart_id: str = ""
    schema_version: str = DIAGNOSTIC_CHART_DATA_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            DIAGNOSTIC_CHART_DATA_SCHEMA_VERSION,
            "diagnostic chart data",
        )
        if not isinstance(self.family, DiagnosticFamily):
            raise TypeError("diagnostic family has the wrong type")
        if not isinstance(self.mark, DiagnosticMark):
            raise TypeError("diagnostic mark has the wrong type")
        if not isinstance(self.status, DiagnosticStatus):
            raise TypeError("diagnostic status has the wrong type")
        object.__setattr__(
            self, "view_id", _safe_text(self.view_id, name="view_id")
        )
        for name in ("title", "caption", "x_label", "y_label"):
            object.__setattr__(
                self, name, _safe_text(getattr(self, name), name=name)
            )
        object.__setattr__(
            self,
            "y_unit",
            _safe_text(self.y_unit, name="y_unit", allow_empty=True),
        )
        reasons = _safe_text_tuple(
            self.reason_codes,
            "reason_codes",
            maximum=MAX_DIAGNOSTIC_REASON_CODES,
            allow_empty=self.status is DiagnosticStatus.AVAILABLE,
        )
        if self.status is DiagnosticStatus.AVAILABLE and reasons:
            raise ValueError("available diagnostic chart has reason codes")
        object.__setattr__(self, "reason_codes", reasons)
        source_ids = _safe_text_tuple(
            self.source_ids,
            "source_ids",
            maximum=MAX_DIAGNOSTIC_SOURCE_IDS_PER_CHART,
            allow_empty=False,
        )
        object.__setattr__(self, "source_ids", source_ids)
        points = tuple(sorted(self.points, key=lambda item: item.datum_id))
        if len(points) > MAX_DIAGNOSTIC_POINTS_PER_CHART:
            raise ValueError("diagnostic chart point count exceeds bound")
        if len({item.datum_id for item in points}) != len(points):
            raise ValueError("diagnostic chart points duplicate")
        if any(
            not set(item.source_ids).issubset(source_ids) for item in points
        ):
            raise ValueError("diagnostic datum source is not chart-bound")
        terminal_no_data = {
            DiagnosticStatus.REFUSED,
            DiagnosticStatus.MISSING_CONTEXT,
            DiagnosticStatus.UNAVAILABLE,
            DiagnosticStatus.EMPTY,
        }
        if self.status is DiagnosticStatus.AVAILABLE and not points:
            raise ValueError("available diagnostic chart is empty")
        if self.status in terminal_no_data and points:
            raise ValueError("non-available diagnostic chart contains points")
        original = _bounded_int(
            self.original_point_count,
            len(points),
            100_000_000,
            "original point count",
        )
        object.__setattr__(self, "original_point_count", original)
        object.__setattr__(self, "points", points)
        expected = _stable_id("reconstruction-diagnostic-chart", self.payload())
        if self.chart_id and self.chart_id != expected:
            raise ValueError("diagnostic chart identity differs")
        object.__setattr__(self, "chart_id", expected)

    @property
    def retained_point_count(self) -> int:
        return len(self.points)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "family": self.family.value,
            "view_id": self.view_id,
            "title": self.title,
            "caption": self.caption,
            "mark": self.mark.value,
            "x_label": self.x_label,
            "y_label": self.y_label,
            "y_unit": self.y_unit,
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
            "source_ids": list(self.source_ids),
            "points": [item.to_dict() for item in self.points],
            "original_point_count": self.original_point_count,
            "retained_point_count": self.retained_point_count,
            "sampling": (
                "sha256_ranked_without_replacement"
                if self.original_point_count > self.retained_point_count
                else "none"
            ),
            "automatic_winner": False,
            "historical_truth_claim": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "chart_id": self.chart_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DiagnosticChartDataV1:
        if (
            data.get("automatic_winner") is not False
            or data.get("historical_truth_claim") is not False
        ):
            raise ValueError("diagnostic chart scientific nonclaim differs")
        chart = cls(
            family=DiagnosticFamily(str(data.get("family", ""))),
            view_id=str(data.get("view_id", "")),
            title=str(data.get("title", "")),
            caption=str(data.get("caption", "")),
            mark=DiagnosticMark(str(data.get("mark", ""))),
            x_label=str(data.get("x_label", "")),
            y_label=str(data.get("y_label", "")),
            y_unit=str(data.get("y_unit", "")),
            status=DiagnosticStatus(str(data.get("status", ""))),
            reason_codes=_string_tuple(
                data.get("reason_codes"), "reason_codes"
            ),
            source_ids=_string_tuple(data.get("source_ids"), "source_ids"),
            points=tuple(
                DiagnosticChartDatumV1.from_dict(_mapping(item, "point"))
                for item in _sequence(data.get("points"), "points")
            ),
            original_point_count=_strict_int(
                data.get("original_point_count"), "original point count"
            ),
            chart_id=str(data.get("chart_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("retained_point_count") != chart.retained_point_count:
            raise ValueError("diagnostic retained point count differs")
        expected_sampling = (
            "sha256_ranked_without_replacement"
            if chart.original_point_count > chart.retained_point_count
            else "none"
        )
        if data.get("sampling") != expected_sampling:
            raise ValueError("diagnostic chart sampling declaration differs")
        return chart


@dataclass(frozen=True, slots=True)
class DiagnosticChartBundleV1:
    """Complete publication-safe chart-data family for one evidence graph."""

    dossier_id: str
    experiment_id: str
    campaign_id: str
    sources: tuple[DiagnosticSourceV1, ...]
    charts: tuple[DiagnosticChartDataV1, ...]
    bundle_id: str = ""
    schema_version: str = DIAGNOSTIC_CHART_BUNDLE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            DIAGNOSTIC_CHART_BUNDLE_SCHEMA_VERSION,
            "diagnostic chart bundle",
        )
        for name in ("dossier_id", "experiment_id", "campaign_id"):
            object.__setattr__(
                self, name, _safe_text(getattr(self, name), name=name)
            )
        sources = tuple(sorted(self.sources, key=lambda item: item.source_id))
        if not sources or len(sources) > MAX_DIAGNOSTIC_SOURCES:
            raise ValueError("diagnostic bundle source count is invalid")
        if len({item.source_id for item in sources}) != len(sources):
            raise ValueError("diagnostic bundle sources duplicate")
        object.__setattr__(self, "sources", sources)
        family_order = {
            family: index
            for index, family in enumerate(REQUIRED_DIAGNOSTIC_FAMILIES)
        }
        charts = tuple(
            sorted(
                self.charts,
                key=lambda item: (family_order[item.family], item.view_id),
            )
        )
        if (
            not len(REQUIRED_DIAGNOSTIC_FAMILIES)
            <= len(charts)
            <= MAX_DIAGNOSTIC_CHARTS
        ):
            raise ValueError("diagnostic chart count exceeds bound")
        if {item.family for item in charts} != set(
            REQUIRED_DIAGNOSTIC_FAMILIES
        ):
            raise ValueError("diagnostic bundle family coverage differs")
        if len({(item.family, item.view_id) for item in charts}) != len(charts):
            raise ValueError("diagnostic bundle family views duplicate")
        source_ids = {item.source_id for item in sources}
        if any(
            not set(item.source_ids).issubset(source_ids) for item in charts
        ):
            raise ValueError("diagnostic chart source is not bundle-bound")
        object.__setattr__(self, "charts", charts)
        expected = _stable_id(
            "reconstruction-diagnostic-bundle", self.payload()
        )
        if self.bundle_id and self.bundle_id != expected:
            raise ValueError("diagnostic bundle identity differs")
        object.__setattr__(self, "bundle_id", expected)
        if len(self.to_json().encode("utf-8")) > MAX_DIAGNOSTIC_BUNDLE_BYTES:
            raise ValueError("diagnostic chart bundle exceeds byte bound")

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "dossier_id": self.dossier_id,
            "experiment_id": self.experiment_id,
            "campaign_id": self.campaign_id,
            "provider_id": CURRENT_DIAGNOSTIC_PROVIDER_ID,
            "source_format": CURRENT_DIAGNOSTIC_SOURCE_FORMAT,
            "timeframe": CURRENT_DIAGNOSTIC_TIMEFRAME,
            "sources": [item.to_dict() for item in self.sources],
            "charts": [item.to_dict() for item in self.charts],
            "family_count": len(REQUIRED_DIAGNOSTIC_FAMILIES),
            "chart_count": len(self.charts),
            "scientific_nonclaim": DIAGNOSTIC_SCIENTIFIC_NONCLAIM,
            "automatic_winner": False,
            "historical_truth_claim": False,
            "raw_rows_embedded": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "bundle_id": self.bundle_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DiagnosticChartBundleV1:
        if (
            data.get("provider_id") != CURRENT_DIAGNOSTIC_PROVIDER_ID
            or data.get("source_format") != CURRENT_DIAGNOSTIC_SOURCE_FORMAT
            or data.get("timeframe") != CURRENT_DIAGNOSTIC_TIMEFRAME
            or data.get("scientific_nonclaim") != DIAGNOSTIC_SCIENTIFIC_NONCLAIM
            or data.get("automatic_winner") is not False
            or data.get("historical_truth_claim") is not False
            or data.get("raw_rows_embedded") is not False
        ):
            raise ValueError("diagnostic bundle scope or nonclaim differs")
        bundle = cls(
            dossier_id=str(data.get("dossier_id", "")),
            experiment_id=str(data.get("experiment_id", "")),
            campaign_id=str(data.get("campaign_id", "")),
            sources=tuple(
                DiagnosticSourceV1.from_dict(_mapping(item, "source"))
                for item in _sequence(data.get("sources"), "sources")
            ),
            charts=tuple(
                DiagnosticChartDataV1.from_dict(_mapping(item, "chart"))
                for item in _sequence(data.get("charts"), "charts")
            ),
            bundle_id=str(data.get("bundle_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("family_count") != len(REQUIRED_DIAGNOSTIC_FAMILIES):
            raise ValueError("diagnostic family count differs")
        if data.get("chart_count") != len(bundle.charts):
            raise ValueError("diagnostic chart count differs")
        return bundle

    @classmethod
    def from_json(cls, text: str) -> DiagnosticChartBundleV1:
        if len(text.encode("utf-8")) > MAX_DIAGNOSTIC_BUNDLE_BYTES:
            raise ValueError("diagnostic bundle JSON exceeds bound")
        return cls.from_dict(_mapping(json.loads(text), "bundle"))


@dataclass(frozen=True, slots=True)
class DiagnosticRendererConfigV1:
    """Deterministic optional static-renderer configuration."""

    formats: tuple[DiagnosticRenderFormat, ...] = ()
    width_px: int = 1200
    height_px: int = 720
    dpi: int = 120
    style: str = "histdatacom-publication-v1"
    font_family: str = "DejaVu Sans"
    hash_salt: str = "histdatacom-diagnostics-v1"
    config_id: str = ""
    schema_version: str = DIAGNOSTIC_RENDERER_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            DIAGNOSTIC_RENDERER_CONFIG_SCHEMA_VERSION,
            "diagnostic renderer config",
        )
        formats = tuple(sorted(set(self.formats), key=lambda item: item.value))
        if any(
            not isinstance(item, DiagnosticRenderFormat) for item in formats
        ):
            raise TypeError("diagnostic render format has the wrong type")
        object.__setattr__(self, "formats", formats)
        object.__setattr__(
            self,
            "width_px",
            _bounded_int(
                self.width_px, 320, MAX_DIAGNOSTIC_RENDER_WIDTH, "render width"
            ),
        )
        object.__setattr__(
            self,
            "height_px",
            _bounded_int(
                self.height_px,
                240,
                MAX_DIAGNOSTIC_RENDER_HEIGHT,
                "render height",
            ),
        )
        object.__setattr__(
            self,
            "dpi",
            _bounded_int(self.dpi, 72, MAX_DIAGNOSTIC_RENDER_DPI, "render dpi"),
        )
        for name in ("style", "font_family", "hash_salt"):
            object.__setattr__(
                self, name, _safe_text(getattr(self, name), name=name)
            )
        expected = _stable_id(
            "reconstruction-diagnostic-renderer", self.payload()
        )
        if self.config_id and self.config_id != expected:
            raise ValueError("diagnostic renderer config identity differs")
        object.__setattr__(self, "config_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "formats": [item.value for item in self.formats],
            "width_px": self.width_px,
            "height_px": self.height_px,
            "dpi": self.dpi,
            "style": self.style,
            "font_family": self.font_family,
            "hash_salt": self.hash_salt,
            "renderer_name": DIAGNOSTIC_RENDERER_NAME,
            "renderer_contract_version": DIAGNOSTIC_RENDERER_CONTRACT_VERSION,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "config_id": self.config_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DiagnosticRendererConfigV1:
        if (
            data.get("renderer_name") != DIAGNOSTIC_RENDERER_NAME
            or data.get("renderer_contract_version")
            != DIAGNOSTIC_RENDERER_CONTRACT_VERSION
        ):
            raise ValueError("diagnostic renderer contract differs")
        return cls(
            formats=tuple(
                DiagnosticRenderFormat(str(item))
                for item in _sequence(data.get("formats"), "formats")
            ),
            width_px=_strict_int(data.get("width_px"), "render width"),
            height_px=_strict_int(data.get("height_px"), "render height"),
            dpi=_strict_int(data.get("dpi"), "render dpi"),
            style=str(data.get("style", "")),
            font_family=str(data.get("font_family", "")),
            hash_salt=str(data.get("hash_salt", "")),
            config_id=str(data.get("config_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DiagnosticPublicationSpecV1:
    """Local strong references and bounds for one diagnostic publication."""

    qualification_dossier: ArtifactRef
    additional_artifacts: tuple[ArtifactRef, ...] = ()
    max_points_per_chart: int = DEFAULT_DIAGNOSTIC_MAX_POINTS_PER_CHART
    renderer: DiagnosticRendererConfigV1 = field(
        default_factory=DiagnosticRendererConfigV1
    )
    spec_id: str = ""
    schema_version: str = DIAGNOSTIC_PUBLICATION_SPEC_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            DIAGNOSTIC_PUBLICATION_SPEC_SCHEMA_VERSION,
            "diagnostic publication spec",
        )
        qualification = _strong_ref(self.qualification_dossier)
        if qualification.kind != "powered_qualification_dossier_v1":
            raise ValueError("diagnostic spec requires a powered dossier")
        object.__setattr__(self, "qualification_dossier", qualification)
        additional = tuple(
            sorted(
                (_strong_ref(item) for item in self.additional_artifacts),
                key=lambda item: (item.kind, item.sha256),
            )
        )
        if len(additional) > MAX_DIAGNOSTIC_SOURCES - 8:
            raise ValueError(
                "diagnostic additional artifact count exceeds bound"
            )
        if len({(item.kind, item.sha256) for item in additional}) != len(
            additional
        ):
            raise ValueError("diagnostic additional artifacts duplicate")
        if any("broker" in item.kind.lower() for item in additional):
            raise ValueError(
                "broker diagnostic artifacts are a later milestone"
            )
        object.__setattr__(self, "additional_artifacts", additional)
        object.__setattr__(
            self,
            "max_points_per_chart",
            _bounded_int(
                self.max_points_per_chart,
                1,
                MAX_DIAGNOSTIC_POINTS_PER_CHART,
                "max points per chart",
            ),
        )
        if not isinstance(self.renderer, DiagnosticRendererConfigV1):
            raise TypeError("diagnostic renderer must use v1")
        expected = _stable_id(
            "reconstruction-diagnostic-spec", self.identity_payload()
        )
        if self.spec_id and self.spec_id != expected:
            raise ValueError("diagnostic publication spec identity differs")
        object.__setattr__(self, "spec_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "qualification_dossier": _artifact_content_identity(
                self.qualification_dossier
            ),
            "additional_artifacts": [
                _artifact_content_identity(item)
                for item in self.additional_artifacts
            ],
            "max_points_per_chart": self.max_points_per_chart,
            "renderer": self.renderer.to_dict(),
            "provider_id": CURRENT_DIAGNOSTIC_PROVIDER_ID,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "qualification_dossier_local_ref": self.qualification_dossier.to_dict(),
            "additional_artifact_local_refs": [
                item.to_dict() for item in self.additional_artifacts
            ],
            "spec_id": self.spec_id,
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DiagnosticPublicationSpecV1:
        if data.get("provider_id") != CURRENT_DIAGNOSTIC_PROVIDER_ID:
            raise ValueError("diagnostic spec provider differs")
        spec = cls(
            qualification_dossier=ArtifactRef.from_dict(
                _mapping(
                    data.get("qualification_dossier_local_ref"),
                    "qualification dossier ref",
                )
            ),
            additional_artifacts=tuple(
                ArtifactRef.from_dict(_mapping(item, "additional artifact ref"))
                for item in _sequence(
                    data.get("additional_artifact_local_refs"),
                    "additional artifact refs",
                )
            ),
            max_points_per_chart=_strict_int(
                data.get("max_points_per_chart"), "max points per chart"
            ),
            renderer=DiagnosticRendererConfigV1.from_dict(
                _mapping(data.get("renderer"), "renderer")
            ),
            spec_id=str(data.get("spec_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("qualification_dossier") != _artifact_content_identity(
            spec.qualification_dossier
        ):
            raise ValueError("diagnostic qualification identity differs")
        if data.get("additional_artifacts") != [
            _artifact_content_identity(item)
            for item in spec.additional_artifacts
        ]:
            raise ValueError("diagnostic additional identities differ")
        return spec

    @classmethod
    def from_json(cls, text: str) -> DiagnosticPublicationSpecV1:
        if len(text.encode("utf-8")) > MAX_DIAGNOSTIC_ARTIFACT_BYTES:
            raise ValueError("diagnostic spec exceeds byte bound")
        return cls.from_dict(_mapping(json.loads(text), "diagnostic spec"))


@dataclass(frozen=True, slots=True)
class DiagnosticRenderedArtifactV1:
    """Hash receipt for one deterministic optional static artifact."""

    chart_id: str
    family: DiagnosticFamily
    format: DiagnosticRenderFormat
    relative_path: str
    size_bytes: int
    sha256: str
    renderer_name: str
    renderer_version: str
    renderer_contract_version: str
    renderer_config_id: str
    artifact_id: str = ""
    schema_version: str = DIAGNOSTIC_RENDERED_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            DIAGNOSTIC_RENDERED_ARTIFACT_SCHEMA_VERSION,
            "diagnostic rendered artifact",
        )
        for name in (
            "chart_id",
            "renderer_name",
            "renderer_version",
            "renderer_contract_version",
            "renderer_config_id",
        ):
            object.__setattr__(
                self, name, _safe_text(getattr(self, name), name=name)
            )
        if self.renderer_name != DIAGNOSTIC_RENDERER_NAME:
            raise ValueError("diagnostic renderer name differs")
        if (
            self.renderer_contract_version
            != DIAGNOSTIC_RENDERER_CONTRACT_VERSION
        ):
            raise ValueError("diagnostic renderer contract version differs")
        if not isinstance(self.family, DiagnosticFamily):
            raise TypeError("rendered diagnostic family has the wrong type")
        if not isinstance(self.format, DiagnosticRenderFormat):
            raise TypeError("rendered diagnostic format has the wrong type")
        relative = _safe_relative_locator(self.relative_path)
        if PurePosixPath(relative).suffix != f".{self.format.value}":
            raise ValueError("rendered diagnostic suffix differs")
        object.__setattr__(self, "relative_path", relative)
        object.__setattr__(
            self,
            "size_bytes",
            _bounded_int(
                self.size_bytes,
                1,
                MAX_DIAGNOSTIC_RENDERED_BYTES,
                "rendered size",
            ),
        )
        object.__setattr__(self, "sha256", _sha256(self.sha256))
        expected = _stable_id(
            "reconstruction-diagnostic-render", self.payload()
        )
        if self.artifact_id and self.artifact_id != expected:
            raise ValueError("rendered diagnostic artifact identity differs")
        object.__setattr__(self, "artifact_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "chart_id": self.chart_id,
            "family": self.family.value,
            "format": self.format.value,
            "relative_path": self.relative_path,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "renderer_name": self.renderer_name,
            "renderer_version": self.renderer_version,
            "renderer_contract_version": self.renderer_contract_version,
            "renderer_config_id": self.renderer_config_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "artifact_id": self.artifact_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DiagnosticRenderedArtifactV1:
        return cls(
            chart_id=str(data.get("chart_id", "")),
            family=DiagnosticFamily(str(data.get("family", ""))),
            format=DiagnosticRenderFormat(str(data.get("format", ""))),
            relative_path=str(data.get("relative_path", "")),
            size_bytes=_strict_int(data.get("size_bytes"), "rendered size"),
            sha256=str(data.get("sha256", "")),
            renderer_name=str(data.get("renderer_name", "")),
            renderer_version=str(data.get("renderer_version", "")),
            renderer_contract_version=str(
                data.get("renderer_contract_version", "")
            ),
            renderer_config_id=str(data.get("renderer_config_id", "")),
            artifact_id=str(data.get("artifact_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DiagnosticPublicationManifestV1:
    """Atomic manifest binding chart data and optional rendered artifacts."""

    spec_id: str
    bundle_id: str
    bundle_relative_path: str
    bundle_size_bytes: int
    bundle_sha256: str
    chart_count: int
    status_counts: Mapping[str, int]
    view_status_counts: Mapping[str, int]
    renderer_config: DiagnosticRendererConfigV1
    rendered_artifacts: tuple[DiagnosticRenderedArtifactV1, ...]
    publication_id: str = ""
    schema_version: str = DIAGNOSTIC_PUBLICATION_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            DIAGNOSTIC_PUBLICATION_MANIFEST_SCHEMA_VERSION,
            "diagnostic publication manifest",
        )
        for name in ("spec_id", "bundle_id"):
            object.__setattr__(
                self, name, _safe_text(getattr(self, name), name=name)
            )
        relative = _safe_relative_locator(self.bundle_relative_path)
        if not relative.endswith(".json"):
            raise ValueError("diagnostic bundle path is not JSON")
        object.__setattr__(self, "bundle_relative_path", relative)
        object.__setattr__(
            self,
            "bundle_size_bytes",
            _bounded_int(
                self.bundle_size_bytes,
                1,
                MAX_DIAGNOSTIC_BUNDLE_BYTES,
                "diagnostic bundle size",
            ),
        )
        object.__setattr__(self, "bundle_sha256", _sha256(self.bundle_sha256))
        object.__setattr__(
            self,
            "chart_count",
            _bounded_int(
                self.chart_count,
                len(REQUIRED_DIAGNOSTIC_FAMILIES),
                MAX_DIAGNOSTIC_CHARTS,
                "diagnostic chart count",
            ),
        )
        object.__setattr__(
            self,
            "status_counts",
            _validated_status_counts(
                self.status_counts,
                total=len(REQUIRED_DIAGNOSTIC_FAMILIES),
                name="diagnostic family status counts",
            ),
        )
        object.__setattr__(
            self,
            "view_status_counts",
            _validated_status_counts(
                self.view_status_counts,
                total=self.chart_count,
                name="diagnostic view status counts",
            ),
        )
        if not isinstance(self.renderer_config, DiagnosticRendererConfigV1):
            raise TypeError("diagnostic publication renderer must use v1")
        rendered = tuple(
            sorted(
                self.rendered_artifacts,
                key=lambda item: (
                    item.family.value,
                    item.chart_id,
                    item.format.value,
                ),
            )
        )
        expected_count = self.chart_count * len(self.renderer_config.formats)
        if len(rendered) != expected_count:
            raise ValueError("diagnostic rendered artifact coverage differs")
        if len({(item.chart_id, item.format) for item in rendered}) != len(
            rendered
        ):
            raise ValueError("diagnostic rendered artifacts duplicate")
        if any(
            item.renderer_config_id != self.renderer_config.config_id
            for item in rendered
        ):
            raise ValueError("diagnostic rendered config identity differs")
        object.__setattr__(self, "rendered_artifacts", rendered)
        expected = _stable_id(
            "reconstruction-diagnostic-publication", self.payload()
        )
        if self.publication_id and self.publication_id != expected:
            raise ValueError("diagnostic publication identity differs")
        object.__setattr__(self, "publication_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "spec_id": self.spec_id,
            "bundle_id": self.bundle_id,
            "bundle_relative_path": self.bundle_relative_path,
            "bundle_size_bytes": self.bundle_size_bytes,
            "bundle_sha256": self.bundle_sha256,
            "chart_count": self.chart_count,
            "status_counts": dict(self.status_counts),
            "view_status_counts": dict(self.view_status_counts),
            "renderer_config": self.renderer_config.to_dict(),
            "rendered_artifacts": [
                item.to_dict() for item in self.rendered_artifacts
            ],
            "provider_id": CURRENT_DIAGNOSTIC_PROVIDER_ID,
            "family_count": len(REQUIRED_DIAGNOSTIC_FAMILIES),
            "raw_rows_embedded": False,
            "scientific_nonclaim": DIAGNOSTIC_SCIENTIFIC_NONCLAIM,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "publication_id": self.publication_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> DiagnosticPublicationManifestV1:
        if (
            data.get("provider_id") != CURRENT_DIAGNOSTIC_PROVIDER_ID
            or data.get("family_count") != len(REQUIRED_DIAGNOSTIC_FAMILIES)
            or data.get("raw_rows_embedded") is not False
            or data.get("scientific_nonclaim") != DIAGNOSTIC_SCIENTIFIC_NONCLAIM
        ):
            raise ValueError("diagnostic publication scope differs")
        return cls(
            spec_id=str(data.get("spec_id", "")),
            bundle_id=str(data.get("bundle_id", "")),
            bundle_relative_path=str(data.get("bundle_relative_path", "")),
            bundle_size_bytes=_strict_int(
                data.get("bundle_size_bytes"), "diagnostic bundle size"
            ),
            bundle_sha256=str(data.get("bundle_sha256", "")),
            chart_count=_strict_int(
                data.get("chart_count"), "diagnostic chart count"
            ),
            status_counts={
                str(key): _strict_int(value, "family status count")
                for key, value in _mapping(
                    data.get("status_counts"), "family status counts"
                ).items()
            },
            view_status_counts={
                str(key): _strict_int(value, "view status count")
                for key, value in _mapping(
                    data.get("view_status_counts"), "view status counts"
                ).items()
            },
            renderer_config=DiagnosticRendererConfigV1.from_dict(
                _mapping(data.get("renderer_config"), "renderer config")
            ),
            rendered_artifacts=tuple(
                DiagnosticRenderedArtifactV1.from_dict(
                    _mapping(item, "rendered artifact")
                )
                for item in _sequence(
                    data.get("rendered_artifacts"), "rendered artifacts"
                )
            ),
            publication_id=str(data.get("publication_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> DiagnosticPublicationManifestV1:
        if len(text.encode("utf-8")) > MAX_DIAGNOSTIC_ARTIFACT_BYTES:
            raise ValueError("diagnostic publication JSON exceeds bound")
        return cls.from_dict(_mapping(json.loads(text), "publication"))


@dataclass(frozen=True, slots=True)
class _EvidenceContext:
    dossier: PoweredQualificationDossierV1
    evaluation: ProposalPortfolioEvaluationV1
    trace: BenchmarkWindowMetricTraceV1
    experiment: ReconstructionExperimentManifestV1
    corpus: ReverseDegradationBenchmarkCorpusV1
    feed_epochs: Mapping[str, Any]
    observation_campaign: Mapping[str, Any]
    optional_payloads: tuple[tuple[DiagnosticSourceV1, Mapping[str, Any]], ...]
    source_by_key: Mapping[str, DiagnosticSourceV1]
    evidence_limitations: tuple[str, ...]


def build_reconstruction_diagnostic_bundle(
    spec: DiagnosticPublicationSpecV1,
) -> DiagnosticChartBundleV1:
    """Verify retained evidence and build every required chart-data family."""
    if not isinstance(spec, DiagnosticPublicationSpecV1):
        raise TypeError("diagnostic publication spec must use v1")
    context, sources = _load_evidence_context(spec)
    charts = (
        _feed_epoch_chart(context, spec.max_points_per_chart),
        _quality_constraint_chart(context, spec.max_points_per_chart),
        *_observation_operator_charts(context, spec.max_points_per_chart),
        _point_process_residual_chart(context, spec.max_points_per_chart),
        *_mark_refusal_charts(context, spec.max_points_per_chart),
        *_proper_score_power_charts(context, spec.max_points_per_chart),
        *_engine_portfolio_charts(context, spec.max_points_per_chart),
        _carving_flow_chart(context, spec.max_points_per_chart),
        *_cross_series_charts(context, spec.max_points_per_chart),
        _protected_split_chart(context, spec.max_points_per_chart),
        _product_origin_chart(context, spec.max_points_per_chart),
        _bar_strategy_chart(context, spec.max_points_per_chart),
    )
    return DiagnosticChartBundleV1(
        dossier_id=context.dossier.dossier_id,
        experiment_id=context.dossier.experiment_id,
        campaign_id=context.dossier.campaign_id,
        sources=sources,
        charts=charts,
    )


def publish_reconstruction_diagnostics(
    spec: DiagnosticPublicationSpecV1 | str | Path,
    *,
    output_directory: str | Path,
) -> DiagnosticPublicationManifestV1:
    """Build, optionally render, atomically write, and verify diagnostics."""
    selected = (
        read_diagnostic_publication_spec(spec)
        if isinstance(spec, (str, Path))
        else spec
    )
    bundle = build_reconstruction_diagnostic_bundle(selected)
    root = Path(output_directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    bundle_bytes = bundle.to_json().encode("utf-8") + b"\n"
    bundle_digest = hashlib.sha256(bundle_bytes).hexdigest()
    bundle_name = f"reconstruction-diagnostic-bundle-{bundle_digest}.json"
    _write_once(root / bundle_name, bundle_bytes)

    rendered: tuple[DiagnosticRenderedArtifactV1, ...] = ()
    if selected.renderer.formats:
        from histdatacom.synthetic.diagnostic_rendering import (
            render_diagnostic_bundle,
        )

        rendered = render_diagnostic_bundle(
            bundle,
            selected.renderer,
            output_directory=root,
        )
    manifest = DiagnosticPublicationManifestV1(
        spec_id=selected.spec_id,
        bundle_id=bundle.bundle_id,
        bundle_relative_path=bundle_name,
        bundle_size_bytes=len(bundle_bytes),
        bundle_sha256=bundle_digest,
        chart_count=len(bundle.charts),
        status_counts=_family_status_counts(bundle.charts),
        view_status_counts=dict(
            sorted(Counter(item.status.value for item in bundle.charts).items())
        ),
        renderer_config=selected.renderer,
        rendered_artifacts=rendered,
    )
    manifest_bytes = manifest.to_json().encode("utf-8") + b"\n"
    manifest_digest = hashlib.sha256(manifest_bytes).hexdigest()
    manifest_path = (
        root / f"reconstruction-diagnostic-publication-{manifest_digest}.json"
    )
    _write_once(manifest_path, manifest_bytes)
    verified, _ = verify_reconstruction_diagnostic_publication(manifest_path)
    return verified


def verify_reconstruction_diagnostic_publication(
    path: str | Path,
) -> tuple[DiagnosticPublicationManifestV1, DiagnosticChartBundleV1]:
    """Verify one publication manifest, chart bundle, and every static artifact."""
    selected = Path(path).expanduser().resolve()
    payload = _read_content_addressed_json(
        selected,
        prefix="reconstruction-diagnostic-publication",
        maximum=MAX_DIAGNOSTIC_ARTIFACT_BYTES,
    )
    manifest = DiagnosticPublicationManifestV1.from_dict(payload)
    bundle_path = _safe_child(selected.parent, manifest.bundle_relative_path)
    bundle_bytes = _read_exact_artifact(
        bundle_path,
        size_bytes=manifest.bundle_size_bytes,
        sha256=manifest.bundle_sha256,
        maximum=MAX_DIAGNOSTIC_BUNDLE_BYTES,
    )
    bundle = DiagnosticChartBundleV1.from_json(bundle_bytes.decode("utf-8"))
    if bundle.bundle_id != manifest.bundle_id:
        raise ValueError("diagnostic publication bundle identity differs")
    if manifest.chart_count != len(bundle.charts):
        raise ValueError("diagnostic publication chart count differs")
    if manifest.status_counts != _family_status_counts(bundle.charts):
        raise ValueError("diagnostic publication family statuses differ")
    expected_view_statuses = dict(
        sorted(Counter(item.status.value for item in bundle.charts).items())
    )
    if manifest.view_status_counts != expected_view_statuses:
        raise ValueError("diagnostic publication view statuses differ")
    chart_by_id = {item.chart_id: item for item in bundle.charts}
    expected_rendered = {
        (chart.chart_id, output_format)
        for chart in bundle.charts
        for output_format in manifest.renderer_config.formats
    }
    actual_rendered = {
        (receipt.chart_id, receipt.format)
        for receipt in manifest.rendered_artifacts
    }
    if actual_rendered != expected_rendered:
        raise ValueError("diagnostic rendered chart coverage differs")
    for receipt in manifest.rendered_artifacts:
        chart = chart_by_id.get(receipt.chart_id)
        if chart is None or chart.family is not receipt.family:
            raise ValueError("rendered diagnostic chart binding differs")
        _read_exact_artifact(
            _safe_child(selected.parent, receipt.relative_path),
            size_bytes=receipt.size_bytes,
            sha256=receipt.sha256,
            maximum=MAX_DIAGNOSTIC_RENDERED_BYTES,
        )
    return manifest, bundle


def diagnostic_publication_listing(path: str | Path) -> dict[str, JSONValue]:
    """Return a bounded publication-safe chart and artifact listing."""
    manifest, bundle = verify_reconstruction_diagnostic_publication(path)
    render_by_chart: dict[str, list[JSONValue]] = {}
    for item in manifest.rendered_artifacts:
        render_by_chart.setdefault(item.chart_id, []).append(item.to_dict())
    family_status_counts: dict[str, JSONValue] = dict(
        _family_status_counts(bundle.charts)
    )
    view_status_counts: dict[str, JSONValue] = dict(
        sorted(Counter(item.status.value for item in bundle.charts).items())
    )
    return {
        "schema_version": DIAGNOSTIC_PUBLICATION_MANIFEST_SCHEMA_VERSION,
        "publication_id": manifest.publication_id,
        "spec_id": manifest.spec_id,
        "bundle_id": bundle.bundle_id,
        "provider_id": CURRENT_DIAGNOSTIC_PROVIDER_ID,
        "family_count": len(REQUIRED_DIAGNOSTIC_FAMILIES),
        "chart_count": len(bundle.charts),
        "status_counts": family_status_counts,
        "view_status_counts": view_status_counts,
        "charts": [
            {
                "chart_id": chart.chart_id,
                "family": chart.family.value,
                "view_id": chart.view_id,
                "title": chart.title,
                "status": chart.status.value,
                "reason_codes": list(chart.reason_codes),
                "original_point_count": chart.original_point_count,
                "retained_point_count": chart.retained_point_count,
                "source_ids": list(chart.source_ids),
                "rendered_artifacts": render_by_chart.get(chart.chart_id, []),
            }
            for chart in bundle.charts
        ],
        "scientific_nonclaim": DIAGNOSTIC_SCIENTIFIC_NONCLAIM,
        "raw_rows_embedded": False,
    }


def write_diagnostic_publication_spec(
    spec: DiagnosticPublicationSpecV1,
    path: str | Path,
) -> Path:
    """Write one local regeneration specification atomically."""
    if not isinstance(spec, DiagnosticPublicationSpecV1):
        raise TypeError("diagnostic publication spec must use v1")
    target = Path(path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    _write_once(target, spec.to_json().encode("utf-8") + b"\n")
    return target


def read_diagnostic_publication_spec(
    path: str | Path,
) -> DiagnosticPublicationSpecV1:
    """Read a bounded local regeneration specification."""
    selected = Path(path).expanduser().resolve()
    if not selected.is_file():
        raise ValueError(f"diagnostic spec is missing: {selected}")
    if selected.stat().st_size > MAX_DIAGNOSTIC_ARTIFACT_BYTES:
        raise ValueError("diagnostic spec exceeds byte bound")
    return DiagnosticPublicationSpecV1.from_json(
        selected.read_text(encoding="utf-8")
    )


def _load_evidence_context(
    spec: DiagnosticPublicationSpecV1,
) -> tuple[_EvidenceContext, tuple[DiagnosticSourceV1, ...]]:
    verify_artifact_ref(spec.qualification_dossier)
    dossier = read_powered_qualification_dossier(
        spec.qualification_dossier.path
    )
    if dossier.control_checks.get("histdata_only") is not True:
        raise ValueError("diagnostic qualification scope differs")

    evaluation_ref = dossier.input_artifacts["evaluation"]
    experiment_ref = dossier.input_artifacts["experiment"]
    trace_ref = dossier.input_artifacts["window_metric_trace"]
    evaluation = read_proposal_portfolio_evaluation(evaluation_ref.path)
    experiment = read_reconstruction_experiment(experiment_ref.path)
    trace = read_benchmark_window_metric_trace(trace_ref.path)
    evidence_limitations: tuple[str, ...] = ()
    try:
        verify_powered_qualification_dossier(dossier)
    except ValueError as err:
        verification = verify_reconstruction_experiment(experiment)
        if str(
            err
        ) != "qualification experiment verification failed" or verification.finding_codes != (
            "implementation_identity_changed",
        ):
            raise
        evidence_limitations = ("experiment_implementation_identity_changed",)
    corpus_ref = evaluation.artifact_refs.get("manifest")
    if corpus_ref is None:
        raise ValueError("diagnostic evaluation has no corpus manifest")
    verify_artifact_ref(corpus_ref)
    corpus = read_reverse_degradation_benchmark_corpus(corpus_ref.path)
    if (
        corpus.corpus_id != dossier.corpus_id
        or trace.corpus_id != corpus.corpus_id
    ):
        raise ValueError("diagnostic corpus identity differs")

    feed_ref = corpus.dependency_artifacts.get("feed_epochs")
    observation_ref = corpus.dependency_artifacts.get("observation_campaign")
    if feed_ref is None or observation_ref is None:
        raise ValueError("diagnostic corpus lacks core evidence dependencies")
    feed_payload = _read_verified_json(feed_ref)
    observation_payload = _read_verified_json(observation_ref)
    if feed_payload.get("definition_id") != corpus.feed_epoch_definition_id:
        raise ValueError("diagnostic feed epoch identity differs")
    if observation_payload.get("campaign_id") != _metadata_text(
        observation_ref, "campaign_id"
    ):
        raise ValueError("diagnostic observation campaign identity differs")

    core_refs = {
        "qualification": spec.qualification_dossier,
        "evaluation": evaluation_ref,
        "experiment": experiment_ref,
        "trace": trace_ref,
        "corpus": corpus_ref,
        "scorecard": dossier.input_artifacts["scorecard"],
        "feed_epochs": feed_ref,
        "observation_campaign": observation_ref,
    }
    source_by_key: dict[str, DiagnosticSourceV1] = {}
    sources: list[DiagnosticSourceV1] = []
    core_payloads: dict[str, Mapping[str, Any]] = {
        "qualification": dossier.to_dict(),
        "evaluation": evaluation.to_dict(),
        "experiment": experiment.to_dict(),
        "trace": trace.to_dict(),
        "corpus": corpus.to_dict(),
        "scorecard": _read_verified_json(dossier.input_artifacts["scorecard"]),
        "feed_epochs": feed_payload,
        "observation_campaign": observation_payload,
    }
    for key, ref in core_refs.items():
        verify_artifact_ref(ref)
        source = _source_identity(ref, core_payloads[key])
        source_by_key[key] = source
        sources.append(source)

    optional_payloads: list[tuple[DiagnosticSourceV1, Mapping[str, Any]]] = []
    for ref in spec.additional_artifacts:
        payload = _read_verified_json(ref)
        schema = str(payload.get("schema_version", ""))
        if schema not in _SUPPORTED_OPTIONAL_SCHEMAS:
            raise ValueError(
                f"unsupported diagnostic evidence schema: {schema}"
            )
        _verify_histdata_optional_scope(ref, payload)
        source = _source_identity(ref, payload)
        sources.append(source)
        optional_payloads.append((source, payload))

    if len({item.source_id for item in sources}) != len(sources):
        raise ValueError("diagnostic evidence sources duplicate")
    context = _EvidenceContext(
        dossier=dossier,
        evaluation=evaluation,
        trace=trace,
        experiment=experiment,
        corpus=corpus,
        feed_epochs=feed_payload,
        observation_campaign=observation_payload,
        optional_payloads=tuple(optional_payloads),
        source_by_key=source_by_key,
        evidence_limitations=evidence_limitations,
    )
    return context, tuple(sources)


def _feed_epoch_chart(
    context: _EvidenceContext, maximum: int
) -> DiagnosticChartDataV1:
    feed_source = context.source_by_key["feed_epochs"]
    trace_source = context.source_by_key["trace"]
    corpus_source = context.source_by_key["corpus"]
    boundaries = _sequence(
        context.feed_epochs.get("boundaries", []), "feed epoch boundaries"
    )
    points = [
        _datum(
            x=str(
                _mapping(item, "feed epoch boundary").get("right_period", "")
            ),
            y=_finite_number(
                _mapping(item, "feed epoch boundary").get("support"),
                "boundary support",
            ),
            series="boundary stability support",
            sources=(feed_source,),
            annotation=str(
                _mapping(item, "feed epoch boundary").get(
                    "transition_label", ""
                )
            ),
        )
        for item in boundaries
    ]
    windows = {item.window_id: item for item in context.corpus.windows}
    metric_specs = (
        ("event_rate_hz", "reference event density"),
        ("interarrival_mean_seconds", "reference event cadence"),
        ("spread_q95_pips", "reference spread q95"),
        ("stale_run_fraction", "reference stale-run fraction"),
        ("timestamp_quantum_ms", "reference timestamp quantum"),
    )
    for window_id, observations in _trace_groups_by_window(context).items():
        window = windows.get(window_id)
        if window is None:
            raise ValueError("diagnostic trace window is absent from corpus")
        label = (
            f"{window.period}:{window.session}:{window.split_kind}:"
            f"{window.window_id[-8:]}"
        )
        strata = {
            "context_state": window.context_state,
            "context_supported": str(window.context_supported).lower(),
            "epoch_label": window.epoch_label,
            "session": window.session,
            "split_kind": window.split_kind,
            "window_id": window.window_id,
        }
        for metric, series in metric_specs:
            value = _trace_metric_median(
                observations, group="reference", metric=metric
            )
            if value is not None:
                points.append(
                    _datum(
                        x=label,
                        y=value,
                        series=series,
                        sources=(trace_source, corpus_source),
                        strata=strata,
                    )
                )
    return _chart(
        family=DiagnosticFamily.FEED_EPOCH_OBSERVATION,
        view_id="observation_profile",
        title="Feed-epoch observation profile",
        caption=(
            "Retained HistData density, cadence, spread, stale-run, precision, "
            "and boundary evidence by feed epoch/session/context. Native metric "
            "units remain named by series; these are observation regimes, not "
            "market regimes."
        ),
        mark=DiagnosticMark.LINE,
        x_label="boundary or protected HistData window",
        y_label="retained observation metric",
        y_unit="native metric units",
        status=(
            DiagnosticStatus.AVAILABLE if points else DiagnosticStatus.EMPTY
        ),
        reason_codes=() if points else ("feed_epoch_boundaries_empty",),
        sources=(feed_source, trace_source, corpus_source),
        points=points,
        maximum=maximum,
    )


def _quality_constraint_chart(
    context: _EvidenceContext, maximum: int
) -> DiagnosticChartDataV1:
    trace_source = context.source_by_key["trace"]
    corpus_source = context.source_by_key["corpus"]
    windows = {item.window_id: item for item in context.corpus.windows}
    metric_specs = (
        ("event_count_relative_error", "event-density error"),
        ("interarrival_quantile_relative_error", "gap/cadence error"),
        ("spread_tail_relative_error", "spread-tail error"),
        ("stale_run_relative_error", "stale-run error"),
        ("burst_quiet_rate_error", "burst/session-state error"),
        ("unsupported_context_emission_count", "unsupported-context emissions"),
    )
    points: list[DiagnosticChartDatumV1] = []
    for window_id, observations in _trace_groups_by_window(context).items():
        window = windows.get(window_id)
        if window is None:
            raise ValueError("diagnostic trace window is absent from corpus")
        label = (
            f"{window.period}:{window.session}:{window.split_kind}:"
            f"{window.window_id[-8:]}"
        )
        strata = {
            "context_state": window.context_state,
            "epoch_label": window.epoch_label,
            "session": window.session,
            "split_kind": window.split_kind,
            "window_id": window.window_id,
        }
        for metric, series in metric_specs:
            value = _trace_metric_median(
                observations, group="comparison", metric=metric
            )
            if value is not None:
                points.append(
                    _datum(
                        x=label,
                        y=value,
                        series=series,
                        sources=(trace_source, corpus_source),
                        strata=strata,
                    )
                )
    return _chart(
        family=DiagnosticFamily.QUALITY_CONSTRAINT_TIMELINE,
        view_id="window_quality",
        title="Protected-window quality and constraint timeline",
        caption=(
            "Portfolio-median gap, event, spread, stale, burst/session, and "
            "unsupported-context evidence by protected HistData window. The "
            "aggregation is a bounded audit view, not an engine ranking."
        ),
        mark=DiagnosticMark.LINE,
        x_label="period, session, and protected split",
        y_label="quality or constraint statistic",
        y_unit="relative error or count",
        status=(DiagnosticStatus.LIMITED if points else DiagnosticStatus.EMPTY),
        reason_codes=(
            ("portfolio_median_projection_across_candidate_members",)
            if points
            else ("quality_constraint_evidence_empty",)
        ),
        sources=(trace_source, corpus_source),
        points=points,
        maximum=maximum,
    )


def _observation_operator_charts(
    context: _EvidenceContext, maximum: int
) -> tuple[DiagnosticChartDataV1, DiagnosticChartDataV1]:
    source = context.source_by_key["observation_campaign"]
    trace_source = context.source_by_key["trace"]
    probability_points: list[DiagnosticChartDatumV1] = []
    comparison_points: list[DiagnosticChartDatumV1] = []
    limited = False
    for raw in _sequence(
        context.observation_campaign.get("targets", []),
        "observation calibration targets",
    ):
        target = _mapping(raw, "observation target")
        label = f"{target.get('epoch_label', '')}:{target.get('symbol', '')}"
        values = _mapping(
            target.get("parameter_values", {}), "parameter values"
        )
        lowers = _mapping(
            target.get("parameter_lower_bounds", {}), "parameter lower bounds"
        )
        uppers = _mapping(
            target.get("parameter_upper_bounds", {}), "parameter upper bounds"
        )
        statuses = _mapping(
            target.get("parameter_status", {}), "parameter status"
        )
        limited = limited or any(
            str(value) in {"bounded", "unsupported"}
            for value in statuses.values()
        )
        for parameter in (
            "retention_probability",
            "duplicate_probability",
            "unchanged_retention_probability",
        ):
            if parameter not in values:
                continue
            probability_points.append(
                _datum(
                    x=label,
                    y=_finite_number(values[parameter], parameter),
                    lower=_finite_number(lowers[parameter], parameter),
                    upper=_finite_number(uppers[parameter], parameter),
                    series=parameter,
                    sources=(source,),
                    strata={
                        "parameter_status": str(statuses.get(parameter, ""))
                    },
                )
            )
    comparison_specs = (
        ("event_count_relative_error", "reconstructed event-density error"),
        (
            "interarrival_quantile_relative_error",
            "reconstructed cadence error",
        ),
        ("spread_tail_relative_error", "reconstructed spread-tail error"),
        ("stale_run_relative_error", "reconstructed stale-run error"),
        (
            "path_realized_variation_relative_error",
            "reconstructed path-variation error",
        ),
        ("update_type_proportion_l1", "reconstructed update-mark error"),
    )
    engine_by_method = {
        item.method_name: item.engine_id
        for item in context.evaluation.engine_evidence
    }
    for (method, split), observations in _trace_groups_by_method_split(
        context
    ).items():
        engine_id = engine_by_method.get(method)
        if engine_id is None:
            continue
        for metric, series in comparison_specs:
            value = _trace_metric_median(
                observations, group="comparison", metric=metric
            )
            if value is not None:
                comparison_points.append(
                    _datum(
                        x=_short_engine(engine_id),
                        y=value,
                        series=f"{split} {series}",
                        sources=(trace_source,),
                        strata={"method_name": method, "split_kind": split},
                    )
                )
    probability_status = (
        DiagnosticStatus.LIMITED
        if probability_points and limited
        else (
            DiagnosticStatus.AVAILABLE
            if probability_points
            else DiagnosticStatus.EMPTY
        )
    )
    probability_reasons = (
        ("operator_contains_explicitly_bounded_or_unsupported_mechanisms",)
        if probability_points and limited
        else (
            ()
            if probability_points
            else ("observation_operator_targets_empty",)
        )
    )
    return (
        _chart(
            family=DiagnosticFamily.OBSERVATION_OPERATOR_RECONSTRUCTION,
            view_id="calibrated_parameters",
            title="Observation-operator probability reconstruction",
            caption=(
                "Epoch/symbol observation-operator probabilities with empirical "
                "bounds. Unsupported mechanisms are not imputed as zeros."
            ),
            mark=DiagnosticMark.INTERVAL,
            x_label="feed epoch and symbol",
            y_label="estimated probability",
            y_unit="fraction",
            status=probability_status,
            reason_codes=probability_reasons,
            sources=(source,),
            points=probability_points,
            maximum=maximum,
        ),
        _chart(
            family=DiagnosticFamily.OBSERVATION_OPERATOR_RECONSTRUCTION,
            view_id="reconstruction_errors",
            title="Observation-operator reconstruction errors",
            caption=(
                "Portfolio row-free reference/candidate comparison errors by "
                "engine and protected split; these views do not rank engines."
            ),
            mark=DiagnosticMark.SCATTER,
            x_label="proposal engine",
            y_label="reconstruction error",
            y_unit="relative error or L1 distance",
            status=(
                DiagnosticStatus.LIMITED
                if comparison_points and limited
                else (
                    DiagnosticStatus.AVAILABLE
                    if comparison_points
                    else DiagnosticStatus.EMPTY
                )
            ),
            reason_codes=(
                ("operator_includes_bounded_or_unsupported_mechanisms",)
                if comparison_points and limited
                else (
                    ()
                    if comparison_points
                    else ("observation_operator_comparisons_empty",)
                )
            ),
            sources=(trace_source,),
            points=comparison_points,
            maximum=maximum,
        ),
    )


def _point_process_residual_chart(
    context: _EvidenceContext, maximum: int
) -> DiagnosticChartDataV1:
    source = context.source_by_key["qualification"]
    points: list[DiagnosticChartDatumV1] = []
    for report in context.dossier.residual_reports:
        label = _short_engine(report.engine_id)
        for series, value in (
            ("time uniform KS", report.time_uniform_ks),
            (
                "absolute time lag-1",
                _abs_optional(report.time_lag1_autocorrelation),
            ),
            ("mark uniform KS", report.mark_uniform_ks),
        ):
            if value is not None:
                points.append(
                    _datum(
                        x=label,
                        y=value,
                        series=f"{report.split_kind} {series}",
                        sources=(source,),
                        strata={"qualification_status": report.status.value},
                    )
                )
    return _chart(
        family=DiagnosticFamily.POINT_PROCESS_RESIDUAL,
        view_id="residual_summary",
        title="Point-process residual diagnostics",
        caption=(
            "Row-free time-rescaling and mark residual summaries by engine and "
            "protected split. Lower values are diagnostic summaries, not rankings."
        ),
        mark=DiagnosticMark.SCATTER,
        x_label="proposal engine",
        y_label="residual statistic",
        y_unit="fraction",
        status=(
            DiagnosticStatus.AVAILABLE if points else DiagnosticStatus.EMPTY
        ),
        reason_codes=() if points else ("residual_reports_empty",),
        sources=(source,),
        points=points,
        maximum=maximum,
    )


def _mark_refusal_charts(
    context: _EvidenceContext, maximum: int
) -> tuple[DiagnosticChartDataV1, DiagnosticChartDataV1]:
    qualification = context.source_by_key["qualification"]
    evaluation = context.source_by_key["evaluation"]
    trace = context.source_by_key["trace"]
    calibration_points: list[DiagnosticChartDatumV1] = []
    dynamics_points: list[DiagnosticChartDatumV1] = []
    for report in context.dossier.residual_reports:
        if report.mark_uniform_p_value is not None:
            calibration_points.append(
                _datum(
                    x=_short_engine(report.engine_id),
                    y=report.mark_uniform_p_value,
                    series=f"{report.split_kind} mark PIT p-value",
                    sources=(qualification,),
                    strata={"qualification_status": report.status.value},
                )
            )
    engine_by_method = {
        item.method_name: item.engine_id
        for item in context.evaluation.engine_evidence
    }
    for (method, split), observations in _trace_groups_by_method_split(
        context
    ).items():
        engine_id = engine_by_method.get(method)
        if engine_id is None:
            continue
        for metric, series in (
            ("simulation_mark_pit_ks", "window mark PIT KS"),
            ("update_type_proportion_l1", "update-type proportion L1"),
            ("update_transition_l1", "update-transition L1"),
        ):
            value = _trace_metric_median(
                observations, group="comparison", metric=metric
            )
            if value is not None:
                dynamics_points.append(
                    _datum(
                        x=_short_engine(engine_id),
                        y=value,
                        series=f"{split} {series}",
                        sources=(trace,),
                        strata={"method_name": method, "split_kind": split},
                    )
                )
    for evidence in context.evaluation.engine_evidence:
        calibration_points.append(
            _datum(
                x=_short_engine(evidence.engine_id),
                y=float(evidence.refusal_count > 0),
                series="execution refusal indicator",
                sources=(evaluation,),
                annotation=(
                    f"{evidence.refusal_count} refusals observed"
                    if evidence.refusal_count
                    else "no refusal observed"
                ),
            )
        )
    return (
        _chart(
            family=DiagnosticFamily.MARK_REFUSAL_CALIBRATION,
            view_id="global_mark_refusal",
            title="Global mark calibration and explicit refusal evidence",
            caption=(
                "Global mark PIT and retained execution-refusal evidence by "
                "engine; the series are not combined into an eligibility decision."
            ),
            mark=DiagnosticMark.BAR,
            x_label="proposal engine",
            y_label="calibration or refusal value",
            y_unit="fraction or binary indicator",
            status=(
                DiagnosticStatus.AVAILABLE
                if calibration_points
                else DiagnosticStatus.EMPTY
            ),
            reason_codes=(
                ()
                if calibration_points
                else ("mark_and_refusal_evidence_empty",)
            ),
            sources=(qualification, evaluation),
            points=calibration_points,
            maximum=maximum,
        ),
        _chart(
            family=DiagnosticFamily.MARK_REFUSAL_CALIBRATION,
            view_id="window_mark_dynamics",
            title="Window-level mark and update dynamics",
            caption=(
                "Portfolio-median mark PIT, update-type, and update-transition "
                "errors by engine and protected split."
            ),
            mark=DiagnosticMark.SCATTER,
            x_label="proposal engine",
            y_label="mark or update error",
            y_unit="KS or L1 distance",
            status=(
                DiagnosticStatus.AVAILABLE
                if dynamics_points
                else DiagnosticStatus.EMPTY
            ),
            reason_codes=(
                () if dynamics_points else ("window_mark_dynamics_empty",)
            ),
            sources=(trace,),
            points=dynamics_points,
            maximum=maximum,
        ),
    )


def _proper_score_power_charts(
    context: _EvidenceContext, maximum: int
) -> tuple[
    DiagnosticChartDataV1,
    DiagnosticChartDataV1,
    DiagnosticChartDataV1,
    DiagnosticChartDataV1,
]:
    source = context.source_by_key["qualification"]
    distance_points: list[DiagnosticChartDatumV1] = []
    marginal_points: list[DiagnosticChartDatumV1] = []
    calibration_points: list[DiagnosticChartDatumV1] = []
    power_points: list[DiagnosticChartDatumV1] = []
    for report in context.dossier.score_reports:
        label = _short_engine(report.engine_id)
        for series, value in (
            ("energy score", report.energy_score),
            ("variogram score p=0.5", report.variogram_score_p05),
            ("variogram score p=1", report.variogram_score_p1),
        ):
            if value is not None:
                distance_points.append(
                    _datum(
                        x=label,
                        y=value,
                        series=f"{report.split_kind} {series}",
                        sources=(source,),
                        strata={"qualification_status": report.status.value},
                    )
                )
        for series, value in (
            ("marginal CRPS", report.marginal_crps),
            ("tail error", report.tail_error),
            ("path error", report.path_error),
        ):
            if value is not None:
                marginal_points.append(
                    _datum(
                        x=label,
                        y=value,
                        series=f"{report.split_kind} {series}",
                        sources=(source,),
                        strata={"qualification_status": report.status.value},
                    )
                )
        for series, value in (
            ("nominal coverage", report.nominal_coverage),
            ("empirical coverage", report.empirical_coverage),
            ("calibration error", report.calibration_error),
            ("sharpness", report.sharpness),
        ):
            if value is not None:
                calibration_points.append(
                    _datum(
                        x=label,
                        y=value,
                        series=f"{report.split_kind} {series}",
                        sources=(source,),
                        strata={"qualification_status": report.status.value},
                    )
                )
    for result in context.dossier.power_study.results:
        strata = {
            "gate_status": result.status.value,
            "misspecification_family": result.misspecification_family,
        }
        for sample_size, power in sorted(
            result.power_by_sample_size.items(), key=lambda item: int(item[0])
        ):
            power_points.append(
                _datum(
                    x=f"power {sample_size}",
                    y=power,
                    series=result.gate_id,
                    sources=(source,),
                    strata=strata,
                )
            )
        for sample_size, false_positive in sorted(
            result.false_positive_by_sample_size.items(),
            key=lambda item: int(item[0]),
        ):
            power_points.append(
                _datum(
                    x=f"false positive {sample_size}",
                    y=false_positive,
                    series=result.gate_id,
                    sources=(source,),
                    strata=strata,
                )
            )
    reliable = context.dossier.power_study.reliable
    power_status = (
        DiagnosticStatus.AVAILABLE
        if power_points and reliable
        else (
            DiagnosticStatus.UNDERPOWERED
            if power_points
            else DiagnosticStatus.EMPTY
        )
    )
    power_reasons = (
        ()
        if power_points and reliable
        else (
            ("one_or_more_qualification_gates_underpowered",)
            if power_points
            else ("power_study_empty",)
        )
    )
    return (
        _chart(
            family=DiagnosticFamily.PROPER_SCORE_POWER,
            view_id="energy_variogram_scores",
            title="Energy and variogram proper-score evidence",
            caption=(
                "Energy and variogram scores by engine and protected split; "
                "native score scales remain explicit."
            ),
            mark=DiagnosticMark.SCATTER,
            x_label="proposal engine",
            y_label="retained predictive score",
            y_unit="native score",
            status=(
                DiagnosticStatus.AVAILABLE
                if distance_points
                else DiagnosticStatus.EMPTY
            ),
            reason_codes=() if distance_points else ("distance_scores_empty",),
            sources=(source,),
            points=distance_points,
            maximum=maximum,
        ),
        _chart(
            family=DiagnosticFamily.PROPER_SCORE_POWER,
            view_id="marginal_tail_path_scores",
            title="Marginal CRPS, tail, and path evidence",
            caption=(
                "Marginal CRPS, tail, and path scores by engine and protected "
                "split, separated from larger-scale energy/variogram scores."
            ),
            mark=DiagnosticMark.SCATTER,
            x_label="proposal engine",
            y_label="retained predictive score",
            y_unit="native score",
            status=(
                DiagnosticStatus.AVAILABLE
                if marginal_points
                else DiagnosticStatus.EMPTY
            ),
            reason_codes=(
                () if marginal_points else ("marginal_tail_path_scores_empty",)
            ),
            sources=(source,),
            points=marginal_points,
            maximum=maximum,
        ),
        _chart(
            family=DiagnosticFamily.PROPER_SCORE_POWER,
            view_id="coverage_calibration_sharpness",
            title="Coverage, calibration, and sharpness evidence",
            caption=(
                "Nominal/empirical coverage, calibration error, and predictive "
                "sharpness by engine and protected split."
            ),
            mark=DiagnosticMark.SCATTER,
            x_label="proposal engine",
            y_label="coverage, error, or sharpness",
            y_unit="fraction or native sharpness",
            status=(
                DiagnosticStatus.AVAILABLE
                if calibration_points
                else DiagnosticStatus.EMPTY
            ),
            reason_codes=(
                () if calibration_points else ("calibration_scores_empty",)
            ),
            sources=(source,),
            points=calibration_points,
            maximum=maximum,
        ),
        _chart(
            family=DiagnosticFamily.PROPER_SCORE_POWER,
            view_id="power_regions",
            title="Qualification-gate finite-sample power regions",
            caption=(
                "False-positive and power regions by predeclared gate and sample "
                "size under named misspecifications. Underpowered gates remain "
                "visibly underpowered."
            ),
            mark=DiagnosticMark.LINE,
            x_label="region and protected-window sample size",
            y_label="observed false-positive rate or power",
            y_unit="fraction",
            status=power_status,
            reason_codes=power_reasons,
            sources=(source,),
            points=power_points,
            maximum=maximum,
        ),
    )


def _engine_portfolio_charts(
    context: _EvidenceContext, maximum: int
) -> tuple[DiagnosticChartDataV1, DiagnosticChartDataV1]:
    source = context.source_by_key["qualification"]
    trace_source = context.source_by_key["trace"]
    calibration = context.dossier.portfolio_calibration
    weight_points: list[DiagnosticChartDatumV1] = [
        _datum(
            x=_short_engine(engine_id),
            y=weight,
            series="frozen validation weight",
            sources=(source,),
            strata={"calibration_status": calibration.status.value},
        )
        for engine_id, weight in calibration.weights.items()
    ]
    diagnostic_points: list[DiagnosticChartDatumV1] = []
    engine_by_method = {
        item.method_name: item.engine_id
        for item in context.evaluation.engine_evidence
    }
    for (method, split), observations in _trace_groups_by_method_split(
        context
    ).items():
        engine_id = engine_by_method.get(method)
        if engine_id is None:
            continue
        for metric, series in (
            ("event_count_relative_error", "event-density error"),
            ("path_realized_variation_relative_error", "path-variation error"),
            ("spread_tail_relative_error", "spread-tail error"),
        ):
            value = _trace_metric_median(
                observations, group="comparison", metric=metric
            )
            if value is not None:
                diagnostic_points.append(
                    _datum(
                        x=_short_engine(engine_id),
                        y=value,
                        series=f"{split} {series}",
                        sources=(trace_source,),
                        strata={"method_name": method, "split_kind": split},
                    )
                )
    for report in context.dossier.score_reports:
        label = _short_engine(report.engine_id)
        for series, value in (
            ("tail score", report.tail_error),
            ("predictive sharpness", report.sharpness),
        ):
            if value is not None:
                diagnostic_points.append(
                    _datum(
                        x=label,
                        y=value,
                        series=f"{report.split_kind} {series}",
                        sources=(source,),
                        strata={"qualification_status": report.status.value},
                    )
                )
    weight_status = _diagnostic_status(
        calibration.status, has_points=bool(weight_points)
    )
    diagnostic_status = _diagnostic_status(
        calibration.status, has_points=bool(diagnostic_points)
    )
    return (
        _chart(
            family=DiagnosticFamily.ENGINE_PORTFOLIO_DISTRIBUTION,
            view_id="frozen_weights",
            title="Frozen engine-portfolio distribution",
            caption=(
                "Validation-fitted weights frozen before one final-holdout "
                "evaluation. Weights are not an automatic model winner."
            ),
            mark=DiagnosticMark.BAR,
            x_label="proposal engine",
            y_label="portfolio weight",
            y_unit="fraction",
            status=weight_status,
            reason_codes=(
                ()
                if weight_status is DiagnosticStatus.AVAILABLE
                else (
                    tuple(calibration.reason_codes)
                    if weight_points
                    else ("portfolio_calibration_empty",)
                )
            ),
            sources=(source,),
            points=weight_points,
            maximum=maximum,
        ),
        _chart(
            family=DiagnosticFamily.ENGINE_PORTFOLIO_DISTRIBUTION,
            view_id="engine_error_profiles",
            title="Per-engine event, path, spread, tail, and uncertainty evidence",
            caption=(
                "Retained portfolio component errors and predictive sharpness by "
                "engine and protected split; the view does not select a winner."
            ),
            mark=DiagnosticMark.BAR,
            x_label="proposal engine",
            y_label="retained error or uncertainty statistic",
            y_unit="relative error or native score",
            status=diagnostic_status,
            reason_codes=(
                ()
                if diagnostic_status is DiagnosticStatus.AVAILABLE
                else (
                    tuple(calibration.reason_codes)
                    if diagnostic_points
                    else ("engine_diagnostic_profiles_empty",)
                )
            ),
            sources=(source, trace_source),
            points=diagnostic_points,
            maximum=maximum,
        ),
    )


def _carving_flow_chart(
    context: _EvidenceContext, maximum: int
) -> DiagnosticChartDataV1:
    selected = _optional_by_schema(
        context,
        {
            "histdatacom.reconstruction-plan-execution-manifest.v1",
            "histdatacom.reconstruction-certification-campaign-result.v1",
        },
    )
    if not selected:
        return _unavailable_chart(
            context,
            family=DiagnosticFamily.CARVING_DECISION_FLOW,
            view_id="decision_flow",
            title="Carving decision flow",
            caption=(
                "Carving execution status requires a retained plan execution or "
                "certification campaign result."
            ),
            reason="carving_execution_manifest_not_supplied",
        )
    sources = tuple(item[0] for item in selected)
    counts: Counter[str] = Counter()
    for _, payload in selected:
        _collect_status_values(payload, counts)
        refusals = payload.get("refusal_ids")
        if isinstance(refusals, list):
            counts["explicit_refusal"] += len(refusals)
        for key in ("planned_window_count", "executable_window_count"):
            value = payload.get(key)
            if type(value) is int:
                counts[key] += value
    points = [
        _datum(
            x=label,
            y=float(count),
            series="retained flow count",
            sources=sources,
        )
        for label, count in sorted(counts.items())
    ]
    return _chart(
        family=DiagnosticFamily.CARVING_DECISION_FLOW,
        view_id="decision_flow",
        title="Carving decision flow",
        caption=(
            "Retained planning, execution, status, and refusal counts from the "
            "supplied HistData reconstruction manifests."
        ),
        mark=DiagnosticMark.BAR,
        x_label="decision or execution state",
        y_label="retained count",
        y_unit="count",
        status=DiagnosticStatus.LIMITED if points else DiagnosticStatus.EMPTY,
        reason_codes=(
            ("summary_projection_of_optional_execution_manifests",)
            if points
            else ("carving_execution_summary_empty",)
        ),
        sources=sources,
        points=points,
        maximum=maximum,
    )


def _cross_series_charts(
    context: _EvidenceContext, maximum: int
) -> tuple[DiagnosticChartDataV1, DiagnosticChartDataV1]:
    source = context.source_by_key["qualification"]
    trace_source = context.source_by_key["trace"]
    score_points: list[DiagnosticChartDatumV1] = [
        _datum(
            x=_short_engine(report.engine_id),
            y=report.cross_series_error,
            series=f"{report.split_kind} cross-series predictive error",
            sources=(source,),
            strata={"qualification_status": report.status.value},
        )
        for report in context.dossier.score_reports
        if report.cross_series_error is not None
    ]
    triangle_points: list[DiagnosticChartDatumV1] = []
    engine_by_method = {
        item.method_name: item.engine_id
        for item in context.evaluation.engine_evidence
    }
    for (method, split), observations in _trace_groups_by_method_split(
        context
    ).items():
        engine_id = engine_by_method.get(method)
        if engine_id is None:
            continue
        for group, metric, series in (
            (
                "candidate",
                "triangle_residual_p99_pips",
                "candidate triangle residual p99",
            ),
            (
                "comparison",
                "triangle_synchronization_error",
                "triangle synchronization error",
            ),
        ):
            value = _trace_metric_median(
                observations, group=group, metric=metric
            )
            if value is not None:
                triangle_points.append(
                    _datum(
                        x=_short_engine(engine_id),
                        y=value,
                        series=f"{split} {series}",
                        sources=(trace_source,),
                        strata={"method_name": method, "split_kind": split},
                    )
                )
    limitation = (
        "limiting_leg_staleness_and_contradiction_breakdown_not_retained",
    )
    return (
        _chart(
            family=DiagnosticFamily.CROSS_SERIES_RECONCILIATION,
            view_id="predictive_scores",
            title="Synchronized cross-series predictive scores",
            caption=(
                "Protected-split cross-series predictive scores from synchronized "
                "HistData windows. Lower error alone is not an eligibility gate."
            ),
            mark=DiagnosticMark.SCATTER,
            x_label="proposal engine",
            y_label="cross-series predictive error",
            y_unit="native score",
            status=(
                DiagnosticStatus.LIMITED
                if score_points
                else DiagnosticStatus.EMPTY
            ),
            reason_codes=limitation if score_points else ("scores_empty",),
            sources=(source,),
            points=score_points,
            maximum=maximum,
        ),
        _chart(
            family=DiagnosticFamily.CROSS_SERIES_RECONCILIATION,
            view_id="triangle_reconciliation",
            title="Triangle residual and synchronization evidence",
            caption=(
                "Protected-split triangle residual and synchronization evidence. "
                "Limiting-leg, staleness, and contradiction breakdowns are not "
                "retained by #490."
            ),
            mark=DiagnosticMark.SCATTER,
            x_label="proposal engine",
            y_label="triangle reconciliation statistic",
            y_unit="pips or fraction",
            status=(
                DiagnosticStatus.LIMITED
                if triangle_points
                else DiagnosticStatus.EMPTY
            ),
            reason_codes=(
                limitation if triangle_points else ("triangle_evidence_empty",)
            ),
            sources=(trace_source,),
            points=triangle_points,
            maximum=maximum,
        ),
    )


def _protected_split_chart(
    context: _EvidenceContext, maximum: int
) -> DiagnosticChartDataV1:
    corpus_source = context.source_by_key["corpus"]
    experiment_source = context.source_by_key["experiment"]
    split_counts = Counter(item.split_kind for item in context.corpus.windows)
    points = [
        _datum(
            x=split,
            y=float(count),
            series="synchronized windows",
            sources=(corpus_source,),
        )
        for split, count in sorted(split_counts.items())
    ]
    audit = context.experiment.leakage_audit
    for label, count in (
        ("overlap violations", audit.overlap_count),
        ("neighbor-guard violations", audit.neighbor_guard_violation_count),
        ("shared partitions", audit.shared_partition_count),
        ("shared cohesion groups", audit.shared_cohesion_group_count),
    ):
        points.append(
            _datum(
                x=label,
                y=float(count),
                series="leakage audit findings",
                sources=(experiment_source,),
            )
        )
    return _chart(
        family=DiagnosticFamily.PROTECTED_SPLIT_LEAKAGE,
        view_id="split_leakage",
        title="Protected-split coverage and leakage audit",
        caption=(
            "Synchronized window counts and explicit overlap/cohesion findings. "
            "Zero findings are recorded audit results, not missing evidence."
        ),
        mark=DiagnosticMark.BAR,
        x_label="split or audit finding",
        y_label="retained count",
        y_unit="count",
        status=(
            DiagnosticStatus.AVAILABLE
            if audit.accepted and not context.evidence_limitations
            else DiagnosticStatus.LIMITED
        ),
        reason_codes=(
            context.evidence_limitations
            if audit.accepted
            else ("leakage_audit_not_accepted", *context.evidence_limitations)
        ),
        sources=(corpus_source, experiment_source),
        points=points,
        maximum=maximum,
    )


def _product_origin_chart(
    context: _EvidenceContext, maximum: int
) -> DiagnosticChartDataV1:
    selected = _optional_by_schema(
        context, {"histdatacom.reconstruction-product.v2"}
    )
    if not selected:
        return _unavailable_chart(
            context,
            family=DiagnosticFamily.PRODUCT_ORIGIN_LINEAGE,
            view_id="origin_lineage",
            title="Product origin and lineage composition",
            caption=(
                "Observed-versus-synthetic origin composition requires retained "
                "reconstruction-product manifests."
            ),
            reason="reconstruction_product_manifest_not_supplied",
        )
    points: list[DiagnosticChartDatumV1] = []
    sources = tuple(item[0] for item in selected)
    for index, (source, payload) in enumerate(selected, start=1):
        label = _payload_label(payload, fallback=f"product-{index:02d}")
        origin_counts = payload.get("origin_counts", {})
        if isinstance(origin_counts, Mapping):
            for origin, count in sorted(origin_counts.items()):
                if type(count) in {int, float}:
                    points.append(
                        _datum(
                            x=label,
                            y=float(count),
                            series=str(origin),
                            sources=(source,),
                        )
                    )
        for key, series in (
            ("observed_event_count", "observed"),
            ("synthetic_event_count", "synthetic"),
            ("output_event_count", "output total"),
        ):
            value = payload.get(key)
            if type(value) in {int, float}:
                points.append(
                    _datum(
                        x=label,
                        y=_finite_number(value, key),
                        series=series,
                        sources=(source,),
                    )
                )
    return _chart(
        family=DiagnosticFamily.PRODUCT_ORIGIN_LINEAGE,
        view_id="origin_lineage",
        title="Product origin and lineage composition",
        caption=(
            "Manifest-level observed, synthetic, and output counts. No event "
            "rows or local storage paths are embedded."
        ),
        mark=DiagnosticMark.BAR,
        x_label="reconstruction product",
        y_label="event count",
        y_unit="count",
        status=(
            DiagnosticStatus.AVAILABLE if points else DiagnosticStatus.EMPTY
        ),
        reason_codes=() if points else ("product_origin_counts_empty",),
        sources=sources,
        points=points,
        maximum=maximum,
    )


def _bar_strategy_chart(
    context: _EvidenceContext, maximum: int
) -> DiagnosticChartDataV1:
    selected = _optional_by_schema(
        context,
        {
            "histdatacom.derived-bar-product.v1",
            "histdatacom.strategy-sensitivity-report.v1",
        },
    )
    if not selected:
        return _unavailable_chart(
            context,
            family=DiagnosticFamily.BAR_STRATEGY_SENSITIVITY,
            view_id="bar_strategy",
            title="Derived-bar and strategy sensitivity",
            caption=(
                "Bar and strategy diagnostics require retained derived-bar and "
                "strategy-sensitivity manifests."
            ),
            reason="bar_and_strategy_manifests_not_supplied",
        )
    points: list[DiagnosticChartDatumV1] = []
    sources = tuple(item[0] for item in selected)
    has_strategy = False
    has_bars = False
    for index, (source, payload) in enumerate(selected, start=1):
        schema = str(payload.get("schema_version", ""))
        if schema == "histdatacom.derived-bar-product.v1":
            has_bars = True
            counts = payload.get("symbol_bar_counts", {})
            if isinstance(counts, Mapping):
                for symbol, count in sorted(counts.items()):
                    if type(count) is int:
                        points.append(
                            _datum(
                                x=str(symbol),
                                y=float(count),
                                series="derived bars",
                                sources=(source,),
                            )
                        )
            elif type(payload.get("bar_count")) is int:
                points.append(
                    _datum(
                        x=f"bar-product-{index:02d}",
                        y=float(cast(int, payload["bar_count"])),
                        series="derived bars",
                        sources=(source,),
                    )
                )
        elif schema == "histdatacom.strategy-sensitivity-report.v1":
            has_strategy = True
            for summary in _sequence(
                payload.get("uncertainty_summaries", []),
                "strategy uncertainty summaries",
            ):
                item = _mapping(summary, "strategy uncertainty summary")
                value = item.get("mean_net_response_bps")
                if type(value) in {int, float}:
                    points.append(
                        _datum(
                            x=(
                                f"{item.get('symbol', '')}:"
                                f"{item.get('session', '')}:"
                                f"{item.get('horizon_ns', '')}"
                            ),
                            y=_finite_number(value, "mean strategy response"),
                            lower=_optional_plain_float(
                                item.get("min_net_response_bps")
                            ),
                            upper=_optional_plain_float(
                                item.get("max_net_response_bps")
                            ),
                            series="strategy response uncertainty",
                            sources=(source,),
                        )
                    )
    complete = has_bars and has_strategy
    return _chart(
        family=DiagnosticFamily.BAR_STRATEGY_SENSITIVITY,
        view_id="bar_strategy",
        title="Derived-bar and strategy sensitivity",
        caption=(
            "Manifest-level bar coverage and strategy uncertainty summaries. "
            "The report is not a backtest, profit claim, or recommendation."
        ),
        mark=DiagnosticMark.INTERVAL,
        x_label="symbol/session/horizon or bar product",
        y_label="manifest diagnostic value",
        y_unit="mixed; see series",
        status=(
            DiagnosticStatus.AVAILABLE
            if points and complete
            else DiagnosticStatus.LIMITED if points else DiagnosticStatus.EMPTY
        ),
        reason_codes=(
            ()
            if points and complete
            else (
                ("bar_or_strategy_companion_manifest_missing",)
                if points
                else ("bar_strategy_summaries_empty",)
            )
        ),
        sources=sources,
        points=points,
        maximum=maximum,
    )


def _chart(
    *,
    family: DiagnosticFamily,
    view_id: str,
    title: str,
    caption: str,
    mark: DiagnosticMark,
    x_label: str,
    y_label: str,
    y_unit: str,
    status: DiagnosticStatus,
    reason_codes: Sequence[str],
    sources: Sequence[DiagnosticSourceV1],
    points: Sequence[DiagnosticChartDatumV1],
    maximum: int,
) -> DiagnosticChartDataV1:
    original_count = len(points)
    retained = _sample_datums(points, maximum)
    return DiagnosticChartDataV1(
        family=family,
        view_id=view_id,
        title=title,
        caption=caption,
        mark=mark,
        x_label=x_label,
        y_label=y_label,
        y_unit=y_unit,
        status=status,
        reason_codes=tuple(reason_codes),
        source_ids=tuple(item.source_id for item in sources),
        points=retained,
        original_point_count=original_count,
    )


def _unavailable_chart(
    context: _EvidenceContext,
    *,
    family: DiagnosticFamily,
    view_id: str,
    title: str,
    caption: str,
    reason: str,
) -> DiagnosticChartDataV1:
    source = context.source_by_key["qualification"]
    return _chart(
        family=family,
        view_id=view_id,
        title=title,
        caption=caption,
        mark=DiagnosticMark.BAR,
        x_label="evidence family",
        y_label="retained value",
        y_unit="",
        status=DiagnosticStatus.UNAVAILABLE,
        reason_codes=(reason,),
        sources=(source,),
        points=(),
        maximum=1,
    )


def _datum(
    *,
    x: str,
    y: float | None,
    series: str,
    sources: Sequence[DiagnosticSourceV1],
    lower: float | None = None,
    upper: float | None = None,
    strata: Mapping[str, str] | None = None,
    annotation: str = "",
) -> DiagnosticChartDatumV1:
    return DiagnosticChartDatumV1(
        x=x,
        y=y,
        lower=lower,
        upper=upper,
        series=series,
        strata=strata or {},
        annotation=annotation,
        source_ids=tuple(item.source_id for item in sources),
    )


def _sample_datums(
    points: Sequence[DiagnosticChartDatumV1], maximum: int
) -> tuple[DiagnosticChartDatumV1, ...]:
    bound = _bounded_int(
        maximum, 1, MAX_DIAGNOSTIC_POINTS_PER_CHART, "sample bound"
    )
    selected = tuple(points)
    if len(selected) <= bound:
        return selected
    return tuple(
        sorted(
            selected,
            key=lambda item: hashlib.sha256(
                item.datum_id.encode("utf-8")
            ).hexdigest(),
        )[:bound]
    )


def _optional_by_schema(
    context: _EvidenceContext, schemas: set[str]
) -> tuple[tuple[DiagnosticSourceV1, Mapping[str, Any]], ...]:
    return tuple(
        item
        for item in context.optional_payloads
        if str(item[1].get("schema_version", "")) in schemas
    )


def _collect_status_values(value: Any, counts: Counter[str]) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key).endswith("status") and isinstance(item, str):
                counts[item] += 1
            else:
                _collect_status_values(item, counts)
    elif isinstance(value, list):
        for item in value:
            _collect_status_values(item, counts)


def _diagnostic_status(
    status: QualificationStatus, *, has_points: bool
) -> DiagnosticStatus:
    if not has_points:
        return DiagnosticStatus.EMPTY
    if status is QualificationStatus.INSUFFICIENT_EVIDENCE:
        return DiagnosticStatus.UNDERPOWERED
    if status is QualificationStatus.REFUSED:
        return DiagnosticStatus.REFUSED
    if status is QualificationStatus.FAILED:
        return DiagnosticStatus.LIMITED
    return DiagnosticStatus.AVAILABLE


def _family_status_counts(
    charts: Sequence[DiagnosticChartDataV1],
) -> dict[str, int]:
    precedence = {
        DiagnosticStatus.AVAILABLE: 0,
        DiagnosticStatus.EMPTY: 1,
        DiagnosticStatus.LIMITED: 2,
        DiagnosticStatus.UNDERPOWERED: 3,
        DiagnosticStatus.UNAVAILABLE: 4,
        DiagnosticStatus.MISSING_CONTEXT: 5,
        DiagnosticStatus.REFUSED: 6,
    }
    by_family: dict[DiagnosticFamily, list[DiagnosticStatus]] = {}
    for chart in charts:
        by_family.setdefault(chart.family, []).append(chart.status)
    selected = Counter(
        max(statuses, key=lambda item: precedence[item]).value
        for statuses in by_family.values()
    )
    return dict(sorted(selected.items()))


def _validated_status_counts(
    value: Mapping[str, int], *, total: int, name: str
) -> dict[str, int]:
    allowed = {item.value for item in DiagnosticStatus}
    counts: dict[str, int] = {}
    for key, count in value.items():
        status = _safe_text(key, name=name)
        if status not in allowed:
            raise ValueError(f"{name} contain an unknown status")
        counts[status] = _bounded_int(count, 1, total, name)
    if sum(counts.values()) != total:
        raise ValueError(f"{name} do not sum to the declared total")
    return dict(sorted(counts.items()))


def _trace_groups_by_window(
    context: _EvidenceContext,
) -> dict[str, tuple[BenchmarkWindowMetricObservationV1, ...]]:
    groups: dict[str, list[BenchmarkWindowMetricObservationV1]] = {}
    for observation in context.trace.observations:
        groups.setdefault(observation.window_id, []).append(observation)
    return {
        key: tuple(sorted(value, key=lambda item: item.observation_id))
        for key, value in sorted(groups.items())
    }


def _trace_groups_by_method_split(
    context: _EvidenceContext,
) -> dict[tuple[str, str], tuple[BenchmarkWindowMetricObservationV1, ...]]:
    groups: dict[tuple[str, str], list[BenchmarkWindowMetricObservationV1]] = {}
    for observation in context.trace.observations:
        groups.setdefault(
            (observation.method_name, observation.split_kind), []
        ).append(observation)
    return {
        key: tuple(sorted(value, key=lambda item: item.observation_id))
        for key, value in sorted(groups.items())
    }


def _trace_metric_median(
    observations: Sequence[BenchmarkWindowMetricObservationV1],
    *,
    group: str,
    metric: str,
) -> float | None:
    values: list[float] = []
    for observation in observations:
        if group == "reference":
            metrics = observation.reference_metrics
        elif group == "candidate":
            metrics = observation.candidate_metrics
        elif group == "comparison":
            metrics = observation.comparison_metrics
        else:  # pragma: no cover - internal programming guard
            raise ValueError("unsupported diagnostic trace metric group")
        value = metrics.get(metric)
        if value is not None:
            values.append(_finite_number(value, metric))
    return float(median(values)) if values else None


def _short_engine(value: str) -> str:
    return value.removeprefix("histdatacom.")


def _abs_optional(value: float | None) -> float | None:
    return abs(value) if value is not None else None


def _payload_label(payload: Mapping[str, Any], *, fallback: str) -> str:
    for key in ("manifest_id", "publication_id", "report_id", "run_id"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value.split(":sha256:", maxsplit=1)[0]
    return fallback


def _require_schema(value: str, expected: str, label: str) -> None:
    if value != expected:
        raise ValueError(f"unsupported {label} schema")


def _safe_text(value: Any, *, name: str, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be text")
    text = value.strip()
    if not text and not allow_empty:
        raise ValueError(f"{name} is empty")
    if len(text) > MAX_DIAGNOSTIC_TEXT:
        raise ValueError(f"{name} exceeds text bound")
    lowered = text.lower()
    if any(marker in text for marker in _LOCAL_PATH_MARKERS):
        raise ValueError(f"{name} contains a local path")
    if any(marker in lowered for marker in _SECRET_MARKERS):
        raise ValueError(f"{name} contains a secret-shaped marker")
    if any(
        ord(character) < 32 and character not in "\t\n" for character in text
    ):
        raise ValueError(f"{name} contains control characters")
    return text


def _safe_relative_locator(value: str) -> str:
    text = _safe_text(value, name="relative locator")
    path = PurePosixPath(text.replace("\\", "/"))
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("diagnostic locator is not publication-relative")
    return path.as_posix()


def _bounded_int(value: Any, minimum: int, maximum: int, name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} is outside bounds")
    return value


def _strict_int(value: Any, name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    return value


def _sha256(value: str) -> str:
    text = str(value).strip().lower()
    if not _SHA256_RE.fullmatch(text):
        raise ValueError("invalid sha256")
    return text


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        str(canonical_contract_json(dict(payload))).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _safe_text_tuple(
    values: Iterable[str],
    name: str,
    *,
    maximum: int,
    allow_empty: bool,
) -> tuple[str, ...]:
    selected = tuple(sorted({_safe_text(item, name=name) for item in values}))
    if not selected and not allow_empty:
        raise ValueError(f"{name} is empty")
    if len(selected) > maximum:
        raise ValueError(f"{name} exceeds count bound")
    return selected


def _string_tuple(value: Any, name: str) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value, name))


def _optional_finite(value: Any, name: str) -> float | None:
    if value is None:
        return None
    return _finite_number(value, name)


def _optional_number(value: Any, name: str) -> float | None:
    return _optional_finite(value, name)


def _optional_plain_float(value: Any) -> float | None:
    return None if value is None else _finite_number(value, "optional value")


def _finite_number(value: Any, name: str) -> float:
    if type(value) not in {int, float}:
        raise TypeError(f"{name} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{name} must be finite")
    return number


def _safe_strata(value: Mapping[str, str]) -> dict[str, str]:
    if len(value) > MAX_DIAGNOSTIC_STRATA:
        raise ValueError("diagnostic strata exceed count bound")
    return {
        _safe_text(str(key), name="stratum key"): _safe_text(
            str(item), name="stratum value", allow_empty=True
        )
        for key, item in sorted(value.items())
    }


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return cast(Mapping[str, Any], value)


def _sequence(value: Any, name: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray)
    ):
        raise TypeError(f"{name} must be a sequence")
    return value


def _strong_ref(ref: ArtifactRef) -> ArtifactRef:
    if not isinstance(ref, ArtifactRef):
        raise TypeError("diagnostic artifact reference has the wrong type")
    kind = _safe_text(ref.kind, name="artifact kind")
    if not isinstance(ref.path, str) or not ref.path.strip():
        raise ValueError("diagnostic artifact path is empty")
    path = ref.path.strip()
    if ref.size_bytes is None:
        raise ValueError("diagnostic artifact reference lacks size")
    size = _bounded_int(
        ref.size_bytes, 1, MAX_DIAGNOSTIC_ARTIFACT_BYTES, "artifact size"
    )
    digest = _sha256(ref.sha256)
    return ArtifactRef(
        kind=kind,
        path=path,
        size_bytes=size,
        sha256=digest,
        metadata=dict(ref.metadata),
    )


def _artifact_content_identity(ref: ArtifactRef) -> dict[str, JSONValue]:
    strong = _strong_ref(ref)
    return {
        "kind": strong.kind,
        "size_bytes": strong.size_bytes,
        "sha256": strong.sha256,
        "metadata": dict(strong.metadata),
    }


def _metadata_text(ref: ArtifactRef, key: str) -> str:
    value = ref.metadata.get(key)
    return _safe_text(value, name=f"artifact metadata {key}")


def _source_identity(
    ref: ArtifactRef, payload: Mapping[str, Any]
) -> DiagnosticSourceV1:
    strong = _strong_ref(ref)
    schema = _safe_text(
        payload.get("schema_version", ""), name="source schema version"
    )
    subject_id = ""
    for key in _SUBJECT_ID_FIELDS:
        value = payload.get(key)
        if isinstance(value, str) and value:
            subject_id = value
            break
    if not subject_id:
        for key in _SUBJECT_ID_FIELDS:
            value = strong.metadata.get(key)
            if isinstance(value, str) and value:
                subject_id = value
                break
    if not subject_id:
        subject_id = f"artifact:sha256:{strong.sha256}"
    locator = publish_safe_path(strong.path)
    if not locator or len(locator) > MAX_DIAGNOSTIC_TEXT:
        locator = f"artifacts/{strong.sha256}.json"
    return DiagnosticSourceV1(
        kind=strong.kind,
        subject_schema_version=schema,
        subject_id=subject_id,
        relative_locator=locator,
        size_bytes=cast(int, strong.size_bytes),
        sha256=strong.sha256,
    )


def _read_verified_json(ref: ArtifactRef) -> Mapping[str, Any]:
    path = verify_artifact_ref(_strong_ref(ref))
    if path.stat().st_size > MAX_DIAGNOSTIC_ARTIFACT_BYTES:
        raise ValueError("diagnostic source exceeds byte bound")
    return _mapping(
        json.loads(path.read_text(encoding="utf-8")), "artifact JSON"
    )


def _verify_histdata_optional_scope(
    ref: ArtifactRef, payload: Mapping[str, Any]
) -> None:
    provider = payload.get("provider_id", payload.get("current_provider_id"))
    if provider not in {None, CURRENT_DIAGNOSTIC_PROVIDER_ID}:
        raise ValueError("optional diagnostic artifact is not HistData-scoped")
    source_format = payload.get("source_format")
    if source_format not in {None, CURRENT_DIAGNOSTIC_SOURCE_FORMAT}:
        raise ValueError("optional diagnostic source format differs")
    timeframe = payload.get("timeframe")
    if timeframe not in {None, CURRENT_DIAGNOSTIC_TIMEFRAME}:
        raise ValueError("optional diagnostic timeframe differs")
    lowered_kind = ref.kind.lower()
    if "broker" in lowered_kind or "oanda" in lowered_kind:
        raise ValueError("broker diagnostic evidence is a later milestone")
    _reject_unsafe_optional_values(payload)


def _reject_unsafe_optional_values(value: Any, *, key: str = "") -> None:
    if isinstance(value, Mapping):
        for item_key, item in value.items():
            _reject_unsafe_optional_values(item, key=str(item_key).lower())
        return
    if isinstance(value, list):
        for item in value:
            _reject_unsafe_optional_values(item, key=key)
        return
    if key.endswith(("rows_embedded", "rows_inline")) and value not in (
        None,
        False,
        0,
    ):
        raise ValueError("optional diagnostic artifact embeds row data")
    if isinstance(value, str) and "oanda" in value.lower():
        raise ValueError("OANDA diagnostic evidence is a later milestone")


def _write_once(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.is_file() and path.read_bytes() == payload:
            return
        raise ValueError(
            f"refusing to overwrite differing artifact: {path.name}"
        )
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _read_content_addressed_json(
    path: Path, *, prefix: str, maximum: int
) -> Mapping[str, Any]:
    if not path.is_file():
        raise ValueError(f"diagnostic artifact is missing: {path}")
    payload = path.read_bytes()
    if not payload or len(payload) > maximum:
        raise ValueError("diagnostic artifact byte size is invalid")
    digest = hashlib.sha256(payload).hexdigest()
    if path.name != f"{prefix}-{digest}.json":
        raise ValueError("diagnostic content-addressed filename differs")
    return _mapping(json.loads(payload.decode("utf-8")), "diagnostic artifact")


def _read_exact_artifact(
    path: Path, *, size_bytes: int, sha256: str, maximum: int
) -> bytes:
    if not path.is_file() or path.is_symlink():
        raise ValueError("diagnostic artifact is missing or symlinked")
    payload = path.read_bytes()
    if len(payload) != size_bytes or len(payload) > maximum:
        raise ValueError("diagnostic artifact size differs")
    if hashlib.sha256(payload).hexdigest() != sha256:
        raise ValueError("diagnostic artifact sha256 differs")
    return payload


def _safe_child(root: Path, relative: str) -> Path:
    locator = _safe_relative_locator(relative)
    selected = (root / locator).resolve()
    resolved_root = root.resolve()
    if selected != resolved_root and resolved_root not in selected.parents:
        raise ValueError("diagnostic artifact escapes publication root")
    return selected


__all__ = [
    "CURRENT_DIAGNOSTIC_PROVIDER_ID",
    "DIAGNOSTIC_CHART_BUNDLE_SCHEMA_VERSION",
    "DIAGNOSTIC_CHART_DATA_SCHEMA_VERSION",
    "DIAGNOSTIC_PUBLICATION_MANIFEST_SCHEMA_VERSION",
    "DIAGNOSTIC_PUBLICATION_SPEC_SCHEMA_VERSION",
    "DIAGNOSTIC_RENDERED_ARTIFACT_SCHEMA_VERSION",
    "DIAGNOSTIC_RENDERER_CONFIG_SCHEMA_VERSION",
    "DIAGNOSTIC_SCIENTIFIC_NONCLAIM",
    "REQUIRED_DIAGNOSTIC_FAMILIES",
    "DiagnosticChartBundleV1",
    "DiagnosticChartDataV1",
    "DiagnosticChartDatumV1",
    "DiagnosticFamily",
    "DiagnosticMark",
    "DiagnosticPublicationManifestV1",
    "DiagnosticPublicationSpecV1",
    "DiagnosticRenderFormat",
    "DiagnosticRenderedArtifactV1",
    "DiagnosticRendererConfigV1",
    "DiagnosticSourceV1",
    "DiagnosticStatus",
    "build_reconstruction_diagnostic_bundle",
    "diagnostic_publication_listing",
    "publish_reconstruction_diagnostics",
    "read_diagnostic_publication_spec",
    "verify_reconstruction_diagnostic_publication",
    "write_diagnostic_publication_spec",
]
