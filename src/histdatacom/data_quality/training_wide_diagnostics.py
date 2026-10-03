"""Bounded value-only feature dependence diagnostics (contract 1.0.0).

These functions do not authenticate sources, grant rights, choose features or
certify independent evidence. The wide-view consumer replays sources before
binding this report to its schema/content roots. Pairwise deletion is useful
descriptively, but is never used to invent a positive-semidefinite rank matrix.
Only a single common complete-case matrix supplies the participation ratio.
Large views must identify their diagnostic blocks; block ranks are not additive.
"""

from __future__ import annotations

import math
import re
from dataclasses import asdict, dataclass
from decimal import ROUND_HALF_EVEN, Context, Decimal, localcontext
from fractions import Fraction

WideDiagnosticScalar = int | float | bool | str | None
MAX_WIDE_DIAGNOSTIC_COLUMNS = 32
MAX_WIDE_DIAGNOSTIC_ROWS = 256
MAX_WIDE_DIAGNOSTIC_CELLS = 4096
MAX_WIDE_DIAGNOSTIC_INTEGER_BITS = 1024
MAX_WIDE_DIAGNOSTIC_TEXT_BYTES = 1024 * 1024
MAX_WIDE_DIAGNOSTIC_RATIONAL_BITS = 4_194_304
WIDE_DIAGNOSTIC_NONCLAIMS = (
    "value_diagnostics_do_not_authenticate_sources_or_authorize_consumption",
    "participation_ratio_is_not_algebraic_rank_or_independent_evidence_count",
    "rank_covers_only_observed_numeric_columns_on_common_complete_cases",
    "within_block_rank_does_not_measure_or_sum_to_whole_bank_rank",
    "no_feature_selection_ablation_or_final_holdout_decision",
    "floating_views_are_rounded_from_exact_binary_scalar_arithmetic",
)


@dataclass(frozen=True, slots=True)
class WideFeatureColumnDiagnosticV1:
    name: str
    status: str
    observed_count: int
    missing_count: int
    numeric_count: int


@dataclass(frozen=True, slots=True)
class WideFeatureCorrelationV1:
    left: str
    right: str
    support_count: int
    status: str
    correlation: float | None
    squared_correlation: float | None


@dataclass(frozen=True, slots=True)
class WideFeatureDiagnosticsV1:
    """An execution result, not a verification certificate when constructed."""

    row_count: int
    column_summaries: tuple[WideFeatureColumnDiagnosticV1, ...]
    pairwise_correlations: tuple[WideFeatureCorrelationV1, ...]
    rank_columns: tuple[str, ...]
    complete_case_count: int
    participation_ratio_rank: float | None
    rank_status: str
    schema_version: str = "histdatacom.wide-feature-diagnostics.v1"
    nonclaims: tuple[str, ...] = WIDE_DIAGNOSTIC_NONCLAIMS

    def to_dict(self) -> dict[str, object]:
        """Return detached JSON primitives, never a shared mutable cache."""
        return {
            "schema_version": self.schema_version,
            "row_count": self.row_count,
            "column_summaries": [asdict(v) for v in self.column_summaries],
            "pairwise_correlations": [
                asdict(v) for v in self.pairwise_correlations
            ],
            "rank_columns": list(self.rank_columns),
            "complete_case_count": self.complete_case_count,
            "participation_ratio_rank": self.participation_ratio_rank,
            "rank_status": self.rank_status,
            "nonclaims": list(self.nonclaims),
        }


def _validate(
    columns: tuple[str, ...],
    rows: tuple[tuple[WideDiagnosticScalar, ...], ...],
) -> None:
    if type(columns) is not tuple or type(rows) is not tuple:
        raise ValueError("diagnostics require exact immutable tuple inputs")
    if (
        not 0 < len(columns) <= MAX_WIDE_DIAGNOSTIC_COLUMNS
        or len(rows) > MAX_WIDE_DIAGNOSTIC_ROWS
        or len(rows) * len(columns) > MAX_WIDE_DIAGNOSTIC_CELLS
    ):
        raise ValueError("diagnostic column/row/cell budget exceeded")
    if any(
        type(name) is not str
        or not 0 < len(name) <= 256
        or re.fullmatch(r"[A-Za-z0-9_.-]+", name) is None
        for name in columns
    ):
        raise ValueError("diagnostic column names must be bounded identifiers")
    if len(set(columns)) != len(columns):
        raise ValueError("duplicate diagnostic column name")
    if any(type(row) is not tuple or len(row) != len(columns) for row in rows):
        raise ValueError("diagnostic rows must be exact non-ragged tuples")
    text_bytes = 0
    for row in rows:
        for value in row:
            if value is None or type(value) is bool:
                continue
            if type(value) is int:
                if value.bit_length() > MAX_WIDE_DIAGNOSTIC_INTEGER_BITS:
                    raise ValueError("diagnostic integer exceeds bit budget")
            elif type(value) is float:
                if not math.isfinite(value):
                    raise ValueError("diagnostic floats must be finite")
            elif type(value) is str:
                if len(value) > 4096:
                    raise ValueError("diagnostic string exceeds length budget")
                text_bytes += len(value.encode("utf-8"))
                if text_bytes > MAX_WIDE_DIAGNOSTIC_TEXT_BYTES:
                    raise ValueError("diagnostic aggregate text exceeds budget")
            else:
                raise ValueError("diagnostics require exact JSON scalar types")


def _center(
    values: tuple[Fraction, ...],
) -> tuple[tuple[Fraction, ...], Fraction]:
    mean = sum(values, Fraction()) / len(values)
    deviations = tuple(value - mean for value in values)
    squares = sum((value * value for value in deviations), Fraction())
    return deviations, squares


def _cross(left: tuple[Fraction, ...], right: tuple[Fraction, ...]) -> Fraction:
    return sum((a * b for a, b in zip(left, right)), Fraction())


def _correlation(cross: Fraction, squared: Fraction) -> float:
    # Sqrt before conversion avoids underflow of a tiny *squared* coefficient.
    # Own context: callers' Decimal precision/traps cannot change the report.
    with localcontext(
        Context(
            prec=80,
            rounding=ROUND_HALF_EVEN,
            Emax=999999,
            Emin=-999999,
            traps=[],
        )
    ):
        absolute = float(
            (Decimal(squared.numerator) / Decimal(squared.denominator)).sqrt()
        )
    return -absolute if cross < 0 else absolute


def diagnose_wide_feature_values(
    columns: tuple[str, ...],
    rows: tuple[tuple[WideDiagnosticScalar, ...], ...],
) -> WideFeatureDiagnosticsV1:
    """Diagnose one explicit bounded block without filling or selecting rows.

    Bool/categorical/mixed-type columns are excluded from rank, not coerced to
    numbers. All-null columns have no observed numeric type. Numeric constants
    are retained and make rank unavailable, rather than silently being dropped.
    Pair support counts rows with two numeric cells, but a mixed-type column
    still refuses a coefficient: unsupported cells cannot be cherry-picked away.
    Empty row selections produce explicit insufficient-support outcomes.
    """
    _validate(columns, rows)
    values: list[tuple[Fraction | None, ...]] = []
    summaries: list[WideFeatureColumnDiagnosticV1] = []
    remaining_bits = MAX_WIDE_DIAGNOSTIC_RATIONAL_BITS
    for index, name in enumerate(columns):
        numeric: list[Fraction | None] = []
        observed = 0
        unsupported = False
        for row in rows:
            value = row[index]
            observed += value is not None
            number = None
            if type(value) in (int, float):
                # Validation already excludes bool/subclasses/nonfinite values.
                assert isinstance(value, (int, float))
                number = Fraction(value)
                remaining_bits -= (
                    abs(number.numerator).bit_length()
                    + number.denominator.bit_length()
                )
                if remaining_bits < 0:
                    raise ValueError("diagnostic aggregate rational bit budget")
            elif value is not None:
                unsupported = True
            numeric.append(number)
        known = tuple(value for value in numeric if value is not None)
        status = (
            "unsupported_type"
            if unsupported
            else (
                "insufficient_support"
                if len(known) < 2
                else "constant" if len(set(known)) == 1 else "numeric"
            )
        )
        summaries.append(
            WideFeatureColumnDiagnosticV1(
                name, status, observed, len(rows) - observed, len(known)
            )
        )
        values.append(tuple(numeric))

    pairs: list[WideFeatureCorrelationV1] = []
    for i, left in enumerate(values):
        for j in range(i + 1, len(values)):
            paired = tuple(
                (a, b)
                for a, b in zip(left, values[j])
                if a is not None and b is not None
            )
            coefficient = squared_view = None
            if "unsupported_type" in (summaries[i].status, summaries[j].status):
                status = "unsupported_type"
            elif len(paired) < 2:
                status = "insufficient_support"
            else:
                a, a2 = _center(tuple(pair[0] for pair in paired))
                b, b2 = _center(tuple(pair[1] for pair in paired))
                if not a2 or not b2:
                    status = "constant"
                else:
                    cross = _cross(a, b)
                    squared = cross * cross / (a2 * b2)
                    coefficient = _correlation(cross, squared)
                    squared_view = float(squared)
                    status = "available"
            pairs.append(
                WideFeatureCorrelationV1(
                    columns[i],
                    columns[j],
                    len(paired),
                    status,
                    coefficient,
                    squared_view,
                )
            )

    selected = tuple(
        i
        for i, summary in enumerate(summaries)
        if summary.status != "unsupported_type" and summary.numeric_count
    )
    complete = tuple(
        row
        for row in range(len(rows))
        if selected and all(values[i][row] is not None for i in selected)
    )
    rank = None
    if not selected:
        rank_status = "no_numeric_columns"
    elif len(complete) < 2:
        rank_status = "insufficient_common_support"
    else:
        centered = []
        for i in selected:
            vector = tuple(values[i][row] for row in complete)
            known_vector = tuple(v for v in vector if v is not None)
            assert len(known_vector) == len(complete)
            centered.append(_center(known_vector))
        if any(not square for _, square in centered):
            rank_status = "constant_on_common_support"
        else:
            denominator = Fraction(len(selected))
            for i, (left, left2) in enumerate(centered):
                for right, right2 in centered[:i]:
                    cross = _cross(left, right)
                    denominator += 2 * cross * cross / (left2 * right2)
            rank = float(Fraction(len(selected) ** 2) / denominator)
            rank_status = "available"
    return WideFeatureDiagnosticsV1(
        len(rows),
        tuple(summaries),
        tuple(pairs),
        tuple(columns[i] for i in selected),
        len(complete),
        rank,
        rank_status,
    )
