"""First-party provider adapters for canonical observed ASCII/T ticks."""

from __future__ import annotations

import csv
import hashlib
import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from histdatacom.datasets.contracts import (
    CanonicalObservedPartitionV2,
    DatasetContractError,
    DatasetDescriptorV1,
    DatasetFailureCode,
    DatasetLicensingPolicy,
    DatasetOrigin,
    DatasetQualificationStatus,
    DatasetVersionManifestV1,
    ProviderAdapterDescriptorV1,
    SourceProviderDescriptorV1,
    normalize_period,
    normalize_symbol,
)
from histdatacom.histdata_ascii import (
    MAX_HISTDATA_SOURCE_ORDER_REGRESSION_MS,
    MAX_HISTDATA_SOURCE_ORDER_REGRESSIONS_PER_PARTITION,
)
from histdatacom.runtime_contracts import ArtifactRef

CANONICAL_TICK_PROJECTION_SCHEMA_VERSION = "histdatacom.canonical-ascii-tick.v2"
CANONICAL_TICK_ARTIFACT_KIND = "provider_ascii_tick_partition_v2"
HISTDATA_PROVIDER_ID = "histdata.com"
FIXTURE_PROVIDER_ID = "fixture.reference"
HISTDATA_ADAPTER_ID = "histdata-arrow-cache"
FIXTURE_ADAPTER_ID = "fixture-rfc3339-csv"

_SHA256_CHUNK_SIZE = 1024 * 1024
_MAX_HISTDATA_MONTH_SPILL_NS = 24 * 60 * 60 * 1_000_000_000
HISTDATA_QUOTE_ORDER_PROJECTION_POLICY = (
    "rowwise-min-bid-max-ask-preserve-raw-v1"
)
HISTDATA_QUOTE_ORDER_IDENTITY_POLICY = "identity-refuse-negative-spread-v1"
HISTDATA_SYSTEMATIC_QUOTE_INVERSION_MIN_RATE = 0.25


@runtime_checkable
class ProviderAdapter(Protocol):
    """Bounded inventory, validation, normalization, and attribution seam."""

    @property
    def provider(self) -> SourceProviderDescriptorV1:
        """Return immutable provider attribution and licensing metadata."""

    @property
    def descriptor(self) -> ProviderAdapterDescriptorV1:
        """Return immutable adapter behavior and version identity."""

    def discover(
        self,
        root: str | Path,
        *,
        symbols: Iterable[str] = (),
        periods: Iterable[str] = (),
    ) -> tuple[CanonicalObservedPartitionV2, ...]:
        """Discover and validate bounded canonical source partitions."""

    def inspect_partition(
        self,
        root: str | Path,
        *,
        symbol: str,
        period: str,
        expected_sha256: str | None = None,
    ) -> CanonicalObservedPartitionV2:
        """Validate one exact provider partition."""

    def read_partition(self, partition: CanonicalObservedPartitionV2) -> Any:
        """Return a canonical Polars frame with datetime/bid/ask/vol."""


@dataclass(frozen=True, slots=True)
class HistDataProviderAdapter:
    """Adapter for existing HistData Polars Arrow IPC ``.data`` caches."""

    @property
    def provider(self) -> SourceProviderDescriptorV1:
        return SourceProviderDescriptorV1(
            source_provider_id=HISTDATA_PROVIDER_ID,
            display_name="HistData.com",
            attribution="Historical observed ticks supplied by HistData.com.",
            licensing_policy=DatasetLicensingPolicy.LOCAL_ONLY,
            redistribution_allowed=False,
        )

    @property
    def descriptor(self) -> ProviderAdapterDescriptorV1:
        return ProviderAdapterDescriptorV1(
            adapter_id=HISTDATA_ADAPTER_ID,
            adapter_version="1.2.0",
            source_provider_id=self.provider.source_provider_id,
            formats=("ascii",),
            granularities=("T",),
            clock_policy_id="histdata-est-no-dst-to-utc-v1",
            partition_policy_id="histdata-source-month-with-utc-spill-v1",
            row_identity_policy_id="one-based-cache-row-ordinal-v1",
            projection_schema_version=CANONICAL_TICK_PROJECTION_SCHEMA_VERSION,
        )

    def discover(
        self,
        root: str | Path,
        *,
        symbols: Iterable[str] = (),
        periods: Iterable[str] = (),
    ) -> tuple[CanonicalObservedPartitionV2, ...]:
        source_root = _histdata_root(root)
        selected_symbols = {normalize_symbol(value) for value in symbols}
        selected_periods = {normalize_period(value) for value in periods}
        partitions: list[CanonicalObservedPartitionV2] = []
        for path in sorted(source_root.glob("*/[0-9]*/[0-9]*/.data")):
            try:
                symbol, period = _histdata_dimensions(source_root, path)
            except DatasetContractError:
                continue
            if selected_symbols and symbol not in selected_symbols:
                continue
            if selected_periods and period not in selected_periods:
                continue
            partitions.append(
                self.inspect_partition(
                    source_root,
                    symbol=symbol,
                    period=period,
                )
            )
        return _complete_selection(
            partitions,
            selected_symbols=selected_symbols,
            selected_periods=selected_periods,
        )

    def inspect_partition(
        self,
        root: str | Path,
        *,
        symbol: str,
        period: str,
        expected_sha256: str | None = None,
    ) -> CanonicalObservedPartitionV2:
        source_root = _histdata_root(root)
        normalized_symbol = normalize_symbol(symbol)
        normalized_period = normalize_period(period)
        path = histdata_cache_path(
            source_root, normalized_symbol, normalized_period
        )
        if not path.is_file():
            raise DatasetContractError(
                DatasetFailureCode.ARTIFACT_MISSING,
                f"HistData cache partition is missing: {path}",
            )
        digest = _file_sha256(path)
        _match_expected_hash(digest, expected_sha256, path)
        frame = _read_histdata_frame(path)
        validation = _validate_canonical_frame(
            frame,
            path,
            maximum_timestamp_regression_ms=(
                MAX_HISTDATA_SOURCE_ORDER_REGRESSION_MS
            ),
            maximum_timestamp_regression_count=(
                MAX_HISTDATA_SOURCE_ORDER_REGRESSIONS_PER_PARTITION
            ),
            allow_negative_spread=True,
        )
        month_start = _month_start_ns(normalized_period)
        month_end = _month_start_ns(_next_period(normalized_period))
        if not (
            month_start - _MAX_HISTDATA_MONTH_SPILL_NS
            <= validation.first_ms * 1_000_000
            < month_end + _MAX_HISTDATA_MONTH_SPILL_NS
            and month_start - _MAX_HISTDATA_MONTH_SPILL_NS
            <= validation.last_ms * 1_000_000
            < month_end + _MAX_HISTDATA_MONTH_SPILL_NS
        ):
            raise DatasetContractError(
                DatasetFailureCode.INCONSISTENT_COVERAGE,
                f"HistData cache timestamps exceed monthly spill policy: {path}",
            )
        descriptor = self.descriptor
        negative_spread_rate = (
            validation.negative_spread_count / validation.row_count
        )
        quote_order_projection_policy = (
            HISTDATA_QUOTE_ORDER_PROJECTION_POLICY
            if negative_spread_rate
            >= HISTDATA_SYSTEMATIC_QUOTE_INVERSION_MIN_RATE
            else HISTDATA_QUOTE_ORDER_IDENTITY_POLICY
        )
        return CanonicalObservedPartitionV2(
            source_provider_id=self.provider.source_provider_id,
            adapter_id=descriptor.adapter_id,
            adapter_version=descriptor.adapter_version,
            symbol=normalized_symbol,
            period=normalized_period,
            artifact=ArtifactRef(
                kind=CANONICAL_TICK_ARTIFACT_KIND,
                path=str(path.resolve()),
                size_bytes=path.stat().st_size,
                sha256=digest,
                metadata={
                    "provider": HISTDATA_PROVIDER_ID,
                    "format": "ascii",
                    "granularity": "T",
                    "symbol": normalized_symbol,
                    "period": normalized_period,
                    "row_count": validation.row_count,
                    "first_timestamp_ms": validation.first_ms,
                    "last_timestamp_ms": validation.last_ms,
                    "timestamp_order_policy": (
                        "source-order-preserved-bounded-regressions-v1"
                    ),
                    "timestamp_regression_count": (
                        validation.timestamp_regression_count
                    ),
                    "maximum_timestamp_regression_ms": (
                        validation.maximum_timestamp_regression_ms
                    ),
                    "raw_negative_spread_count": (
                        validation.negative_spread_count
                    ),
                    "raw_negative_spread_rate": (negative_spread_rate),
                    "quote_order_projection_policy": (
                        quote_order_projection_policy
                    ),
                    "systematic_quote_order_inversion": (
                        quote_order_projection_policy
                        == HISTDATA_QUOTE_ORDER_PROJECTION_POLICY
                    ),
                },
            ),
            source_artifact_sha256=digest,
            row_count=validation.row_count,
            coverage_start_ns=month_start,
            coverage_end_ns=month_end,
            clock_policy_id=descriptor.clock_policy_id,
            partition_policy_id=descriptor.partition_policy_id,
            row_identity_policy_id=descriptor.row_identity_policy_id,
            licensing_policy=self.provider.licensing_policy,
            native_partition_id=f"{normalized_symbol}:{normalized_period}",
            # Preserve the existing enriched-training series identity exactly.
            series_id=f"ascii:T:{normalized_symbol}:{HISTDATA_PROVIDER_ID}",
        )

    def read_partition(self, partition: CanonicalObservedPartitionV2) -> Any:
        _require_adapter_partition(partition, self.descriptor)
        _verify_partition_artifact(partition)
        frame = _read_histdata_frame(Path(partition.artifact.path))
        validation = _validate_canonical_frame(
            frame,
            Path(partition.artifact.path),
            maximum_timestamp_regression_ms=(
                MAX_HISTDATA_SOURCE_ORDER_REGRESSION_MS
            ),
            maximum_timestamp_regression_count=(
                MAX_HISTDATA_SOURCE_ORDER_REGRESSIONS_PER_PARTITION
            ),
            allow_negative_spread=True,
        )
        if validation.row_count != partition.row_count:
            raise DatasetContractError(
                DatasetFailureCode.INCONSISTENT_COVERAGE,
                "HistData cache row count changed after inventory",
            )
        return frame.select("datetime", "bid", "ask", "vol")


@dataclass(frozen=True, slots=True)
class FixtureProviderAdapter:
    """Deterministic reference adapter for explicit-UTC fixture CSV files."""

    source_provider_id: str = FIXTURE_PROVIDER_ID
    licensing_policy: DatasetLicensingPolicy = DatasetLicensingPolicy.PUBLIC
    redistribution_allowed: bool = True

    @property
    def provider(self) -> SourceProviderDescriptorV1:
        return SourceProviderDescriptorV1(
            source_provider_id=self.source_provider_id,
            display_name=(
                f"Reference fixture provider ({self.source_provider_id})"
            ),
            attribution=(
                "Deterministic local reference fixture; not a market feed."
            ),
            licensing_policy=self.licensing_policy,
            redistribution_allowed=self.redistribution_allowed,
        )

    @property
    def descriptor(self) -> ProviderAdapterDescriptorV1:
        return ProviderAdapterDescriptorV1(
            adapter_id=(
                FIXTURE_ADAPTER_ID
                if self.source_provider_id == FIXTURE_PROVIDER_ID
                else f"{FIXTURE_ADAPTER_ID}-{self.source_provider_id}"
            ),
            adapter_version="1.0.0",
            source_provider_id=self.provider.source_provider_id,
            formats=("ascii",),
            granularities=("T",),
            clock_policy_id="explicit-rfc3339-utc-v1",
            partition_policy_id="fixture-symbol-yyyy-mm-csv-v1",
            row_identity_policy_id="one-based-csv-source-row-v1",
            projection_schema_version=CANONICAL_TICK_PROJECTION_SCHEMA_VERSION,
        )

    def discover(
        self,
        root: str | Path,
        *,
        symbols: Iterable[str] = (),
        periods: Iterable[str] = (),
    ) -> tuple[CanonicalObservedPartitionV2, ...]:
        source_root = Path(root).expanduser().resolve()
        selected_symbols = {normalize_symbol(value) for value in symbols}
        selected_periods = {normalize_period(value) for value in periods}
        partitions: list[CanonicalObservedPartitionV2] = []
        for path in sorted(source_root.glob("*/????-??.csv")):
            symbol = normalize_symbol(path.parent.name)
            period = normalize_period(path.stem.replace("-", ""))
            if selected_symbols and symbol not in selected_symbols:
                continue
            if selected_periods and period not in selected_periods:
                continue
            partitions.append(
                self.inspect_partition(
                    source_root,
                    symbol=symbol,
                    period=period,
                )
            )
        return _complete_selection(
            partitions,
            selected_symbols=selected_symbols,
            selected_periods=selected_periods,
        )

    def inspect_partition(
        self,
        root: str | Path,
        *,
        symbol: str,
        period: str,
        expected_sha256: str | None = None,
    ) -> CanonicalObservedPartitionV2:
        source_root = Path(root).expanduser().resolve()
        normalized_symbol = normalize_symbol(symbol)
        normalized_period = normalize_period(period)
        path = fixture_csv_path(
            source_root, normalized_symbol, normalized_period
        )
        if not path.is_file():
            raise DatasetContractError(
                DatasetFailureCode.ARTIFACT_MISSING,
                f"fixture CSV partition is missing: {path}",
            )
        digest = _file_sha256(path)
        _match_expected_hash(digest, expected_sha256, path)
        frame = _read_fixture_frame(path)
        validation = _validate_canonical_frame(frame, path)
        month_start = _month_start_ns(normalized_period)
        month_end = _month_start_ns(_next_period(normalized_period))
        if not (
            month_start <= validation.first_ms * 1_000_000 < month_end
            and month_start <= validation.last_ms * 1_000_000 < month_end
        ):
            raise DatasetContractError(
                DatasetFailureCode.INCONSISTENT_COVERAGE,
                f"fixture UTC timestamps are outside {normalized_period}",
            )
        descriptor = self.descriptor
        return CanonicalObservedPartitionV2(
            source_provider_id=self.provider.source_provider_id,
            adapter_id=descriptor.adapter_id,
            adapter_version=descriptor.adapter_version,
            symbol=normalized_symbol,
            period=normalized_period,
            artifact=ArtifactRef(
                kind=CANONICAL_TICK_ARTIFACT_KIND,
                path=str(path.resolve()),
                size_bytes=path.stat().st_size,
                sha256=digest,
                metadata={
                    "provider": self.provider.source_provider_id,
                    "format": "ascii",
                    "granularity": "T",
                    "symbol": normalized_symbol,
                    "period": normalized_period,
                    "row_count": validation.row_count,
                    "first_timestamp_ms": validation.first_ms,
                    "last_timestamp_ms": validation.last_ms,
                },
            ),
            source_artifact_sha256=digest,
            row_count=validation.row_count,
            coverage_start_ns=month_start,
            coverage_end_ns=month_end,
            clock_policy_id=descriptor.clock_policy_id,
            partition_policy_id=descriptor.partition_policy_id,
            row_identity_policy_id=descriptor.row_identity_policy_id,
            licensing_policy=self.provider.licensing_policy,
            native_partition_id=path.stem,
            series_id=(
                f"ascii:T:{normalized_symbol}:{self.provider.source_provider_id}"
            ),
        )

    def read_partition(self, partition: CanonicalObservedPartitionV2) -> Any:
        _require_adapter_partition(partition, self.descriptor)
        _verify_partition_artifact(partition)
        frame = _read_fixture_frame(Path(partition.artifact.path))
        validation = _validate_canonical_frame(
            frame, Path(partition.artifact.path)
        )
        if validation.row_count != partition.row_count:
            raise DatasetContractError(
                DatasetFailureCode.INCONSISTENT_COVERAGE,
                "fixture row count changed after inventory",
            )
        return frame


def build_observed_dataset_version(
    adapter: ProviderAdapter,
    root: str | Path,
    descriptor: DatasetDescriptorV1,
    *,
    symbols: Iterable[str],
    periods: Iterable[str],
    qualification_evidence: Iterable[ArtifactRef],
    qualified: bool = True,
) -> DatasetVersionManifestV1:
    """Inventory one provider and bind it to an immutable dataset version."""
    if not isinstance(adapter, ProviderAdapter):
        raise TypeError("adapter must implement ProviderAdapter")
    if DatasetOrigin.OBSERVED not in descriptor.allowed_origins:
        raise DatasetContractError(
            DatasetFailureCode.UNSUPPORTED_ORIGIN,
            "dataset descriptor does not allow observed versions",
        )
    selected_symbols = tuple(
        sorted({normalize_symbol(value) for value in symbols})
    )
    selected_periods = tuple(
        sorted({normalize_period(value) for value in periods})
    )
    if not selected_symbols or not selected_periods:
        raise DatasetContractError(
            DatasetFailureCode.INCONSISTENT_COVERAGE,
            "observed dataset build requires symbols and periods",
        )
    partitions = adapter.discover(
        root, symbols=selected_symbols, periods=selected_periods
    )
    expected = {
        (symbol, period)
        for symbol in selected_symbols
        for period in selected_periods
    }
    actual = {(item.symbol, item.period) for item in partitions}
    if actual != expected:
        raise DatasetContractError(
            DatasetFailureCode.INCONSISTENT_COVERAGE,
            "provider inventory does not cover the exact requested matrix",
        )
    return DatasetVersionManifestV1(
        dataset_id=descriptor.dataset_id,
        origin=DatasetOrigin.OBSERVED,
        normalization_policy_id=(
            f"{adapter.descriptor.adapter_id}@"
            f"{adapter.descriptor.adapter_version}:"
            f"{adapter.descriptor.projection_schema_version}"
        ),
        qualification_status=(
            DatasetQualificationStatus.QUALIFIED
            if qualified
            else DatasetQualificationStatus.UNQUALIFIED
        ),
        partitions=partitions,
        qualification_evidence=tuple(qualification_evidence),
    )


def histdata_cache_path(root: str | Path, symbol: str, period: str) -> Path:
    """Return the existing cache path policy owned by the HistData adapter."""
    source_root: Path = Path(root).expanduser().resolve()
    normalized_symbol = normalize_symbol(symbol).lower()
    normalized_period = normalize_period(period)
    cache_path: Path = (
        source_root
        / normalized_symbol
        / str(int(normalized_period[:4]))
        / str(int(normalized_period[4:]))
        / ".data"
    )
    return cache_path


def fixture_csv_path(root: str | Path, symbol: str, period: str) -> Path:
    """Return the distinct flat reference-provider partition path."""
    source_root: Path = Path(root).expanduser().resolve()
    normalized_symbol = normalize_symbol(symbol)
    normalized_period = normalize_period(period)
    fixture_path: Path = (
        source_root
        / normalized_symbol
        / f"{normalized_period[:4]}-{normalized_period[4:]}.csv"
    )
    return fixture_path


def _histdata_root(root: str | Path) -> Path:
    source_root = Path(root).expanduser().resolve()
    if (
        source_root.name.upper() != "T"
        or source_root.parent.name.upper() != "ASCII"
    ):
        raise DatasetContractError(
            DatasetFailureCode.UNSUPPORTED_FORMAT,
            "HistData adapter root must be the existing ASCII/T directory",
        )
    return source_root


def _histdata_dimensions(root: Path, path: Path) -> tuple[str, str]:
    relative = path.resolve().relative_to(root.resolve())
    if len(relative.parts) != 4 or relative.name != ".data":
        raise DatasetContractError(
            DatasetFailureCode.INCONSISTENT_COVERAGE,
            f"unsupported HistData cache path: {path}",
        )
    symbol = normalize_symbol(relative.parts[0])
    try:
        year = int(relative.parts[1])
        month = int(relative.parts[2])
    except ValueError as err:
        raise DatasetContractError(
            DatasetFailureCode.INVALID_PERIOD,
            f"invalid HistData cache period path: {path}",
        ) from err
    return symbol, normalize_period(f"{year:04d}{month:02d}")


def _complete_selection(
    partitions: Iterable[CanonicalObservedPartitionV2],
    *,
    selected_symbols: set[str],
    selected_periods: set[str],
) -> tuple[CanonicalObservedPartitionV2, ...]:
    result = tuple(
        sorted(partitions, key=lambda item: (item.period, item.symbol))
    )
    if selected_symbols and selected_periods:
        expected = {
            (symbol, period)
            for symbol in selected_symbols
            for period in selected_periods
        }
        if {(item.symbol, item.period) for item in result} != expected:
            raise DatasetContractError(
                DatasetFailureCode.INCONSISTENT_COVERAGE,
                "provider discovery does not contain the requested "
                "symbol/period matrix",
            )
    return result


def _read_histdata_frame(path: Path) -> Any:
    try:
        import polars as pl  # pylint: disable=import-outside-toplevel

        return pl.read_ipc(path)
    except Exception as err:
        raise DatasetContractError(
            DatasetFailureCode.UNSUPPORTED_FORMAT,
            f"HistData partition is not readable Arrow IPC: {path}",
        ) from err


def _read_fixture_frame(path: Path) -> Any:
    try:
        import polars as pl  # pylint: disable=import-outside-toplevel
    except ImportError as err:
        raise RuntimeError("fixture provider adapter requires polars") from err
    rows: list[tuple[int, float, float, int, str | None]] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as source:
            reader = csv.DictReader(source)
            required = {"timestamp", "bid", "ask"}
            if reader.fieldnames is None or not required.issubset(
                reader.fieldnames
            ):
                raise DatasetContractError(
                    DatasetFailureCode.UNSUPPORTED_FORMAT,
                    "fixture CSV requires timestamp,bid,ask headers",
                )
            for source_row, row in enumerate(reader, start=1):
                timestamp_ms = _explicit_utc_ms(
                    row.get("timestamp"), source_row
                )
                try:
                    bid = float(str(row.get("bid", "")).strip())
                    ask = float(str(row.get("ask", "")).strip())
                    vol = int(str(row.get("vol", "0") or "0").strip())
                except ValueError as err:
                    raise DatasetContractError(
                        DatasetFailureCode.MALFORMED_QUOTE,
                        f"fixture row {source_row} contains non-numeric quote fields",
                    ) from err
                native = str(row.get("native_id", "") or "").strip() or None
                rows.append((timestamp_ms, bid, ask, vol, native))
    except OSError as err:
        raise DatasetContractError(
            DatasetFailureCode.ARTIFACT_MISSING,
            f"fixture partition cannot be read: {path}",
        ) from err
    return pl.DataFrame(
        {
            "datetime": [item[0] for item in rows],
            "bid": [item[1] for item in rows],
            "ask": [item[2] for item in rows],
            "vol": [item[3] for item in rows],
            "native_record_id": [item[4] for item in rows],
        },
        schema={
            "datetime": pl.Int64,
            "bid": pl.Float64,
            "ask": pl.Float64,
            "vol": pl.Int32,
            "native_record_id": pl.Utf8,
        },
    )


def _explicit_utc_ms(value: Any, row_number: int) -> int:
    text = str(value or "").strip()
    if not text or (not text.endswith("Z") and not text.endswith("+00:00")):
        raise DatasetContractError(
            DatasetFailureCode.AMBIGUOUS_CLOCK,
            f"fixture row {row_number} requires an explicit UTC RFC3339 timestamp",
        )
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as err:
        raise DatasetContractError(
            DatasetFailureCode.AMBIGUOUS_CLOCK,
            f"fixture row {row_number} has an invalid RFC3339 timestamp",
        ) from err
    if parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise DatasetContractError(
            DatasetFailureCode.AMBIGUOUS_CLOCK,
            f"fixture row {row_number} timestamp is not UTC",
        )
    return int(parsed.timestamp() * 1000)


@dataclass(frozen=True, slots=True)
class _CanonicalFrameValidation:
    row_count: int
    first_ms: int
    last_ms: int
    timestamp_regression_count: int
    maximum_timestamp_regression_ms: int
    negative_spread_count: int


def _validate_canonical_frame(
    frame: Any,
    path: Path,
    *,
    maximum_timestamp_regression_ms: int = 0,
    maximum_timestamp_regression_count: int = 0,
    allow_negative_spread: bool = False,
) -> _CanonicalFrameValidation:
    columns = set(getattr(frame, "columns", ()))
    if not {"datetime", "bid", "ask"}.issubset(columns):
        raise DatasetContractError(
            DatasetFailureCode.UNSUPPORTED_FORMAT,
            f"canonical tick partition lacks datetime/bid/ask: {path}",
        )
    if columns.intersection({"open", "high", "low", "close"}):
        raise DatasetContractError(
            DatasetFailureCode.UNSUPPORTED_TIMEFRAME,
            f"canonical tick partition contains forbidden OHLC fields: {path}",
        )
    row_count = int(getattr(frame, "height", 0))
    if row_count < 1:
        raise DatasetContractError(
            DatasetFailureCode.INCONSISTENT_COVERAGE,
            f"canonical tick partition is empty: {path}",
        )
    timestamps = [
        int(value) for value in frame.get_column("datetime").to_list()
    ]
    bids = frame.get_column("bid").to_list()
    asks = frame.get_column("ask").to_list()
    previous: int | None = None
    regression_count = 0
    maximum_regression = 0
    negative_spread_count = 0
    for ordinal, (timestamp, bid_value, ask_value) in enumerate(
        zip(timestamps, bids, asks, strict=True), start=1
    ):
        try:
            bid = float(bid_value)
            ask = float(ask_value)
        except (TypeError, ValueError) as err:
            raise DatasetContractError(
                DatasetFailureCode.MALFORMED_QUOTE,
                f"partition row {ordinal} has non-numeric bid/ask",
            ) from err
        negative_spread = ask < bid
        if negative_spread:
            negative_spread_count += 1
        if (
            not math.isfinite(bid)
            or not math.isfinite(ask)
            or bid <= 0.0
            or ask <= 0.0
            or (negative_spread and not allow_negative_spread)
        ):
            raise DatasetContractError(
                DatasetFailureCode.MALFORMED_QUOTE,
                f"partition row {ordinal} has invalid bid/ask",
            )
        if previous is not None and timestamp < previous:
            regression = previous - timestamp
            regression_count += 1
            maximum_regression = max(maximum_regression, regression)
            if (
                regression > maximum_timestamp_regression_ms
                or regression_count > maximum_timestamp_regression_count
            ):
                raise DatasetContractError(
                    DatasetFailureCode.INCONSISTENT_COVERAGE,
                    f"partition row {ordinal} timestamp regresses",
                )
        previous = timestamp
    return _CanonicalFrameValidation(
        row_count=row_count,
        first_ms=min(timestamps),
        last_ms=max(timestamps),
        timestamp_regression_count=regression_count,
        maximum_timestamp_regression_ms=maximum_regression,
        negative_spread_count=negative_spread_count,
    )


def _require_adapter_partition(
    partition: CanonicalObservedPartitionV2,
    descriptor: ProviderAdapterDescriptorV1,
) -> None:
    if (
        partition.adapter_id != descriptor.adapter_id
        or partition.adapter_version != descriptor.adapter_version
        or partition.source_provider_id != descriptor.source_provider_id
        or partition.clock_policy_id != descriptor.clock_policy_id
        or partition.partition_policy_id != descriptor.partition_policy_id
        or partition.row_identity_policy_id != descriptor.row_identity_policy_id
    ):
        raise DatasetContractError(
            DatasetFailureCode.IDENTITY_MISMATCH,
            "partition is not owned by this exact provider adapter version",
        )


def _verify_partition_artifact(partition: CanonicalObservedPartitionV2) -> None:
    path = Path(partition.artifact.path)
    if not path.is_file():
        raise DatasetContractError(
            DatasetFailureCode.ARTIFACT_MISSING,
            f"partition artifact is missing: {path}",
        )
    if path.stat().st_size != partition.artifact.size_bytes:
        raise DatasetContractError(
            DatasetFailureCode.ARTIFACT_SIZE_MISMATCH,
            f"partition artifact size changed: {path}",
        )
    if _file_sha256(path) != partition.artifact.sha256:
        raise DatasetContractError(
            DatasetFailureCode.ARTIFACT_HASH_MISMATCH,
            f"partition artifact hash changed: {path}",
        )


def _match_expected_hash(actual: str, expected: str | None, path: Path) -> None:
    if expected is None:
        return
    normalized = str(expected).strip().lower().removeprefix("sha256:")
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise DatasetContractError(
            DatasetFailureCode.MISSING_HASH,
            f"expected source hash is invalid for {path}",
        )
    if actual != normalized:
        raise DatasetContractError(
            DatasetFailureCode.ARTIFACT_HASH_MISMATCH,
            f"source artifact hash differs for {path}",
        )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(_SHA256_CHUNK_SIZE), b""):
                digest.update(chunk)
    except OSError as err:
        raise DatasetContractError(
            DatasetFailureCode.ARTIFACT_MISSING,
            f"artifact cannot be hashed: {path}",
        ) from err
    return digest.hexdigest()


def _month_start_ns(period: str) -> int:
    normalized = normalize_period(period)
    return int(
        datetime(
            int(normalized[:4]),
            int(normalized[4:]),
            1,
            tzinfo=timezone.utc,
        ).timestamp()
        * 1_000_000_000
    )


def _next_period(period: str) -> str:
    normalized = normalize_period(period)
    year, month = int(normalized[:4]), int(normalized[4:])
    return f"{year + 1:04d}01" if month == 12 else f"{year:04d}{month + 1:02d}"


__all__ = [
    "CANONICAL_TICK_ARTIFACT_KIND",
    "CANONICAL_TICK_PROJECTION_SCHEMA_VERSION",
    "FIXTURE_ADAPTER_ID",
    "FIXTURE_PROVIDER_ID",
    "HISTDATA_ADAPTER_ID",
    "HISTDATA_PROVIDER_ID",
    "FixtureProviderAdapter",
    "HistDataProviderAdapter",
    "ProviderAdapter",
    "build_observed_dataset_version",
    "fixture_csv_path",
    "histdata_cache_path",
]
