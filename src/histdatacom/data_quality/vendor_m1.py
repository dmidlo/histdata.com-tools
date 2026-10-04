"""Read-only, bounded HistData Generic ASCII M1 versus raw-tick validation.

Both inputs are references, not reconstruction or publication authority. M1
prices are bid only; its sixth field is an unqualified nominal integer. No
source repair, gap fill, volume feature, file mutation, or download occurs.
"""

from __future__ import annotations

import hashlib
import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path
from typing import Any

from histdatacom.data_quality.vendor_m1_contracts import (
    MAX_LINE_BYTES,
    MAX_MINUTES,
    MAX_REFERENCE_ROWS,
    MAX_REGRESSION_MS,
    MAX_REGRESSIONS,
    MAX_SOURCE_BYTES,
    MAX_TICKS,
    MINUTE_NS,
    OHLC,
    VendorM1DifferenceV1,
    VendorM1MinuteV1,
    VendorM1PolicyV1,
    VendorM1ReferenceV1,
    VendorM1ReportV1,
    bounded_fraction,
    canonical,
    decimal_value,
    rational_text,
    readmit,
    require_id,
    require_sha,
    reserve_report,
)

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_INTEGER = re.compile(r"-?(?:0|[1-9][0-9]*)\Z")


@dataclass(frozen=True, slots=True)
class _M1Row:
    ordinal: int
    time_ns: int
    prices: tuple[str, ...]
    volume: int


@dataclass(frozen=True, slots=True)
class _Tick:
    ordinal: int
    time_ns: int
    bid: str
    ask: str
    volume: int


def _stat_key(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_source(path: str | Path, expected_sha256: str) -> str:
    """Snapshot a bounded regular final component; never follow its symlink.

    lstat/open/fstat checks are also retained when O_NOFOLLOW/O_NONBLOCK are
    unavailable; those platforms do not gain POSIX atomic no-follow guarantees.
    Ancestor directories are not claimed to be pinned against hostile writers.
    """
    require_sha(expected_sha256)
    if not isinstance(path, (str, Path)):
        raise TypeError("source path required")
    selected = Path(path)
    before = selected.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        raise ValueError("source must be a single-link regular file")
    if before.st_size > MAX_SOURCE_BYTES:
        raise ValueError("source byte bound exceeded")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    descriptor = os.open(selected, flags)
    try:
        opened = os.fstat(descriptor)
        if _stat_key(opened) != _stat_key(before):
            raise ValueError("source identity changed before read")
        parts: list[bytes] = []
        remaining = MAX_SOURCE_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                break
            parts.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(parts)
        after = os.fstat(descriptor)
        if (
            len(raw) != opened.st_size
            or len(raw) > MAX_SOURCE_BYTES
            or _stat_key(after) != _stat_key(opened)
            or _stat_key(selected.lstat()) != _stat_key(opened)
        ):
            raise ValueError("source identity/size changed during read")
    finally:
        os.close(descriptor)
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("source SHA-256 differs from selected root")
    try:
        return raw.decode("ascii")
    except UnicodeDecodeError as exc:
        raise ValueError("source must be ASCII") from exc


def _lines(text: str, maximum: int) -> tuple[str, ...]:
    if (
        type(text) is not str
        or len(text) > MAX_SOURCE_BYTES
        or not text.isascii()
    ):
        raise ValueError("invalid bounded ASCII source")
    # Count before splitting/expanding. Only LF and CRLF record endings.
    if text.count("\n") > maximum or text.count("\r") > maximum:
        raise ValueError("source row bound exceeded")
    normalized = text.replace("\r\n", "\n")
    if "\r" in normalized:
        raise ValueError("bare CR is not an admitted record ending")
    normalized = normalized.removesuffix("\n")
    if not normalized:
        return ()
    lines = tuple(normalized.split("\n"))
    if len(lines) > maximum:
        raise ValueError("source row bound exceeded")
    if any(not line or len(line) > MAX_LINE_BYTES for line in lines):
        raise ValueError("blank/oversized source record")
    return lines


def _volume(value: str) -> int:
    if len(value) > 20 or not _INTEGER.fullmatch(value):
        raise ValueError("nominal volume must be a bounded integer")
    result = int(value)
    if not -(2**63) <= result <= 2**63 - 1:
        raise ValueError("nominal volume exceeds signed 64-bit bound")
    return result


def _time(value: str, period: str, *, tick: bool) -> int:
    length = 18 if tick else 15
    if (
        len(value) != length
        or value[8] != " "
        or not (value[:8] + value[9:]).isdigit()
    ):
        raise ValueError("invalid HistData timestamp grammar")
    if value[:6] != period:
        raise ValueError("source timestamp differs from declared source month")
    if not tick and value[-2:] != "00":
        raise ValueError("M1 timestamp must start a minute")
    instant = datetime(
        int(value[:4]),
        int(value[4:6]),
        int(value[6:8]),
        int(value[9:11]),
        int(value[11:13]),
        int(value[13:15]),
        int(value[15:]) * 1000 if tick else 0,
        tzinfo=timezone.utc,
    )
    delta = instant - _EPOCH
    # Fixed EST, not America/New_York and not a DST-ambiguous local datetime.
    milliseconds = (
        delta.days * 86400 + delta.seconds + 18000
    ) * 1000 + delta.microseconds // 1000
    result = milliseconds * 1_000_000
    if not 0 <= result <= 2**63 - 1:
        raise ValueError("source timestamp exceeds native nanosecond range")
    return result


def _parse_m1(text: str, period: str) -> tuple[_M1Row, ...]:
    result: list[_M1Row] = []
    previous = -1
    for ordinal, line in enumerate(_lines(text, MAX_REFERENCE_ROWS), 1):
        cells = line.split(";")
        if len(cells) != 6:
            raise ValueError("M1 requires exactly six semicolon fields")
        stamp = _time(cells[0], period, tick=False)
        if stamp <= previous:
            raise ValueError("M1 minutes must be unique and strictly ordered")
        previous = stamp
        prices = tuple(cells[1:5])
        opening, high, low, close = tuple(decimal_value(x) for x in prices)
        if not low <= min(opening, close) <= max(opening, close) <= high:
            raise ValueError("M1 OHLC bounds disagree")
        result.append(_M1Row(ordinal, stamp, prices, _volume(cells[5])))
    return tuple(result)


def _parse_ticks(
    text: str, policy: VendorM1PolicyV1
) -> tuple[tuple[_Tick, ...], int, int]:
    lines = _lines(text, MAX_TICKS)
    ticks: list[_Tick] = []
    previous = -1
    regressions = maximum = 0
    for ordinal, line in enumerate(lines, 1):
        cells = line.split(",")
        if len(cells) != 4:
            raise ValueError("T requires exactly four comma fields")
        stamp = _time(cells[0], policy.source_period, tick=True)
        bid, ask = decimal_value(cells[1]), decimal_value(cells[2])
        if ask < bid:
            raise ValueError(
                "raw negative spread is ineligible; no quote repair"
            )
        if stamp < previous:
            regressions += 1
            maximum = max(maximum, (previous - stamp) // 1_000_000)
            if (
                policy.tick_order != "bounded_regressions_event_time_row"
                or regressions > MAX_REGRESSIONS
                or maximum > MAX_REGRESSION_MS
            ):
                raise ValueError(
                    "tick timestamp regression exceeds declared policy"
                )
        previous = stamp
        ticks.append(
            _Tick(ordinal, stamp, cells[1], cells[2], _volume(cells[3]))
        )
    # Duplicate tick timestamps remain distinct; original row ordinal breaks
    # ties. No row is deduplicated or re-labelled as monotonic source order.
    return (
        tuple(sorted(ticks, key=lambda x: (x.time_ns, x.ordinal))),
        regressions,
        maximum,
    )


def _verify_native_ticks(text: str, ticks: tuple[_Tick, ...]) -> None:
    from histdatacom.histdata_ascii import parse_ascii_lines

    if ticks:
        # Actual unchanged public T parser, on the same admitted byte snapshot,
        # only after complete output reservation. Native rows retain file order.
        parsed = parse_ascii_lines("T", _lines(text, MAX_TICKS))
        expected = tuple(
            (x.time_ns // 1_000_000, float(x.bid), float(x.ask), x.volume)
            for x in sorted(ticks, key=lambda x: x.ordinal)
        )
        if parsed.rows != expected:
            raise ValueError(
                "native T parser projection differs from exact source"
            )


def _implementation_id() -> str:
    from histdatacom import histdata_ascii
    from histdatacom.data_quality import vendor_m1_contracts
    from histdatacom.synthetic import bars, contracts

    payload = {}
    for module in (histdata_ascii, vendor_m1_contracts, bars, contracts):
        path = Path(module.__file__ or "")
        payload[module.__name__] = hashlib.sha256(path.read_bytes()).hexdigest()
    payload[__name__] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    return (
        "vendor-m1-implementation:sha256:"
        + hashlib.sha256(canonical(payload).encode("ascii")).hexdigest()
    )


def load_histdata_m1_reference(
    path: str | Path,
    *,
    expected_sha256: str,
    symbol: str,
    source_period: str,
    available_at_ns: int | None = None,
) -> VendorM1ReferenceV1:
    """Snapshot exact reference CSV bytes; supplied availability is not attested."""
    return VendorM1ReferenceV1(
        symbol,
        source_period,
        _read_source(path, expected_sha256),
        expected_sha256,
        available_at_ns,
    )


def _exact_ohlc(ticks: list[_Tick]) -> tuple[str, ...]:
    return (
        ticks[0].bid,
        max(ticks, key=lambda x: decimal_value(x.bid)).bid,
        min(ticks, key=lambda x: decimal_value(x.bid)).bid,
        ticks[-1].bid,
    )


def _differences(
    reference: tuple[str, ...], ticks: tuple[str, ...], policy: VendorM1PolicyV1
) -> tuple[VendorM1DifferenceV1, ...]:
    values = []
    for name, vendor, tick in zip(OHLC, reference, ticks, strict=True):
        delta = bounded_fraction(decimal_value(tick) - decimal_value(vendor))
        values.append(
            VendorM1DifferenceV1(
                name,
                vendor,
                tick,
                policy.pip_size,
                rational_text(delta),
                rational_text(abs(delta)),
                rational_text(
                    bounded_fraction(delta / decimal_value(policy.pip_size))
                ),
            )
        )
    return tuple(values)


def _native_bars(
    ticks: tuple[_Tick, ...], tick_sha256: str, policy: VendorM1PolicyV1
) -> tuple[str, dict[int, Any]]:
    from histdatacom.synthetic.activity import ActivitySliceScope
    from histdatacom.synthetic.bars import (
        DerivedBarPolicyV1,
        derive_reconstruction_bars,
    )
    from histdatacom.synthetic.contracts import SyntheticEventV1

    native_policy = DerivedBarPolicyV1(
        intervals=("1m",),
        scopes=(ActivitySliceScope.OBSERVED,),
        max_bars=MAX_MINUTES,
        max_symbols=1,
        max_provenance_values=1,
        rounding_digits=15,
    )
    run_id = (
        "vendor-m1-validation-input:sha256:"
        + hashlib.sha256(
            canonical(
                {"tick_sha256": tick_sha256, "policy_id": policy.artifact_id}
            ).encode("ascii")
        ).hexdigest()
    )
    source_id = "histdata-ascii-t-reference:sha256:" + tick_sha256
    events = (
        SyntheticEventV1.observed(
            symbol=policy.symbol,
            event_time_ns=x.time_ns,
            event_sequence=x.ordinal,
            bid=float(x.bid),
            ask=float(x.ask),
            run_id=run_id,
            ensemble_member_id="validation-only",
            source_version_id=source_id,
            source_series_id=f"validation-only:ASCII:T:{policy.symbol}",
            source_period=policy.source_period,
            source_row_id=x.ordinal,
        )
        for x in ticks
    )
    bars = derive_reconstruction_bars(
        events,
        source_product_manifest_id=run_id,
        run_id=run_id,
        ensemble_member_id="validation-only",
        policy=native_policy,
        start_ns=policy.start_ns,
        end_ns=policy.end_ns,
    )
    return native_policy.policy_id, {x.bar_start_ns: x for x in bars}


def compare_histdata_m1_to_ticks(
    reference: VendorM1ReferenceV1,
    tick_path: str | Path,
    *,
    expected_tick_sha256: str,
    policy: VendorM1PolicyV1,
) -> VendorM1ReportV1:
    """Recompute actual bid comparison; never promote either input as evidence.

    Complete bins are geometric query eligibility only, not proof of complete
    provider support. Missing bins and partial query bins remain explicit.
    The native bar IDs name an in-memory computation, not a verified committed
    reconstruction product. Decimal agreement is computed independently.
    """
    require_sha(expected_tick_sha256)
    selected = readmit(policy, VendorM1PolicyV1)
    admitted = readmit(reference, VendorM1ReferenceV1)
    if (admitted.symbol, admitted.source_period) != (
        selected.symbol,
        selected.source_period,
    ):
        raise ValueError("reference and policy axes differ")
    tick_ascii = _read_source(tick_path, expected_tick_sha256)
    implementation = _implementation_id()
    ticks, regressions, maximum = _parse_ticks(tick_ascii, selected)
    if (
        ticks
        and selected.tick_available_at_ns is not None
        and selected.tick_available_at_ns < ticks[-1].time_ns
    ):
        raise ValueError(
            "tick availability precedes full snapshot latest event"
        )
    all_reference_rows = _parse_m1(
        admitted.source_ascii, admitted.source_period
    )
    reference_rows = {
        x.time_ns: x
        for x in all_reference_rows
        if x.time_ns < selected.end_ns
        and x.time_ns + MINUTE_NS > selected.start_ns
    }
    by_minute: dict[int, list[_Tick]] = {}
    selected_ticks = tuple(
        x for x in ticks if selected.start_ns <= x.time_ns < selected.end_ns
    )
    for tick in selected_ticks:
        by_minute.setdefault(tick.time_ns // MINUTE_NS * MINUTE_NS, []).append(
            tick
        )
    minutes = tuple(sorted(set(reference_rows) | set(by_minute)))
    if len(minutes) > MAX_MINUTES:
        raise ValueError("comparison minute bound exceeded")
    # Per minute <= 4 differences, each with three <=157-character exact
    # ratios, bounded labels/IDs/lexemes/counts. 8192 is a conservative ASCII
    # envelope reservation, including tuple/object framing; not an RSS claim.
    reserve_report(len(minutes))
    _verify_native_ticks(tick_ascii, ticks)
    native_policy_id, native = _native_bars(
        selected_ticks, expected_tick_sha256, selected
    )
    if set(native) != set(by_minute):
        raise ValueError("native bar minute inventory differs")
    outputs: list[VendorM1MinuteV1] = []
    tolerance = decimal_value(selected.rounding_tolerance_pips, positive=False)
    for minute in minutes:
        vendor = reference_rows.get(minute)
        support = by_minute.get(minute, [])
        tick_prices = _exact_ohlc(support) if support else None
        bar = native.get(minute)
        native_prices = None
        ordered_sha = None
        if bar is not None:
            assert tick_prices is not None
            expected = tuple(round(float(x), 15) for x in tick_prices)
            actual = tuple(getattr(bar, "bid_" + name) for name in OHLC)
            if (
                actual != expected
                or bar.event_count != len(support)
                or bar.observed_event_count != len(support)
                or bar.synthetic_event_count != 0
            ):
                raise ValueError("native bid OHLC/count projection differs")
            native_prices = tuple(repr(x) for x in actual)
            ordered = hashlib.sha256(b"vendor-m1-ordered-tick-rows-v1\n")
            for tick in support:
                ordered.update(f"{tick.ordinal}\n".encode("ascii"))
            ordered_sha = ordered.hexdigest()
        differences: tuple[VendorM1DifferenceV1, ...] = ()
        if minute < selected.start_ns or minute + MINUTE_NS > selected.end_ns:
            status = "partial_query_refused"
        elif vendor is None:
            status = "missing_m1"
        elif not support:
            status = "missing_tick_support"
        else:
            assert tick_prices is not None
            differences = _differences(vendor.prices, tick_prices, selected)
            deltas = tuple(abs(Fraction(x.pip_difference)) for x in differences)
            status = (
                "exact_match"
                if not any(deltas)
                else (
                    "bounded_rounding_match"
                    if max(deltas) <= tolerance
                    else "material_mismatch"
                )
            )
        outputs.append(
            VendorM1MinuteV1(
                minute,
                status,
                vendor.ordinal if vendor else None,
                vendor.prices if vendor else None,
                vendor.volume if vendor else None,
                len(support),
                tick_prices,
                ordered_sha,
                bar.bar_id if bar else None,
                native_prices,
                differences,
            )
        )
    report = VendorM1ReportV1(
        admitted.artifact_id,
        admitted.source_sha256,
        len(admitted.source_ascii),
        len(all_reference_rows),
        (
            all_reference_rows[-1].time_ns + MINUTE_NS
            if all_reference_rows
            else None
        ),
        admitted.available_at_ns,
        expected_tick_sha256,
        len(tick_ascii),
        selected,
        len(ticks),
        ticks[-1].time_ns if ticks else None,
        len(selected_ticks),
        regressions,
        maximum,
        native_policy_id,
        implementation,
        tuple(outputs),
    )
    # Detect ordinary changes during computation. Retained immutable snapshots
    # remain the evidence; this is not a hostile-process filesystem guarantee.
    if (
        _read_source(tick_path, expected_tick_sha256) != tick_ascii
        or _implementation_id() != implementation
    ):
        raise ValueError("source/implementation changed during comparison")
    if (
        readmit(reference, VendorM1ReferenceV1) != admitted
        or readmit(policy, VendorM1PolicyV1) != selected
    ):
        raise ValueError("caller inputs changed during comparison")
    return report


def replay_histdata_m1_comparison(
    expected: VendorM1ReportV1,
    reference_path: str | Path,
    tick_path: str | Path,
    *,
    expected_report_id: str,
    expected_reference_sha256: str,
    expected_tick_sha256: str,
) -> VendorM1ReportV1:
    """Freshly recompute both selected byte roots and every retained result.

    A fully resealed different history is a different valid report, not the
    independently selected report. No current-provider authenticity, complete
    corpus, same-provider independent market truth, or release attestation is
    implied by replay.
    """
    require_id(expected_report_id, "vendor-m1-report")
    require_sha(expected_reference_sha256)
    require_sha(expected_tick_sha256)
    admitted = readmit(expected, VendorM1ReportV1)
    if (
        admitted.report_id != expected_report_id
        or admitted.reference_sha256 != expected_reference_sha256
        or admitted.tick_sha256 != expected_tick_sha256
    ):
        raise ValueError(
            "report/source roots differ from independently selected roots"
        )
    reference = load_histdata_m1_reference(
        reference_path,
        expected_sha256=expected_reference_sha256,
        symbol=admitted.policy.symbol,
        source_period=admitted.policy.source_period,
        available_at_ns=admitted.reference_available_at_ns,
    )
    actual = compare_histdata_m1_to_ticks(
        reference,
        tick_path,
        expected_tick_sha256=expected_tick_sha256,
        policy=admitted.policy,
    )
    if actual != admitted:
        raise ValueError("actual source replay differs from expected report")
    if (
        readmit(expected, VendorM1ReportV1) != admitted
        or _read_source(reference_path, expected_reference_sha256)
        != reference.source_ascii
    ):
        raise ValueError("reference/report changed during replay")
    return actual


__all__ = [
    "VendorM1DifferenceV1",
    "VendorM1MinuteV1",
    "VendorM1PolicyV1",
    "VendorM1ReferenceV1",
    "VendorM1ReportV1",
    "compare_histdata_m1_to_ticks",
    "load_histdata_m1_reference",
    "replay_histdata_m1_comparison",
]
