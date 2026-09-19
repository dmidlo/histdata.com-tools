"""Executable bridges from normalized calendar and qualified market artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace

from histdatacom.market_context.economic_calendar import (
    EconomicCalendarCorpusV1,
    EconomicCalendarForecastV1,
    EconomicCalendarReleaseV1,
    EconomicForecastKind,
    EconomicForecastScope,
)
from histdatacom.market_context.release_schedules import (
    EconomicReleaseScheduleEvidenceV1,
)
from histdatacom.synthetic.bars import DerivedBarV1

from .contracts import _json, _seal
from .feature_contracts import (
    FeatureDefinitionV1,
    FeatureEvidenceV1,
    FeatureKind,
    FeatureObservationV1,
    FeaturePeriodV1,
    FeatureScheduleV1,
    FeatureSourceMode,
)


def calendar_actual_feature_key(release: EconomicCalendarReleaseV1) -> str:
    """Series versions deliberately occupy separate, non-bridged columns."""
    return f"calendar.actual:{release.series_id}"


def calendar_consensus_feature_key(
    release: EconomicCalendarReleaseV1,
    forecast: EconomicCalendarForecastV1,
) -> str:
    return str(
        _seal(
            "calendar-consensus-feature",
            {
                "series_id": release.series_id,
                "source": forecast.source.name,
                "adapter": forecast.source.adapter_name,
                "scope": forecast.scope.value,
                "statistic": forecast.statistic.value,
            },
        )["id"]
    )


def _calendar_evidence(
    record: EconomicCalendarReleaseV1 | EconomicCalendarForecastV1,
) -> FeatureEvidenceV1:
    identity = (
        record.release_id
        if isinstance(record, EconomicCalendarReleaseV1)
        else record.forecast_id
    )
    return FeatureEvidenceV1(
        source_name=record.source.name,
        source_uri=f"urn:histdatacom:calendar:{identity}",
        source_sha256=record.source.content_sha256,
        source_record_id=identity,
        adapter_name="economic-calendar-feature-adapter",
        adapter_version="1.0",
        availability_basis="Normalized calendar available_at_ns; publication and retrieval remain separate.",
        retrieved_at_ns=record.source.retrieved_at_ns,
        mode=FeatureSourceMode.HISTORICAL_VINTAGE,
        record_json=_json(record.to_dict()),
    )


def calendar_feature_records(
    corpus: EconomicCalendarCorpusV1,
    periods: Mapping[tuple[str, str], FeaturePeriodV1],
    *,
    schedule_evidence: tuple[EconomicReleaseScheduleEvidenceV1, ...] = (),
) -> tuple[tuple[FeatureObservationV1, ...], tuple[FeatureScheduleV1, ...]]:
    """Adapt actuals and admissible event consensus, preserving full records.

    Period starts cannot be inferred from arbitrary provider period labels;
    callers supply an explicit (series_key, reference_period) interval map.
    If external schedule evidence is supplied for a release, its independent
    known-at clock is authoritative (and cannot precede release availability).
    """
    if not isinstance(corpus, EconomicCalendarCorpusV1):
        raise TypeError("calendar adapter requires a normalized corpus")
    evidence_by_release = {item.release_id: item for item in schedule_evidence}
    if len(evidence_by_release) != len(schedule_evidence):
        raise ValueError("duplicate schedule evidence for release")
    release_ids = {item.release_id for item in corpus.releases}
    if not set(evidence_by_release) <= release_ids:
        raise ValueError("schedule evidence references an absent release")
    observations: list[FeatureObservationV1] = []
    schedules: list[FeatureScheduleV1] = []
    latest: dict[tuple[str, FeaturePeriodV1], FeatureObservationV1] = {}
    event_releases: dict[str, list[EconomicCalendarReleaseV1]] = {}
    owners: dict[tuple[str, FeaturePeriodV1], str] = {}
    for release in sorted(
        corpus.releases,
        key=lambda item: (item.logical_event_key, item.revision_sequence),
    ):
        period = periods.get((release.series_key, release.reference_period))
        if period is None:
            continue
        if period.end_ns != release.reference_period_end_ns:
            raise ValueError(
                "calendar reference interval disagrees with release"
            )
        key = calendar_actual_feature_key(release)
        evidence = _calendar_evidence(release)
        external = evidence_by_release.get(release.release_id)
        known_at = release.available_at_ns
        if external is not None:
            known_at = max(known_at, external.known_at_ns)
            evidence = replace(
                evidence,
                record_json=_json(
                    {
                        "release": release.to_dict(),
                        "schedule_evidence": external.to_dict(),
                    }
                ),
            )
        schedules.append(
            FeatureScheduleV1(
                key, period, release.scheduled_for_ns, known_at, evidence
            )
        )
        event_releases.setdefault(release.logical_event_key, []).append(release)
        if release.actual_value is None:
            continue
        owner = owners.setdefault((key, period), release.logical_event_key)
        if owner != release.logical_event_key:
            raise ValueError(
                "multiple events/stages share one feature reference period"
            )
        definition = FeatureDefinitionV1(
            key,
            FeatureKind.MACRO,
            release.unit,
            release.scale,
            release.seasonality,
            f"calendar:{release.series_version}",
            release.frequency,
            release.available_at_ns,
            release.base,
            release.event_family.value,
        )
        previous = latest.get((key, period))
        observation = FeatureObservationV1(
            definition,
            period,
            release.actual_value,
            release.available_at_ns,
            release.released_at_ns,
            0 if previous is None else previous.vintage_sequence + 1,
            None if previous is None else previous.observation_id,
            _calendar_evidence(release),
        )
        observations.append(observation)
        latest[(key, period)] = observation
    for forecast in sorted(
        corpus.forecasts,
        key=lambda item: (item.available_at_ns, item.forecast_id),
    ):
        candidates = event_releases.get(forecast.logical_event_key)
        if (
            candidates is None
            or forecast.kind is not EconomicForecastKind.OBSERVED_CONSENSUS
            or forecast.scope
            not in {
                EconomicForecastScope.EVENT_CONSENSUS,
                EconomicForecastScope.OFFICIAL_PROFESSIONAL_SURVEY_EVENT_TARGET,
            }
        ):
            continue
        visible = [
            item
            for item in candidates
            if item.available_at_ns <= forecast.available_at_ns
        ]
        if not visible:
            raise ValueError(
                "consensus metadata has no release vintage visible at survey availability"
            )
        release = max(visible, key=lambda item: item.revision_sequence)
        period = periods[(release.series_key, release.reference_period)]
        key = calendar_consensus_feature_key(release, forecast)
        owner = owners.setdefault((key, period), release.logical_event_key)
        if owner != release.logical_event_key:
            raise ValueError(
                "multiple consensus events share one feature reference period"
            )
        definition = FeatureDefinitionV1(
            key,
            FeatureKind.CONSENSUS,
            forecast.unit,
            forecast.scale,
            release.seasonality,
            f"{forecast.scope.value}:{forecast.statistic.value}:{release.series_version}",
            release.frequency,
            forecast.available_at_ns,
            forecast.base,
            release.event_family.value,
        )
        previous = latest.get((key, period))
        if (
            previous is not None
            and previous.available_at_ns == forecast.available_at_ns
        ):
            raise ValueError("ambiguous simultaneous consensus vintages")
        observation = FeatureObservationV1(
            definition,
            period,
            forecast.value,
            forecast.available_at_ns,
            forecast.produced_at_ns,
            0 if previous is None else previous.vintage_sequence + 1,
            None if previous is None else previous.observation_id,
            replace(
                _calendar_evidence(forecast),
                record_json=_json(
                    {
                        "forecast": forecast.to_dict(),
                        "metadata_release": release.to_dict(),
                    }
                ),
            ),
        )
        observations.append(observation)
        latest[(key, period)] = observation
    return tuple(observations), tuple(schedules)


def observed_bar_feature(
    bar: DerivedBarV1,
    *,
    definition: FeatureDefinitionV1,
    available_at_ns: int,
    evidence: FeatureEvidenceV1,
    field: str = "mid_close",
) -> FeatureObservationV1:
    """Explicitly qualify a complete observed-only bar; never infer its clock.

    A reconstruction product, final data-quality classification, synthetic
    bar, or bar closing time alone does not establish real-time availability.
    The supplied receipt must name this exact bar and retain its normalized
    bytes, with an independently established availability basis.
    """
    if (
        not isinstance(bar, DerivedBarV1)
        or not isinstance(definition, FeatureDefinitionV1)
        or not isinstance(evidence, FeatureEvidenceV1)
    ):
        raise TypeError(
            "market adapter requires typed bar, definition, evidence"
        )
    if field not in {"bid_close", "ask_close", "mid_close", "spread_close"}:
        raise ValueError("unsupported observed bar feature")
    if definition.kind is not FeatureKind.MARKET:
        raise ValueError("bar feature must have market semantics")
    if (
        bar.synthetic_event_count
        or bar.is_partial_start
        or bar.is_partial_end
        or bar.observed_event_count != bar.event_count
    ):
        raise ValueError("market feature requires a complete observed-only bar")
    if (
        evidence.mode is not FeatureSourceMode.OBSERVED
        or evidence.source_record_id != bar.bar_id
        or evidence.record_json != _json(bar.to_dict())
    ):
        raise ValueError(
            "market availability receipt does not bind the exact observed bar"
        )
    period = FeaturePeriodV1(
        f"{bar.bar_start_ns}/{bar.bar_end_ns}", bar.bar_start_ns, bar.bar_end_ns
    )
    return FeatureObservationV1(
        definition,
        period,
        getattr(bar, field),
        available_at_ns,
        None,
        0,
        None,
        evidence,
    )
