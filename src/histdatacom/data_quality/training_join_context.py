"""Native context replay/refitting with explicit normalized-clock limitations."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import hashlib
from itertools import islice
from pathlib import Path
import re

from histdatacom.broker_capture.contracts import BrokerCaptureSessionManifestV1
from histdatacom.broker_capture.fingerprint_contracts import (
    BrokerDeliveryFingerprintV1,
)
from histdatacom.broker_capture.fingerprints import (
    fit_broker_delivery_fingerprint,
)
from histdatacom.data_quality.calendar_profiles import (
    calendar_profile_from_mapping,
)
from histdatacom.market_context.contracts import MarketContextTimelineV1
from histdatacom.forecasting.engine_runner import ForecastEngineSnapshotV1
from histdatacom.forecasting.feature_artifacts import FeatureArtifact
from histdatacom.forecasting.feature_contracts import FeatureKind
from histdatacom.forecasting.feature_forecasts import (
    ForecastFeatureSnapshotV1,
    forecast_feature_baseline,
)
from histdatacom.forecasting.feature_store import (
    FeatureMatrixSnapshotV1,
    VintageFeatureStoreV1,
)
from histdatacom.market_context.economic_calendar import (
    replay_economic_calendar_corpus,
)
from histdatacom.market_context.positioning import (
    CftcPositioningCorpusV1,
    CftcPositioningRawSourceV1,
    MAX_CFTC_SOURCE_BYTES,
    _select_symbol_mapping,
    build_cftc_positioning_corpus_from_sources,
)

from .training_contracts import training_json, training_load
from .training_join_contracts import (
    JoinFamily,
    JoinMeaning,
    JoinState,
    TrainingJoinColumnV1,
    TrainingJoinSourceV1,
)
from .training_join_sources import (
    _JoinAdapter,
    _JoinRecord,
    _bound_file,
    _configuration,
    _prefix,
    _semantics,
)
from .training_lineage import _VerifiedSource


def _owned_feature(path: str, verified: _VerifiedSource) -> FeatureArtifact:
    names = (
        verified.source.context_artifact_paths
        + verified.source.derived_artifact_paths
    )
    matches = [
        item.artifact
        for name, item in zip(names, verified.features)
        if name == path
    ]
    if len(matches) != 1:
        raise ValueError(
            "feature must have one exact verified source/ownership binding"
        )
    # Consume the bytes already verified with the complete ownership map,
    # never a second mutable-path interpretation of that same locator.
    return matches[0]


def _forecast_support(forecast: ForecastFeatureSnapshotV1) -> tuple[int, int]:
    starts = [forecast.model.trained_at_ns, forecast.cutoff.cutoff_at_ns]
    ends = [forecast.generated_at_ns + 1]
    for inputs in (forecast.inputs, forecast.model.training_inputs):
        starts.extend(p.start_ns for p in inputs.features.request.periods)
        for observation in inputs.features.observations:
            starts.extend(
                (
                    observation.period.start_ns,
                    observation.definition.known_at_ns,
                )
            )
            ends.append(observation.available_at_ns + 1)
            # A survey estimate of a future reference period is information
            # at survey availability, not observation of the future period.
            if observation.definition.kind is not FeatureKind.CONSENSUS:
                ends.append(observation.period.end_ns)
        for schedule in inputs.features.schedules:
            starts.append(schedule.known_at_ns)
            ends.append(schedule.known_at_ns + 1)
        calendar = inputs.calendar_inputs.calendar
        for release in calendar.releases:
            starts.append(release.available_at_ns)
            ends.append(release.available_at_ns + 1)
            if release.released_at_ns is not None:
                starts.append(release.reference_period_end_ns)
                ends.extend(
                    (
                        release.reference_period_end_ns,
                        release.released_at_ns + 1,
                    )
                )
        for survey in calendar.forecasts:
            starts.append(survey.available_at_ns)
            ends.append(survey.available_at_ns + 1)
    # Corpus coverage/scheduled future release clocks are scope metadata, not
    # future observed conditioning. All actual retained fitting history is kept.
    return min(starts), max(ends)


def _calendar(source: TrainingJoinSourceV1) -> _JoinAdapter:
    _configuration(source, keys=set())
    if len(source.paths) != 1 or source.evidence_json:
        raise ValueError(
            "calendar join requires one retained normalized corpus"
        )
    _bound_file(source.paths[0])
    corpus = replay_economic_calendar_corpus(source.paths[0])
    if len(corpus.releases) > 4096:
        raise ValueError("calendar join release inventory exceeds4096")

    def select(
        column: TrainingJoinColumnV1, cutoff: int
    ) -> tuple[_JoinRecord, ...]:
        _prefix(column, "calendar.")
        if (
            column.entity.series is None
            or column.entity.currency is None
            or column.entity.economy is None
        ):
            raise ValueError(
                "calendar columns require native series/currency/economy mapping"
            )
        if column.field not in (
            "actual_value",
            "occurrence",
            "scheduled_for_ns",
        ):
            raise ValueError("unsupported calendar scalar field")
        _semantics(
            column,
            (
                JoinMeaning.EVENT
                if column.field == "occurrence"
                else JoinMeaning.STATE
            ),
        )
        result = []
        for release in corpus.releases:
            if release.series_key != column.entity.series or (
                column.coordinate
                and release.reference_period != column.coordinate
            ):
                continue
            if (
                column.entity.currency != release.currency
                or column.entity.economy != release.economy_code
                or (
                    column.entity.symbol not in release.affected_symbols
                    and column.entity.currency
                    not in release.affected_currencies
                )
            ):
                continue
            if column.field == "scheduled_for_ns":
                time = release.available_at_ns
                value: float | int | None = release.scheduled_for_ns
            else:
                if release.released_at_ns is None:
                    continue
                time = release.released_at_ns
                value = (
                    1 if column.field == "occurrence" else release.actual_value
                )
            result.append(
                _JoinRecord(
                    time,
                    time,
                    time + 1,
                    release.available_at_ns,
                    min(time, release.reference_period_end_ns),
                    max(time, release.available_at_ns) + 1,
                    (release.release_id,),
                    (corpus.corpus_id, release.source_release_id),
                    release.schema_version,
                    "normalized_official_context_not_source_authenticity",
                    value,
                    (
                        JoinState.AVAILABLE
                        if value is not None
                        else JoinState.UNAVAILABLE
                    ),
                    "retained_calendar_release_or_schedule",
                )
            )
        return tuple(result)

    return _JoinAdapter(source, (corpus.corpus_id,), select)


def _vintage(
    source: TrainingJoinSourceV1, verified: _VerifiedSource
) -> _JoinAdapter:
    _configuration(source, keys=set())
    if (
        len(source.paths) != 1
        or source.evidence_json
        or source.paths[0] not in verified.source.derived_artifact_paths
    ):
        raise ValueError(
            "vintage matrix must be in complete606 derived inventory"
        )
    artifact = _owned_feature(source.paths[0], verified)
    if not isinstance(artifact, FeatureMatrixSnapshotV1):
        raise ValueError("vintage adapter requires a full feature matrix")
    matrix = artifact
    store = VintageFeatureStoreV1(matrix.observations, matrix.schedules)
    cache: dict[int, FeatureMatrixSnapshotV1] = {}

    def select(
        column: TrainingJoinColumnV1, cutoff: int
    ) -> tuple[_JoinRecord, ...]:
        _prefix(column, "calendar.")
        _semantics(column, JoinMeaning.SNAPSHOT)
        if (
            column.entity.currency is not None
            or column.entity.economy is not None
        ):
            raise ValueError(
                "generic macro matrix does not prove a currency/economy attribution"
            )
        columns = [c for c in matrix.request.columns if c.name == column.field]
        if len(columns) != 1 or column.entity.series != columns[0].feature_key:
            raise ValueError(
                "vintage column does not bind exact source feature key"
            )
        periods = [
            p for p in matrix.request.periods if p.label == column.coordinate
        ]
        if len(periods) != 1:
            raise ValueError("vintage reference period is not in sealed grid")
        definitions = [
            o.definition
            for o in matrix.observations
            if o.definition.feature_key == column.entity.series
            and o.definition.known_at_ns <= cutoff
            and o.available_at_ns <= cutoff
        ]
        if not definitions:
            # A requested empty key has no authoritative macro classification.
            # Preserve a generic null instead of inventing a source origin.
            return ()
        if any(d.kind is not FeatureKind.MACRO for d in definitions):
            raise ValueError(
                "calendar namespace cannot relabel nonmacro observations"
            )
        if cutoff > matrix.request.cutoff_at_ns:
            return ()
        if cutoff not in cache:
            if len(cache) >= 257:
                raise ValueError("vintage join cutoff cache exceeds bound")
            cache[cutoff] = store.snapshot(
                replace(matrix.request, cutoff_at_ns=cutoff)
            )
        historical = cache[cutoff]
        cell = next(
            c
            for c in historical.cells
            if c.column == column.field and c.period == periods[0]
        )
        observations = [
            o
            for o in historical.observations
            if o.observation_id in cell.observation_ids
        ]
        ids = list(cell.observation_ids)
        ids.extend(str(o.definition.to_dict()["id"]) for o in observations)
        available = max(
            (
                max(o.available_at_ns, o.definition.known_at_ns)
                for o in observations
            ),
            default=None,
        )
        if cell.schedule_id is not None:
            schedule = next(
                s
                for s in historical.schedules
                if s.to_dict()["id"] == cell.schedule_id
            )
            ids.append(cell.schedule_id)
            available = max(available or 0, schedule.known_at_ns)
        lo = min(
            (o.period.start_ns for o in observations),
            default=cell.period.start_ns,
        )
        hi = max(
            (o.period.end_ns for o in observations), default=cell.period.end_ns
        )
        reason = cell.status.value
        state = (
            JoinState.AVAILABLE
            if cell.value is not None
            else (
                JoinState.INSUFFICIENT_WARMUP
                if "warmup" in reason or "support" in reason
                else JoinState.UNAVAILABLE
            )
        )
        return (
            _JoinRecord(
                cutoff,
                cell.period.end_ns,
                cutoff + 1,
                available,
                lo,
                hi,
                tuple(dict.fromkeys(ids)) or (historical.snapshot_id,),
                tuple(
                    dict.fromkeys((matrix.snapshot_id, historical.snapshot_id))
                ),
                "histdatacom.feature-cell.v1",
                "normalized_macro_declared_covariate_not_official_fx_attribution",
                cell.value,
                state,
                reason,
            ),
        )

    return _JoinAdapter(source, (matrix.snapshot_id,), select)


def _forecast(
    source: TrainingJoinSourceV1, verified: _VerifiedSource
) -> _JoinAdapter:
    _configuration(source, keys=set())
    if (
        len(source.paths) != 1
        or len(source.evidence_json) > 1
        or source.paths[0] not in verified.source.derived_artifact_paths
    ):
        raise ValueError(
            "forecast requires complete606 feature forecast inventory"
        )
    artifact = _owned_feature(source.paths[0], verified)
    if not isinstance(artifact, ForecastFeatureSnapshotV1):
        raise ValueError(
            "forecast input must retain the complete feature envelope"
        )
    forecast = artifact
    if source.evidence_json:
        executed = ForecastEngineSnapshotV1.from_json(source.evidence_json[0])
        if (
            training_json(executed.to_dict()) != source.evidence_json[0]
            or executed.forecast != forecast
        ):
            raise ValueError(
                "executed engine receipt differs from joined feature forecast"
            )
        execution_id = executed.snapshot_id
    else:
        expected = forecast_feature_baseline(
            forecast.model,
            forecast.inputs,
            cutoff=forecast.cutoff,
            target=forecast.target,
            generated_at_ns=forecast.generated_at_ns,
        )
        if expected != forecast:
            raise ValueError(
                "forecast differs from native executable baseline replay"
            )
        execution_id = forecast.model.model_id
    roots = (forecast.snapshot_id, forecast.model.model_id, execution_id)
    support_start, support_end = _forecast_support(forecast)
    releases = [
        r
        for r in forecast.inputs.calendar_inputs.calendar.releases
        if r.release_id == forecast.cutoff.schedule_release_id
    ]
    if len(releases) != 1:
        raise ValueError("forecast lacks exact pre-release metadata ownership")
    release = releases[0]

    def select(
        column: TrainingJoinColumnV1, cutoff: int
    ) -> tuple[_JoinRecord, ...]:
        _prefix(column, "forecast.")
        _semantics(column, JoinMeaning.STATE)
        if (
            column.entity.series != forecast.target.series_id
            or column.coordinate != forecast.target.reference_period
        ):
            raise ValueError("forecast target series/reference period differs")
        if (
            column.entity.currency != release.currency
            or column.entity.economy != release.economy_code
            or (
                column.entity.symbol not in release.affected_symbols
                and column.entity.currency not in release.affected_currencies
            )
        ):
            raise ValueError(
                "forecast column invents native event entity attribution"
            )
        if column.field != "point":
            raise ValueError(
                "forecast adapter exposes point with full distribution lineage only"
            )
        generated = forecast.generated_at_ns
        return (
            _JoinRecord(
                generated,
                generated,
                generated + 1,
                generated,
                support_start,
                support_end,
                (forecast.snapshot_id,),
                tuple(
                    dict.fromkeys(roots + (forecast.inputs.information_set_id,))
                ),
                "histdatacom.forecast-feature-snapshot.v1",
                "machine_forecast_not_actual_or_revision",
                forecast.distribution.point,
            ),
        )

    return _JoinAdapter(source, tuple(dict.fromkeys(roots)), select)


def _positioning(source: TrainingJoinSourceV1) -> _JoinAdapter:
    _configuration(source, keys=set())
    if len(source.paths) != 2 or source.evidence_json:
        raise ValueError(
            "CFTC adapter requires corpus and exact retained source directory"
        )
    # Rebuild native values from bounded, no-follow reads. The legacy replay
    # helper reads whole raw files before checking size, which is not this
    # consumer's resource boundary.
    content = _bound_file(source.paths[0])
    name = Path(source.paths[0]).name
    if (
        name
        != f"cftc-positioning-corpus-{hashlib.sha256(content).hexdigest()}.json"
    ):
        raise ValueError("CFTC corpus name differs from retained bytes")
    payload = training_load(content.decode())
    retained = CftcPositioningCorpusV1.from_dict(payload)
    if training_json(retained.to_dict()) != training_json(payload):
        raise ValueError("CFTC corpus has unknown/coercive native fields")
    restored = []
    total = 0
    for item in retained.sources:
        key = str(item.get("source_key", ""))
        digest = str(item.get("content_sha256", ""))
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", key) or not re.fullmatch(
            r"[0-9a-f]{64}", digest
        ):
            raise ValueError(
                "CFTC retained source identity is not a safe filename"
            )
        matches = tuple(
            islice(Path(source.paths[1]).glob(f"{key}-{digest}.*"), 2)
        )
        if len(matches) != 1:
            raise ValueError("CFTC source inventory is missing or ambiguous")
        remaining = retained.profile.max_total_source_bytes - total
        if remaining <= 0:
            raise ValueError("CFTC raw source total exceeds native byte bound")
        raw = _bound_file(
            str(matches[0]), limit=min(MAX_CFTC_SOURCE_BYTES, remaining)
        )
        total += len(raw)
        restored.append(CftcPositioningRawSourceV1.restore(item, raw))
    rebuilt = build_cftc_positioning_corpus_from_sources(
        restored, profile=retained.profile
    )
    if rebuilt.corpus.corpus_id != retained.corpus_id:
        raise ValueError("CFTC native raw source replay differs from corpus")
    corpus = rebuilt.corpus
    if len(corpus.snapshots) > 4096:
        raise ValueError("CFTC join snapshot budget exceeded")

    def select(
        column: TrainingJoinColumnV1, cutoff: int
    ) -> tuple[_JoinRecord, ...]:
        _prefix(column, "positioning.")
        _semantics(column, JoinMeaning.STATE)
        parts = column.coordinate.split(":")
        if len(parts) != 3 or column.entity.series != parts[2]:
            raise ValueError(
                "CFTC coordinate requires family:scope:contract and exact contract mapping"
            )
        mapping = _select_symbol_mapping(
            corpus,
            column.entity.symbol,
            datetime.fromtimestamp(
                cutoff // 1_000_000_000, timezone.utc
            ).date(),
        )
        if parts[2] not in mapping.contract_codes:
            raise ValueError(
                "CFTC contract is not mapped to the requested currency pair"
            )
        if (
            column.entity.currency is not None
            or column.entity.economy is not None
        ):
            raise ValueError(
                "CFTC futures mapping does not declare an economy attribution"
            )
        selected = [
            s
            for s in corpus.snapshots
            if (s.report_family.value, s.report_scope.value, s.contract_code)
            == tuple(parts)
        ]
        if not selected or any(column.field not in s.values for s in selected):
            raise ValueError(
                "CFTC field/coordinate is outside retained native schema"
            )
        return tuple(
            _JoinRecord(
                s.measurement_start_ns,
                s.measurement_start_ns,
                2**63 - 1,
                (
                    s.release_evidence.knowledge_at_ns
                    if s.strict_ex_ante_eligible
                    else None
                ),
                s.measurement_start_ns,
                max(
                    s.measurement_start_ns,
                    s.release_evidence.knowledge_at_ns
                    or s.measurement_start_ns,
                )
                + 1,
                (s.snapshot_id,),
                (corpus.corpus_id, s.source_id, mapping.mapping_id),
                s.schema_version,
                "official_futures_positioning_not_spot_volume",
                s.values[column.field],
                reason=(
                    "original_verified"
                    if s.strict_ex_ante_eligible
                    else "current_state_or_unproven_original_vintage"
                ),
            )
            for s in selected
        )

    return _JoinAdapter(source, (corpus.corpus_id,), select)


def _broker(source: TrainingJoinSourceV1) -> _JoinAdapter:
    config = training_load(source.configuration_json)
    if not set(config) <= {
        "calendar_profile_json",
        "market_context_timeline_json",
    }:
        raise ValueError("unknown broker fitting-context configuration")
    profile = None
    timeline = None
    for key, text in config.items():
        if type(text) is not str:
            raise ValueError(
                "broker fitting contexts require canonical native JSON"
            )
        if key == "calendar_profile_json":
            profile = calendar_profile_from_mapping(training_load(text))
            if training_json(profile.to_metadata()) != text:
                raise ValueError("noncanonical broker calendar profile")
        else:
            timeline = MarketContextTimelineV1.from_dict(training_load(text))
            if training_json(timeline.to_dict()) != text:
                raise ValueError("noncanonical broker market context")
    if not 2 <= len(source.paths) <= 32 or len(source.evidence_json) != 1:
        raise ValueError(
            "broker source requires root/session paths and one native fingerprint"
        )
    retained = BrokerDeliveryFingerprintV1.from_json(source.evidence_json[0])
    if training_json(retained.to_dict()) != source.evidence_json[0]:
        raise ValueError("noncanonical broker fingerprint")
    if retained.supersedes_fingerprint_id is not None:
        raise ValueError(
            "broker successor requires its retained predecessor; unsupported in this adapter version"
        )
    manifests = tuple(
        BrokerCaptureSessionManifestV1.from_json(_bound_file(path).decode())
        for path in source.paths[1:]
    )
    for path, manifest in zip(source.paths[1:], manifests):
        if training_json(
            training_load(_bound_file(path).decode())
        ) != training_json(manifest.to_dict()):
            raise ValueError("broker session has unknown/coercive wire fields")
    actual = fit_broker_delivery_fingerprint(
        source.paths[0],
        manifests,
        config=retained.fit_config,
        calendar_profile=profile,
        market_context_timeline=timeline,
        effective_start_utc_ns=retained.effective_start_utc_ns,
        effective_end_utc_ns=retained.effective_end_utc_ns,
    )
    if actual != retained:
        raise ValueError(
            "broker fingerprint differs from full native capture refit"
        )
    roots = (actual.fingerprint_id,) + tuple(
        e.manifest_id for e in actual.capture_evidence
    )

    def select(
        column: TrainingJoinColumnV1, cutoff: int
    ) -> tuple[_JoinRecord, ...]:
        _prefix(column, "broker_style.")
        _semantics(column, JoinMeaning.STATE)
        cells = [
            c for c in actual.cells if c.condition.key == column.coordinate
        ]
        if len(cells) != 1:
            raise ValueError(
                "broker condition is not in exact refitted fingerprint"
            )
        cell = cells[0]
        symbol = cell.condition.dimensions.get("symbol")
        captured_symbols = {
            c.condition.dimensions["symbol"]
            for c in actual.cells
            if "symbol" in c.condition.dimensions
        }
        if (
            symbol is not None and symbol != column.entity.symbol
        ) or column.entity.symbol not in captured_symbols:
            raise ValueError("broker fingerprint lacks declared symbol support")
        if any(
            v is not None
            for v in (
                column.entity.currency,
                column.entity.economy,
                column.entity.series,
            )
        ):
            raise ValueError(
                "broker delivery profile has no macro entity mapping"
            )
        metrics = [m for m in cell.metrics if m.name == column.field]
        if len(metrics) != 1:
            raise ValueError("broker metric is outside native fingerprint")
        metric = metrics[0]
        # Complete support and fit computation are required. Receive clocks
        # bound dependencies but no retained fitted-at availability receipt
        # exists: do not promote effective_start into causal fit availability.
        time = max(actual.effective_start_utc_ns, actual.support_end_utc_ns)
        return (
            _JoinRecord(
                time,
                time,
                actual.effective_end_utc_ns or 2**63 - 1,
                None,
                actual.support_start_utc_ns,
                actual.support_end_utc_ns + 1,
                (cell.cell_id, metric.metric_id),
                roots,
                metric.schema_version,
                "broker_observation_delivery_system_only",
                metric.estimate,
                (
                    JoinState.AVAILABLE
                    if metric.estimate is not None
                    else JoinState.UNAVAILABLE
                ),
                "native_capture_refit_unknown_fit_availability",
                valid_until_ns=actual.effective_end_utc_ns,
            ),
        )

    return _JoinAdapter(source, roots, select)


def prepare_context_join(
    source: TrainingJoinSourceV1, verified: _VerifiedSource
) -> _JoinAdapter:
    if source.family is JoinFamily.CALENDAR:
        return _calendar(source)
    if source.family is JoinFamily.VINTAGE:
        return _vintage(source, verified)
    if source.family is JoinFamily.FORECAST:
        return _forecast(source, verified)
    if source.family is JoinFamily.POSITIONING:
        return _positioning(source)
    if source.family is JoinFamily.BROKER:
        return _broker(source)
    raise ValueError("unsupported context join family")
