"""Closed generated-source research replay, never campaign eligibility.

All scientific objects are recomputed from the observed catalog and a closed
native recipe. No caller fit, scenario, success flag or verification callback
is accepted. Replay does not write products, caches or fitting artifacts.
"""

from __future__ import annotations

import hashlib
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .training_contracts import (
    TrainingRootV1,
    TrainingSourceV1,
    TrainingVerificationLevel,
    training_json,
    training_load,
)
from .training_lineage import (
    read_training_regular,
    verify_training_ownership,
    verify_training_source,
)
from .training_provider_policy import require_training_derivation

RECIPE_SCHEMA = "histdatacom.training-scenario-research-recipe.v1"
RECIPE_PREFIX = "training-scenario-research-recipe:sha256:"
RESEARCH_SCOPE = "source_replayed_unqualified_native_scenario_research"
MAX_RECIPE_BYTES = 131_072
MAX_SOURCE_PARTITIONS = 24
MAX_SOURCE_ROWS = 4096
MAX_SOURCE_BYTES = 32 * 1024**2
MAX_OUTPUT_ROWS = 16_384
MAX_RESEARCH_MEMBERS = 32
MAX_INPUT_FILES = 512
MAX_INPUT_BYTES = 64 * 1024**2
_CONFIG_FIELDS = (
    "feed_epoch_fit_config",
    "observation_fit_config",
    "uncertainty_policy",
    "transition_policy",
    "marked_config",
    "carving_constraints",
    "cross_currency_config",
    "storage_policy",
)
_FIELDS = frozenset(
    (
        "schema_version",
        "dataset_version_id",
        "start_ns",
        "end_ns",
        "calibration_periods",
        "base_seed",
        "paths_per_cell",
        *_CONFIG_FIELDS,
    )
)


def research_recipe_id(recipe_json: str) -> str:
    """Identify exact canonical recipe bytes; identity alone is not authority."""
    if type(recipe_json) is not str or len(recipe_json) > MAX_RECIPE_BYTES:
        raise ValueError("research recipe exceeds its bounded text contract")
    data = training_load(recipe_json)
    if training_json(data) != recipe_json or set(data) != _FIELDS:
        raise ValueError(
            "research recipe must use the exact canonical field set"
        )
    if data["schema_version"] != RECIPE_SCHEMA:
        raise ValueError("unsupported research recipe schema")
    return (
        RECIPE_PREFIX + hashlib.sha256(recipe_json.encode("ascii")).hexdigest()
    )


def _admit(binding: Any, source: TrainingSourceV1) -> dict[str, Any]:
    from histdatacom.data_analytics import FeedEpochFitConfigV2
    from histdatacom.synthetic.carving import HistoricalCarvingConstraintSetV1
    from histdatacom.synthetic.cross_currency import (
        CrossCurrencyReconciliationConfigV1,
    )
    from histdatacom.synthetic.feed_epoch_transition import (
        FeedEpochTransitionPolicyV1,
    )
    from histdatacom.synthetic.marked_hawkes import MarkedHawkesConfigV1
    from histdatacom.synthetic.observation import ObservationOperatorFitConfigV1
    from histdatacom.synthetic.observation_uncertainty import (
        ObservationUncertaintyPolicyV1,
    )
    from histdatacom.synthetic.streaming import ReconstructionStoragePolicyV1

    from .training_scenario_contracts import (
        TrainingScenarioResearchBindingV1,
        readmit_scenario,
    )

    if type(binding) is not TrainingScenarioResearchBindingV1:
        raise TypeError("research replay requires its exact recipe binding")
    binding = readmit_scenario(binding, TrainingScenarioResearchBindingV1)
    identity = research_recipe_id(binding.recipe_json)
    if binding.expected_recipe_id != identity:
        raise ValueError(
            "research recipe differs from independently selected identity"
        )
    data: dict[str, Any] = training_load(binding.recipe_json)
    if data["dataset_version_id"] != source.dataset_version_id:
        raise ValueError("research recipe has a foreign observed dataset")
    for field in ("start_ns", "end_ns", "base_seed", "paths_per_cell"):
        if type(data[field]) is not int or not 0 <= data[field] < 2**63:
            raise ValueError(
                "research recipe requires exact nonnegative int64 fields"
            )
    if not data["start_ns"] < data["end_ns"] <= data["start_ns"] + 3600 * 10**9:
        raise ValueError("research generation interval exceeds one hour")
    if not 1 <= data["paths_per_cell"] <= 3:
        raise ValueError("research paths per crossed cell exceeds bound")
    periods = data["calibration_periods"]
    if (
        type(periods) is not list
        or not 2 <= len(periods) <= 8
        or any(
            type(p) is not str
            or len(p) != 6
            or not p.isascii()
            or not p.isdecimal()
            or not 1 <= int(p[4:]) <= 12
            or not 1900 <= int(p[:4]) <= 2200
            for p in periods
        )
        or periods != sorted(set(periods))
    ):
        raise ValueError(
            "research calibration periods must be a bounded exact inventory"
        )
    classes: tuple[Any, ...] = (
        FeedEpochFitConfigV2,
        ObservationOperatorFitConfigV1,
        ObservationUncertaintyPolicyV1,
        FeedEpochTransitionPolicyV1,
        MarkedHawkesConfigV1,
        HistoricalCarvingConstraintSetV1,
        CrossCurrencyReconciliationConfigV1,
        ReconstructionStoragePolicyV1,
    )
    for field, cls in zip(_CONFIG_FIELDS, classes):
        raw = data[field]
        if type(raw) is not dict:
            raise TypeError(
                "research native configuration requires an exact object"
            )
        native = cls.from_dict(raw)
        if training_json(native.to_dict()) != training_json(raw):
            raise ValueError(
                "research native configuration has unknown/coercive fields"
            )
        data[field] = native
    constraints = data["carving_constraints"]
    if (
        constraints.require_fingerprint_validation
        or constraints.require_complete_calendar_profile
    ):
        raise ValueError(
            "research recipe cannot invent fingerprint/calendar qualification"
        )
    total = (
        len(data["uncertainty_policy"].scenario_order)
        * len(data["transition_policy"].scenario_order)
        * data["paths_per_cell"]
    )
    if (
        total > MAX_RESEARCH_MEMBERS
        or total * data["marked_config"].limits.max_generated_events_per_window
        > MAX_OUTPUT_ROWS
    ):
        raise ValueError(
            "research complete crossed output reservation exceeds bound"
        )
    data["recipe_id"] = identity
    data["member_count"] = total
    return data


def _identity(path: Path) -> tuple[int, ...]:
    if not path.is_absolute():
        raise ValueError("research inputs require absolute paths")
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise ValueError(
                "research source path has a symbolic-link ancestor"
            )
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("research source must be a regular file")
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def _inputs(
    source: TrainingSourceV1,
) -> tuple[
    Any, tuple[Any, ...], tuple[tuple[str, int, str], ...], tuple[Any, ...]
]:
    from histdatacom.datasets import DatasetCatalog

    if source.context_artifact_paths or source.derived_artifact_paths:
        raise ValueError(
            "research v1 admits only declared observed source and native products"
        )
    catalog = DatasetCatalog.from_dict(training_load(source.catalog_json))
    version = next(
        v
        for v in catalog.versions
        if v.dataset_version_id == source.dataset_version_id
    )
    partitions = tuple(version.partitions)
    if (
        not 1 <= len(partitions) <= MAX_SOURCE_PARTITIONS
        or sum(p.row_count for p in partitions) > MAX_SOURCE_ROWS
        or sum(p.artifact.size_bytes or 0 for p in partitions)
        > MAX_SOURCE_BYTES
    ):
        raise ValueError(
            "research complete observed-source reservation exceeds bound"
        )
    refs = tuple(p.artifact for p in partitions) + tuple(
        version.qualification_evidence
    )
    if len(refs) > 48 or any(ref.size_bytes is None for ref in refs):
        raise ValueError(
            "research source references require complete bounded sizes"
        )
    if sum(ref.size_bytes or 0 for ref in refs) > MAX_SOURCE_BYTES:
        raise ValueError("research source reference bytes exceed bound")
    files, identities = [], []
    for ref in refs:
        path = Path(ref.path)
        if not path.is_absolute() or str(path) != ref.path:
            raise ValueError(
                "research source references require canonical absolute paths"
            )
        before = _identity(path)
        raw = read_training_regular(path, MAX_SOURCE_BYTES)
        digest = hashlib.sha256(raw).hexdigest()
        if (
            len(raw) != ref.size_bytes
            or digest != ref.sha256
            or _identity(path) != before
        ):
            raise ValueError(
                "research source differs from its retained strong reference"
            )
        files.append((str(path), len(raw), digest))
        identities.append(before)
    from histdatacom.synthetic.persistence import (
        RECONSTRUCTION_PRODUCT_V2_SCHEMA_VERSION,
        RECONSTRUCTION_PRODUCT_V3_SCHEMA_VERSION,
        ReconstructionProductManifestV2,
        ReconstructionProductManifestV3,
    )

    for name in source.product_manifest_paths:
        path = Path(name)
        before = _identity(path)
        raw = read_training_regular(path)
        data = training_load(raw.decode("utf-8"))
        if data.get("schema_version") not in (
            RECONSTRUCTION_PRODUCT_V2_SCHEMA_VERSION,
            RECONSTRUCTION_PRODUCT_V3_SCHEMA_VERSION,
        ):
            raise ValueError(
                "research accepts only native generic delivery products"
            )
        cls = (
            ReconstructionProductManifestV3
            if data.get("schema_version")
            == RECONSTRUCTION_PRODUCT_V3_SCHEMA_VERSION
            else ReconstructionProductManifestV2
        )
        manifest = cls.from_dict(data)
        if (
            training_json(manifest.to_dict()) != training_json(data)
            or _identity(path) != before
        ):
            raise ValueError(
                "research product snapshot has noncanonical fields or changed bytes"
            )
        files.append((name, len(raw), hashlib.sha256(raw).hexdigest()))
        identities.append(before)
        for part in manifest.partitions:
            selected = path.parent / part.relative_path
            current = _identity(selected)
            if (
                part.size_bytes > MAX_INPUT_BYTES
                or sum(f[1] for f in files) + part.size_bytes > MAX_INPUT_BYTES
                or len(files) >= MAX_INPUT_FILES
            ):
                raise ValueError(
                    "research product input reservation exceeds bound"
                )
            payload = read_training_regular(selected, part.size_bytes)
            digest = hashlib.sha256(payload).hexdigest()
            if (
                len(payload) != part.size_bytes
                or digest != part.byte_sha256
                or _identity(selected) != current
            ):
                raise ValueError(
                    "research product differs from declared partition bytes"
                )
            files.append((str(selected), len(payload), digest))
            identities.append(current)
        if isinstance(manifest, ReconstructionProductManifestV3):
            for segment in manifest.observed_anchor_segments:
                ref = segment.source_artifact
                if (
                    ref.size_bytes is None
                    or sum(f[1] for f in files) + ref.size_bytes
                    > MAX_INPUT_BYTES
                    or len(files) >= MAX_INPUT_FILES
                ):
                    raise ValueError(
                        "research portable-source reservation exceeds bound"
                    )
                selected = Path(ref.path)
                current = _identity(selected)
                payload = read_training_regular(selected, ref.size_bytes)
                digest = hashlib.sha256(payload).hexdigest()
                if (
                    len(payload) != ref.size_bytes
                    or digest != ref.sha256
                    or _identity(selected) != current
                ):
                    raise ValueError(
                        "research portable source differs from its strong reference"
                    )
                files.append((str(selected), len(payload), digest))
                identities.append(current)
    if (
        len(files) > MAX_INPUT_FILES
        or sum(f[1] for f in files) > MAX_INPUT_BYTES
    ):
        raise ValueError("research complete input inventory exceeds bound")
    return version, partitions, tuple(files), tuple(identities)


def _finish_inputs(
    files: tuple[tuple[str, int, str], ...], identities: tuple[Any, ...]
) -> None:
    for (name, size, digest), before in zip(files, identities):
        path = Path(name)
        raw = read_training_regular(path, min(MAX_INPUT_BYTES, size))
        if (
            _identity(path) != before
            or len(raw) != size
            or hashlib.sha256(raw).hexdigest() != digest
        ):
            raise ValueError("research source changed during native replay")


@dataclass(frozen=True)
class _ResearchCell:
    """Invocation-local actual kernel output, not persisted verification authority."""

    run: Any
    window: Any
    observation_kind: str
    transition_kind: str
    observation_id: str
    transition_id: str
    evidence_ids: tuple[str, ...]
    status: str
    reasons: tuple[str, ...]
    delivered: Any | None
    validation: Any | None
    anchors: tuple[Any, ...]
    scientific_json: str

    @property
    def benchmark_evidence(self) -> dict[str, Any]:
        return {
            "training_scenario_research": {
                "recipe_id": training_load(self.scientific_json)["recipe_id"],
                "scope": RESEARCH_SCOPE,
                "evidence_ids": list(self.evidence_ids),
                "scientific_payload_sha256": hashlib.sha256(
                    self.scientific_json.encode("ascii")
                ).hexdigest(),
                "scientific_payload_commitment": "complete-canonical-native-replay-payload.v1",
            },
            "scientific_promotion_evaluated": False,
            "candidate_promotion_eligible": False,
        }


def _benchmark(
    rows: tuple[Any, ...],
    definition: Any,
    *,
    run: Any | None = None,
    member: str | None = None,
) -> tuple[Any, ...]:
    from histdatacom.market_context import market_context_calendar_state
    from histdatacom.synthetic.benchmark import BenchmarkEventV1
    from histdatacom.synthetic.contracts import SyntheticEventV1

    output: list[Any] = []
    for row in rows:
        epoch = definition.assign(
            symbol=row.symbol, timestamp_utc_ms=row.event_time_ns // 1_000_000
        ).label
        session = market_context_calendar_state(row.event_time_ns).session_state
        if run is None:
            output.append(
                BenchmarkEventV1(
                    source_event_id=f"{row.series_id}|{row.period}|{row.row_id}",
                    symbol=row.symbol,
                    event_time_ns=row.event_time_ns,
                    event_sequence=0,
                    bid=row.bid,
                    ask=row.ask,
                    epoch_id=epoch,
                    session=session,
                    event_state="observed",
                    sparsity="generated-research-calibration",
                    anchor_id=None,
                )
            )
        else:
            if member is None:
                raise ValueError(
                    "research observed rows require an exact member"
                )
            output.append(
                SyntheticEventV1.observed(
                    symbol=row.symbol,
                    event_time_ns=row.event_time_ns,
                    event_sequence=0,
                    bid=row.bid,
                    ask=row.ask,
                    run_id=run.run_id,
                    ensemble_member_id=member,
                    source_version_id=run.source_version_ids[0],
                    source_series_id=row.series_id,
                    source_period=row.period,
                    source_row_id=row.row_id,
                )
            )
    return tuple(output)


def _execute(
    verified: Any, recipe: dict[str, Any], partitions: tuple[Any, ...]
) -> tuple[_ResearchCell, ...]:
    from histdatacom.data_analytics import (
        fit_active_time_feed_epochs,
        scan_active_time_evidence,
    )
    from histdatacom.market_context import market_context_calendar_state
    from histdatacom.synthetic.event_clock import EventClockCalibrationWindowV1
    from histdatacom.synthetic.historical_conditioning import (
        historical_product_observation_conditioning,
    )
    from histdatacom.synthetic.information import InformationMode
    from histdatacom.synthetic.marked_hawkes import (
        build_fitted_marked_hawkes_generator,
        fit_marked_hawkes_challenger,
    )
    from histdatacom.synthetic.observation import fit_observation_operator
    from histdatacom.synthetic.observation_calibration import (
        _build_fit_evidence,
        _build_targets,
        _fit_source_evidence,
    )
    from histdatacom.synthetic.observation_uncertainty import (
        build_observation_uncertainty_ensemble,
    )
    from histdatacom.synthetic.streaming import (
        ReconstructionRunV1,
        ReconstructionWindowV1,
    )

    selected = tuple(
        r
        for r in verified.observed
        if recipe["start_ns"] <= r.event_time_ns < recipe["end_ns"]
    )
    if (
        recipe["member_count"]
        * (
            len(selected)
            + recipe["marked_config"].limits.max_generated_events_per_window
        )
        > MAX_OUTPUT_ROWS
    ):
        raise ValueError(
            "research observed plus generated output reservation exceeds bound"
        )
    if any(
        sum(r.symbol == s for r in selected) < 2 for s in verified.graph_symbols
    ):
        raise ValueError(
            "research window lacks actual adjacent observed anchors"
        )
    if any(
        {r.symbol for r in verified.observed if r.period == period}
        != set(verified.graph_symbols)
        for period in recipe["calibration_periods"]
    ):
        raise ValueError(
            "research calibration period lacks complete actual symbols"
        )
    evidence = tuple(
        scan_active_time_evidence(
            p.artifact.path,
            symbol=p.symbol,
            period=p.period,
            config=recipe["feed_epoch_fit_config"],
        )
        for p in partitions
    )
    definition = fit_active_time_feed_epochs(
        evidence, config=recipe["feed_epoch_fit_config"]
    )
    if not definition.valid_for_observation_models:
        raise ValueError(
            "research native feed fit genuinely lacks stable epochs"
        )
    source_evidence = _fit_source_evidence(
        tuple(e for e in evidence if e.period in recipe["calibration_periods"]),
        epoch_definition=definition,
        reference_epoch_label=definition.epochs[-1].label,
        calibration_end_period=max(recipe["calibration_periods"]),
    )
    targets = _build_targets(
        source_evidence,
        epoch_definition=definition,
        reference_epoch_label=definition.epochs[-1].label,
        calibration_end_period=max(recipe["calibration_periods"]),
        rounding_digits=recipe["observation_fit_config"].rounding_digits,
    )
    corpus_hash = (
        "sha256:"
        + hashlib.sha256(
            training_json([e.to_dict() for e in source_evidence]).encode()
        ).hexdigest()
    )
    fit_evidence = _build_fit_evidence(
        targets, epoch_definition=definition, corpus_hash=corpus_hash
    )
    operator = fit_observation_operator(
        fit_evidence,
        epoch_definition=definition,
        config=recipe["observation_fit_config"],
    )
    windows = []
    for period in recipe["calibration_periods"]:
        rows = tuple(r for r in verified.observed if r.period == period)
        if not rows or {r.symbol for r in rows} != set(verified.graph_symbols):
            raise ValueError(
                "research calibration period lacks complete actual symbols"
            )
        windows.append(
            EventClockCalibrationWindowV1(
                window_id=f"{recipe['recipe_id']}|calibration|{period}",
                start_ns=min(r.event_time_ns for r in rows),
                end_ns=max(r.event_time_ns for r in rows) + 1,
                events=_benchmark(rows, definition),
            )
        )
    config = recipe["marked_config"]
    fit = fit_marked_hawkes_challenger(config, tuple(windows))
    if fit.status.value != "fitted":
        raise ValueError("research native Marked fit genuinely refused")
    selected = tuple(
        r
        for r in verified.observed
        if recipe["start_ns"] <= r.event_time_ns < recipe["end_ns"]
    )
    if any(
        sum(r.symbol == s for r in selected) < 2 for s in verified.graph_symbols
    ):
        raise ValueError(
            "research window lacks actual adjacent observed anchors"
        )
    midpoint = (recipe["start_ns"] + recipe["end_ns"]) // 2
    epoch_ids = {
        definition.assign(
            symbol=s, timestamp_utc_ms=midpoint // 1_000_000
        ).label
        for s in verified.graph_symbols
    }
    if len(epoch_ids) != 1 or not next(iter(epoch_ids)).startswith(
        "transition:"
    ):
        raise ValueError(
            "research recipe requires one actual synchronized transition window"
        )
    epoch = next(iter(epoch_ids))
    calendar = market_context_calendar_state(midpoint)
    all_members = tuple(
        f"research-member-{i:02d}" for i in range(recipe["member_count"])
    )
    run = ReconstructionRunV1(
        symbols=verified.graph_symbols,
        source_version_ids=(verified.source.dataset_version_id,),
        configuration_ids=tuple(
            sorted(
                (
                    recipe["recipe_id"],
                    config.config_id,
                    recipe["carving_constraints"].constraint_set_id,
                    recipe["cross_currency_config"].config_id,
                )
            )
        ),
        ensemble_member_ids=all_members,
        base_seed=recipe["base_seed"],
        storage_policy=recipe["storage_policy"],
    )
    generator = build_fitted_marked_hawkes_generator(
        config, fit, ensemble_member_ids=all_members
    )
    cells = []
    width = (
        len(recipe["uncertainty_policy"].scenario_order)
        * recipe["paths_per_cell"]
    )
    for transition_index, transition_kind in enumerate(
        recipe["transition_policy"].scenario_order
    ):
        conditioning = historical_product_observation_conditioning(
            operator,
            feed_epoch_label=epoch,
            symbols=verified.graph_symbols,
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            used_at_ns=midpoint,
            feed_epoch_definition=definition,
            transition_policy=recipe["transition_policy"],
            transition_scenario_kind=transition_kind,
        )
        members = all_members[
            transition_index * width : (transition_index + 1) * width
        ]
        ensemble = build_observation_uncertainty_ensemble(
            recipe["uncertainty_policy"],
            conditioning,
            ensemble_members=tuple(
                (m, recipe["base_seed"] + all_members.index(m)) for m in members
            ),
            observed_counts={
                **{
                    s: sum(r.symbol == s for r in selected)
                    for s in verified.graph_symbols
                },
                "GLOBAL": len(selected),
            },
            session=calendar.session_state,
            maximum_missing_event_count=config.limits.max_generated_events_per_window,
            maximum_candidate_amplification=min(
                config.limits.max_candidate_amplification,
                run.storage_policy.max_candidate_amplification,
            ),
        )
        for member in members:
            observation = ensemble.scenario_for(member)
            assignment = ensemble.member_for(member)
            window = ReconstructionWindowV1(
                run.run_id,
                member,
                run.symbols,
                recipe["start_ns"],
                recipe["end_ns"],
            )
            anchors = _benchmark(selected, definition, run=run, member=member)
            ids = tuple(
                sorted(
                    (
                        recipe["recipe_id"],
                        definition.definition_id,
                        operator.operator_id,
                        fit.fit_id,
                        ensemble.ensemble_id,
                        observation.scenario_id,
                        str(conditioning["transition_scenario_id"]),
                    )
                )
            )
            if not ensemble.admitted:
                cells.append(
                    _ResearchCell(
                        run,
                        window,
                        observation.kind.value,
                        transition_kind.value,
                        observation.scenario_id,
                        str(conditioning["transition_scenario_id"]),
                        ids,
                        "refused",
                        tuple(sorted(ensemble.refusal_reasons)),
                        None,
                        None,
                        anchors,
                        training_json(
                            {
                                "scope": RESEARCH_SCOPE,
                                "recipe_id": recipe["recipe_id"],
                                "ensemble": ensemble.to_dict(),
                            }
                        ),
                    )
                )
                continue
            cells.append(
                _produce_cell(
                    recipe,
                    run,
                    window,
                    anchors,
                    fit,
                    generator,
                    conditioning,
                    observation,
                    assignment,
                    calendar,
                    epoch,
                    ids,
                )
            )
    if (
        sum(
            len(stream.events)
            for cell in cells
            if cell.delivered is not None
            for stream in cell.delivered.streams
        )
        > MAX_OUTPUT_ROWS
    ):
        raise ValueError("research actual complete output exceeds bound")
    return tuple(cells)


def _produce_cell(
    recipe: dict[str, Any],
    run: Any,
    window: Any,
    anchors: tuple[Any, ...],
    fit: Any,
    generator: Any,
    conditioning: dict[str, Any],
    observation: Any,
    assignment: Any,
    calendar: Any,
    epoch: str,
    ids: tuple[str, ...],
) -> _ResearchCell:
    from histdatacom.market_context import (
        MarketContextMissingReason,
        MarketContextQueryStatus,
        MarketContextQueryV1,
        MarketContextView,
    )
    from histdatacom.synthetic.benchmark import (
        BenchmarkEventV1,
        BenchmarkScenarioV1,
        BenchmarkSplitKind,
    )
    from histdatacom.synthetic.carving import carve_reconstruction_candidates
    from histdatacom.synthetic.contracts import SyntheticEventStreamV1
    from histdatacom.synthetic.cross_currency import (
        CrossCurrencyValidationStage,
        reconcile_cross_currency_window,
        validate_cross_currency_output,
    )
    from histdatacom.synthetic.delivery import project_modern_reference_delivery
    from histdatacom.synthetic.marked_hawkes import (
        build_marked_hawkes_candidate_batches,
    )

    scenario = BenchmarkScenarioV1(
        split_kind=BenchmarkSplitKind.PRODUCT_INPUT,
        epoch_id=epoch,
        severity_id="unqualified-research-scenario",
        observation_operator_id=str(conditioning["observation_operator_id"]),
        degradation_parameters={
            "runtime_role": "source_replayed_unqualified_research",
            "retention_probability": observation.retention_probability,
            "observation_scenario_id": observation.scenario_id,
            "observation_scenario_kind": observation.kind.value,
            "observation_path_seed": assignment.path_seed,
            "transition_scenario_id": conditioning["transition_scenario_id"],
            "transition_scenario_kind": conditioning[
                "transition_scenario_kind"
            ],
            "observation_conditioning_id": conditioning["conditioning_id"],
        },
    )
    benchmark = tuple(
        BenchmarkEventV1.from_synthetic_event(
            e,
            epoch_id=epoch,
            session=calendar.session_state,
            event_state="observed",
            sparsity="research-product-input",
        )
        for e in anchors
    )
    generation = generator.generate_with_evidence(
        benchmark,
        scenario=scenario,
        window=window,
        ensemble_member_id=window.ensemble_member_id,
    )
    reasons = []
    if generation.evidence.status.value in ("refused", "failed"):
        reasons.append(
            generation.evidence.failure_reason
            or generation.evidence.status.value
        )
    accepted: list[Any] = []
    if not reasons:
        batches = build_marked_hawkes_candidate_batches(
            run=run,
            window=window,
            config=recipe["marked_config"],
            fit_result=fit,
            generation_result=generation,
            observed_events=anchors,
            session_state=calendar.session_state,
            special_tags=calendar.special_tags,
            event_tags=calendar.event_tags,
        )
        for batch in batches:
            context = MarketContextQueryV1(
                timeline_id="research-deterministic-calendar-no-external-corpus",
                view=MarketContextView.EX_POST,
                start_ns=window.core_start_ns,
                end_ns=window.core_end_ns,
                as_of_ns=None,
                events=(),
                status=MarketContextQueryStatus.MISSING,
                missing_reason=MarketContextMissingReason.NO_MATCHING_EVENT,
                calendar_state=calendar,
                requested_symbols=(batch.symbol,),
                window_id=window.window_id,
            )
            anchor_ids = {
                batch.left_anchor_event_id,
                batch.right_anchor_event_id,
            }
            result = carve_reconstruction_candidates(
                run=run,
                window=window,
                candidate_batch=batch,
                observed_events=tuple(
                    e for e in anchors if e.event_id in anchor_ids
                ),
                market_context=context,
                constraints=recipe["carving_constraints"],
                fingerprint_evidence=None,
            )
            if result.status.value == "refused":
                reasons.append(
                    result.refusal_reason.value
                    if result.refusal_reason
                    else "native_carving_refused"
                )
            accepted.extend(result.accepted_events)
    delivered, validation, group = None, None, None
    if not reasons:
        streams = {
            s: SyntheticEventStreamV1.merge(
                run_id=run.run_id,
                ensemble_member_id=window.ensemble_member_id,
                symbol=s,
                observed_events=tuple(e for e in anchors if e.symbol == s),
                synthetic_events=tuple(e for e in accepted if e.symbol == s),
            )
            for s in run.symbols
        }
        group = reconcile_cross_currency_window(
            run=run,
            window=window,
            streams=streams,
            config=recipe["cross_currency_config"],
        )
        if group.status.value != "reconciled":
            reasons.extend(
                group.generation_validation.failure_reasons
                or ("native_cross_currency_refused",)
            )
        else:
            delivered = project_modern_reference_delivery(
                group,
                delivery_profile_id="modern-reference:unqualified-training-scenario-research-v1",
            )
            validation = validate_cross_currency_output(
                run=run,
                window=window,
                streams={s.symbol: s for s in delivered.streams},
                config=recipe["cross_currency_config"],
                stage=CrossCurrencyValidationStage.POST_BROKER,
                observed_anchors=anchors,
            )
            if not validation.passed:
                reasons.extend(validation.failure_reasons)
                delivered = None
    return _ResearchCell(
        run,
        window,
        observation.kind.value,
        str(conditioning["transition_scenario_kind"]),
        observation.scenario_id,
        str(conditioning["transition_scenario_id"]),
        tuple(
            sorted(
                (*ids, generation.evidence.evidence_id, scenario.scenario_id)
            )
        ),
        "refused" if reasons else "eligible",
        tuple(sorted(set(reasons))),
        delivered,
        validation,
        anchors,
        training_json(
            {
                "scope": RESEARCH_SCOPE,
                "recipe_id": recipe["recipe_id"],
                "evidence_ids": sorted(
                    (
                        *ids,
                        generation.evidence.evidence_id,
                        scenario.scenario_id,
                    )
                ),
                "conditioning": conditioning,
                "observation": observation.to_dict(),
                "assignment": assignment.to_dict(),
                "fit": fit.identity_payload(),
                "generation": generation.evidence.identity_payload(),
                "generation_scenario": scenario.to_dict(),
                "calendar": calendar.to_dict(),
                "constraints": recipe["carving_constraints"].to_dict(),
                "cross_currency_config": recipe[
                    "cross_currency_config"
                ].to_dict(),
                "final_validation": (
                    None if validation is None else validation.to_dict()
                ),
                "cross_reconciliation": (
                    None if group is None else group.to_dict()
                ),
            }
        ),
    )


def execute_training_scenario_research_recipe(
    source: TrainingSourceV1, binding: Any
) -> tuple[_ResearchCell, ...]:
    """Execute a declared native research recipe; fixture publishers may persist it.

    This is not a receipt, certification or materialization bypass. The consuming
    view always runs the same kernels afresh and compares retained products.
    """
    from .training_scenario_contracts import readmit_scenario

    source = readmit_scenario(source, TrainingSourceV1)
    if source.product_manifest_paths:
        raise ValueError(
            "research creation accepts only the observed source, not preexisting products"
        )
    recipe = _admit(binding, source)
    _, partitions, files, identities = _inputs(source)
    verified = verify_training_source(source)
    require_training_derivation(verified)
    cells = _execute(verified, recipe, partitions)
    _finish_inputs(files, identities)
    require_training_derivation(verified)
    return cells


def _compare_product(
    cell: _ResearchCell,
    manifest: Any,
    cells: tuple[_ResearchCell, ...],
    recipe: dict[str, Any],
) -> None:
    """Rebuild native scientific summaries, not only their selected labels."""
    from histdatacom.synthetic.persistence import (
        ReconstructionEnsembleManifestV1,
        _constraint_manifest,
        _delivery_quality_manifest,
        _source_manifest,
        estimate_reconstruction_retention,
    )

    if cell.delivered is None or cell.validation is None:
        raise ValueError(
            "research product comparison requires actual accepted output"
        )
    expected_delivery = cell.delivered.manifest
    for name in (
        "run_id",
        "window_id",
        "synchronization_unit_id",
        "ensemble_member_id",
        "delivery_profile_id",
    ):
        if getattr(manifest, name) != getattr(expected_delivery, name):
            raise ValueError(
                "research native product delivery coordinate differs"
            )
    if (
        manifest.symbol_group_id != cell.window.synchronization_unit_id
        or manifest.symbols != cell.run.symbols
    ):
        raise ValueError(
            "research native product synchronization scope differs"
        )
    events = tuple(
        e for stream in cell.delivered.streams for e in stream.events
    )
    source = _source_manifest(events, cell.anchors)
    constraints = _constraint_manifest(events)
    quality = _delivery_quality_manifest(
        cell.delivered,
        final_validation=cell.validation,
        benchmark_artifact_ids=cell.evidence_ids,
        benchmark_evidence=cell.benchmark_evidence,
        point_in_time_evidence_projection_ids=(),
        point_in_time_evidence_decision_ids=(),
        cross_series_constraint_bundle_ids=(),
        cross_series_constraint_window_ids=(),
        cross_series_constraint_decision_ids=(),
        projection_burden_report_ids=(),
        projection_burden_receipt_ids=(),
        projection_burden_status="not_claimed",
    )
    counts = {
        c.window.ensemble_member_id: sum(
            len(s.events) for s in c.delivered.streams
        )
        for c in cells
        if c.delivered is not None
    }
    retention = estimate_reconstruction_retention(
        run_id=cell.run.run_id,
        primary_member_id=min(counts),
        retained_member_event_counts=counts,
        estimated_partition_count=6 * len(counts),
        estimated_product_count=len(counts),
        storage_policy=recipe["storage_policy"],
    )
    ensemble = ReconstructionEnsembleManifestV1(
        run_id=cell.run.run_id,
        materialized_member_id=cell.window.ensemble_member_id,
        primary_member_id=retention.primary_member_id,
        retained_member_ids=retention.retained_member_ids,
        member_event_estimates=retention.member_event_counts,
        retention_plan_id=retention.plan_id,
    )
    for name, expected in (
        ("source", source),
        ("constraints", constraints),
        ("quality", quality),
        ("ensemble", ensemble),
        ("retention", retention),
    ):
        if training_json(getattr(manifest, name).to_dict()) != training_json(
            expected.to_dict()
        ):
            raise ValueError(
                f"research native product {name} differs from fresh replay"
            )


def replay_training_scenario_research(
    plan: Any,
) -> tuple[
    tuple[Any, ...],
    tuple[TrainingRootV1, ...],
    tuple[tuple[str, int, str], ...],
]:
    """Fresh native recipe and exact retained-product equality, without writes."""
    from .training_scenario_contracts import (
        ScenarioAxis,
        ScenarioAxisState,
        ScenarioMemberStatus,
        TrainingScenarioAxisV1,
        TrainingScenarioMemberV1,
        TrainingScenarioPlanV1,
        readmit_scenario,
    )

    plan = readmit_scenario(plan, TrainingScenarioPlanV1)
    if plan.research_binding is None or plan.campaign_binding is not None:
        raise ValueError(
            "research replay requires its exclusive closed binding"
        )
    recipe = _admit(plan.research_binding, plan.source)
    _, partitions, files, identities = _inputs(plan.source)
    verified = verify_training_ownership(plan.source, plan.ownership)
    require_training_derivation(verified)
    cells = _execute(verified, recipe, partitions)
    products = {
        (
            p.manifest.run_id,
            p.manifest.window_id,
            p.manifest.ensemble_member_id,
        ): p
        for p in verified.products
    }
    if len(products) != len(verified.products):
        raise ValueError(
            "research native product inventory has duplicate coordinates"
        )
    consumed, members = set(), []
    for cell in cells:
        key = (
            cell.run.run_id,
            cell.window.window_id,
            cell.window.ensemble_member_id,
        )
        product = products.get(key)
        product_ids: tuple[str, ...] = ()
        if cell.status == "eligible":
            if (
                product is None
                or cell.delivered is None
                or cell.validation is None
            ):
                raise ValueError("research expected native product is missing")
            expected_events = tuple(
                e.to_dict()
                for stream in cell.delivered.streams
                for e in stream.events
            )
            actual_events = tuple(e.to_dict() for e in product.events)

            def order(row: dict[str, Any]) -> tuple[Any, Any, Any, Any]:
                return (
                    row["symbol"],
                    row["event_time_ns"],
                    row["event_sequence"],
                    row["event_id"],
                )

            if training_json(
                sorted(expected_events, key=order)
            ) != training_json(sorted(actual_events, key=order)):
                raise ValueError(
                    "research retained events differ from fresh native production"
                )
            from histdatacom.synthetic.persistence import (
                ReconstructionProductManifestV2,
                ReconstructionProductManifestV3,
            )

            if type(product.manifest) not in (
                ReconstructionProductManifestV2,
                ReconstructionProductManifestV3,
            ):
                raise ValueError(
                    "research recipe rejects broker-derived products"
                )
            quality: Any = product.manifest.quality
            if quality.final_validation_id != cell.validation.validation_id:
                raise ValueError(
                    "research retained final validation differs from replay"
                )
            if training_json(quality.benchmark_evidence) != training_json(
                cell.benchmark_evidence
            ):
                raise ValueError(
                    "research retained scientific recipe lineage differs"
                )
            _compare_product(cell, product.manifest, cells, recipe)
            product_ids = (product.manifest.manifest_id,)
            consumed.add(key)
        elif product is not None:
            raise ValueError(
                "research refused cell has a fabricated retained product"
            )
        units = tuple(
            u
            for u in plan.ownership.units
            if u.start_ns <= cell.window.core_start_ns
            and cell.window.core_end_ns <= u.end_ns
        )
        if len(units) != 1:
            raise ValueError(
                "research generation must remain inside one actual evidence unit"
            )
        values = {
            ScenarioAxis.OBSERVATION_RETENTION: cell.observation_kind,
            ScenarioAxis.TRANSITION: cell.transition_kind,
            ScenarioAxis.PATH_REALIZATION: cell.window.ensemble_member_id,
            ScenarioAxis.ENGINE: "histdatacom.marked-hawkes."
            + recipe["marked_config"].excitation_structure.value,
            ScenarioAxis.DELIVERY_PROFILE: "modern-reference:unqualified-training-scenario-research-v1",
        }
        axes = tuple(
            TrainingScenarioAxisV1(
                axis,
                (
                    ScenarioAxisState.KNOWN
                    if axis in values
                    else ScenarioAxisState.NOT_APPLICABLE
                ),
                values.get(axis),
                cell.evidence_ids,
            )
            for axis in ScenarioAxis
        )
        members.append(
            TrainingScenarioMemberV1(
                units[0].artifact_id,
                cell.window.ensemble_member_id,
                cell.window.core_start_ns,
                cell.window.core_end_ns,
                (recipe["marked_config"].config_id,),
                (cell.run.run_id,),
                product_ids,
                axes,
                ScenarioMemberStatus(cell.status),
                cell.reasons,
                cell.evidence_ids,
            )
        )
    if consumed != set(products):
        raise ValueError("research source has out-of-recipe native products")
    _finish_inputs(files, identities)
    require_training_derivation(verified)
    projection = training_json(
        [
            member.to_dict()
            for member in sorted(members, key=lambda m: m.member_key)
        ]
    )
    root = TrainingRootV1(
        "scenario-research-native-replay",
        recipe["recipe_id"],
        hashlib.sha256(projection.encode()).hexdigest(),
        TrainingVerificationLevel.PRODUCT_REPLAY,
    )
    return tuple(sorted(members, key=lambda m: m.member_key)), (root,), files
