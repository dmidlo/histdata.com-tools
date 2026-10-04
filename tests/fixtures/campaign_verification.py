"""Invented native inputs for campaign *integrity*, never model promotion.

Every Arrow/source/context byte is generated locally.  The retained plans are
constructed with the ordinary native contracts, not the public scientific
planner: no experiment here purports to establish a historical model's fitness.
No reader, writer, carving function, or verification function is substituted.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from histdatacom.runtime_contracts import ArtifactRef

SECOND = 1_000_000_000
SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
PERIODS = ("201501", "201502", "201503", "201504", "201505", "201506")
PERIOD = PERIODS[-1]
START_NS = (
    int(datetime(2015, 6, 24, 13, tzinfo=timezone.utc).timestamp()) * SECOND
)
END_NS = START_NS + 2 * SECOND + 1
QUOTES = {
    "EURUSD": (1.1999, 1.2001),
    "GBPUSD": (1.4999, 1.5001),
    "EURGBP": (1.1999 / 1.5001, 1.2001 / 1.4999),
}
NONCLAIM = (
    "Generated integrity fixture; not acquired market history, a calibrated "
    "historical reconstruction, model-promotion evidence, or a trading claim."
)


def write_json(
    directory: Path,
    name: str,
    value: Any,
    *,
    kind: str,
    metadata: dict[str, Any] | None = None,
) -> ArtifactRef:
    """Retain exact canonical bytes and return the ordinary strong native ref."""
    from histdatacom.orchestration.reconstruction import artifact_ref_for_file

    payload = value.to_dict() if hasattr(value, "to_dict") else value
    content = (
        json.dumps(
            payload, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        + "\n"
    ).encode("utf-8")
    digest = hashlib.sha256(content).hexdigest()
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{name}-{digest}.json"
    target.write_bytes(content)
    return artifact_ref_for_file(target, kind=kind, metadata=metadata or {})


@dataclass(frozen=True)
class CampaignSources:
    root: Path
    artifacts: dict[str, ArtifactRef]
    definition: Any
    feed_evidence: tuple[Any, ...]
    market_context: Any
    positioning: Any
    observation_operator: Any


def build_campaign_sources(root: Path) -> CampaignSources:
    """Eighteen tiny IPC files; a declared two-regime raw-source calibration."""
    import polars as pl

    from histdatacom.data_analytics import (
        FeedEpochFitConfigV2,
        fit_active_time_feed_epochs,
        scan_active_time_evidence,
    )
    from histdatacom.market_context import (
        BankOfEnglandBankRateAdapterV1,
        EcbPolicyRateAdapterV1,
        FederalReserveFomcCalendarAdapterV1,
        MarketContextFetchProfileV1,
        MarketContextSourceSnapshotV1,
        build_market_context_corpus_from_snapshots,
        write_market_context_corpus,
    )
    from histdatacom.market_context.positioning import (
        build_cftc_positioning_corpus_from_sources,
        write_cftc_positioning_corpus,
    )
    from histdatacom.synthetic.observation import (
        ObservationFitEvidenceV1,
        fit_observation_operator,
        write_observation_operator,
    )
    from tests.unit.test_cftc_positioning import _profile, _sources

    source_root = root / "ASCII" / "T"
    # The first retained 4-month flat fixture was genuinely refused because
    # native stability requires a shared technology boundary. This replacement
    # declares two invented regimes before execution: Jan/Feb sparse, Mar-Jun
    # with an additional dense burst one hour outside the product core. Every
    # month has the same three core-near anchor times; only the first two are
    # inside the June product window. The 0.5 PELT penalty is the existing tiny
    # native unit-test profile, not a historical-calibration claim or search.
    config = FeedEpochFitConfigV2(
        feature_names=(
            "log_interarrival_median_ms",
            "burst_interval_rate",
        ),
        min_evidence_periods=6,
        min_segment_periods=2,
        penalty_multiplier=0.5,
    )
    evidence = []
    for period in PERIODS:
        start_ms = int(
            datetime(
                int(period[:4]), int(period[4:]), 24, 13, tzinfo=timezone.utc
            ).timestamp()
            * 1000
        )
        for symbol in SYMBOLS:
            bid, ask = QUOTES[symbol]
            times = (start_ms, start_ms + 2000, start_ms + 3000)
            if period >= "201503":
                times += tuple(
                    start_ms + 3_600_000 + i * 100 for i in range(12)
                )
            target = (
                source_root
                / symbol.lower()
                / period[:4]
                / str(int(period[4:]))
                / ".data"
            )
            target.parent.mkdir(parents=True, exist_ok=True)
            pl.DataFrame(
                {
                    "datetime": times,
                    "bid": [bid] * len(times),
                    "ask": [ask] * len(times),
                    "vol": [0] * len(times),
                },
                schema={
                    "datetime": pl.Int64,
                    "bid": pl.Float64,
                    "ask": pl.Float64,
                    "vol": pl.Int32,
                },
            ).write_ipc(target)
            evidence.append(
                scan_active_time_evidence(
                    target, symbol=symbol, period=period, config=config
                )
            )
    definition = fit_active_time_feed_epochs(tuple(evidence), config=config)
    artifacts = {
        "feed_epochs": write_json(
            root / "artifacts",
            "feed-epochs",
            definition,
            kind="feed_epoch_definition_v2",
            metadata={"definition_id": definition.definition_id},
        )
    }
    if not definition.valid_for_observation_models or tuple(
        item.right_period for item in definition.boundaries
    ) != ("201503",):
        raise AssertionError(
            "Native fit did not recover the declared March synthetic regime"
        )
    operator = fit_observation_operator(
        tuple(
            ObservationFitEvidenceV1.from_feed_epoch_evidence(item, definition)
            for item in evidence
        ),
        epoch_definition=definition,
    )
    artifacts["observation_operator"] = write_observation_operator(
        operator, root / "observation"
    )

    retrieved = (
        int(datetime(2030, 1, 1, tzinfo=timezone.utc).timestamp()) * SECOND
    )

    def snapshot(
        key: str, adapter: str, content: bytes, content_type: str
    ) -> Any:
        return MarketContextSourceSnapshotV1(
            source_key=key,
            source_name="Invented campaign integrity source",
            source_uri=f"https://example.invalid/{key}",
            retrieved_at_ns=retrieved,
            content=content,
            content_type=content_type,
            adapter_name=adapter,
            adapter_version="1.0",
            license_name="Synthetic test data",
            redistribution_allowed=True,
            redistribution_constraints=(
                "Never represent as acquired market evidence.",
            ),
            limitations=(NONCLAIM,),
            metadata={"synthetic_fixture": True},
        )

    daily = ["KEY,TIME_PERIOD,OBS_VALUE,TITLE"]
    day = date(2015, 6, 1)
    while day <= date(2015, 6, 30):
        daily.append(
            "FM.D.U2.EUR.4F.KR.MRR_RT.LEV,"
            f"{day.isoformat()},0.5,Main refinancing operations"
        )
        day += timedelta(days=1)
    market = build_market_context_corpus_from_snapshots(
        (
            snapshot(
                "ecb.policy-rate",
                EcbPolicyRateAdapterV1.adapter_name,
                ("\n".join(daily) + "\n").encode(),
                "text/csv",
            ),
            snapshot(
                "boe.bank-rate",
                BankOfEnglandBankRateAdapterV1.adapter_name,
                b'<table id="stats-table"><thead><tr><th>Date Changed</th><th>Rate</th></tr></thead>'
                b"<tbody><tr><td>04 Jun 15</td><td>0.5</td></tr></tbody></table>",
                "text/html",
            ),
            snapshot(
                "fed.fomc-calendar",
                FederalReserveFomcCalendarAdapterV1.adapter_name,
                b'<h4><a>2015 FOMC Meetings</a></h4><div class="fomc-meeting">'
                b'<div class="fomc-meeting__month"><strong>June</strong></div>'
                b'<div class="fomc-meeting__date">16-17</div></div>'
                b'<div class="fomc-meeting"><div class="fomc-meeting__month"><strong>June</strong></div>'
                b'<div class="fomc-meeting__date">29-30</div></div>',
                "text/html",
            ),
        ),
        profile=MarketContextFetchProfileV1(
            start_date="2015-06-01",
            end_date="2015-06-30",
            sources=("ecb", "boe", "fed"),
        ),
    )
    market_refs = write_market_context_corpus(market, root / "market-context")
    artifacts["market_context"] = market_refs["corpus"]
    # Existing parser fixture consists entirely of generated JSON/archive bytes;
    # no request, authentication, live provider, or claimed acquisition is used.
    positioning = build_cftc_positioning_corpus_from_sources(
        _sources(), profile=_profile()
    )
    positioning_refs = write_cftc_positioning_corpus(
        positioning, root / "positioning"
    )
    artifacts["cftc_positioning"] = positioning_refs["corpus"]
    return CampaignSources(
        root=source_root,
        artifacts=artifacts,
        definition=definition,
        feed_evidence=tuple(evidence),
        market_context=market.corpus,
        positioning=positioning.corpus,
        observation_operator=operator,
    )


def build_reference_inputs(
    root: Path, sources: CampaignSources
) -> tuple[Any, Any]:
    """Real TRAIN index and source-replayable, deliberately unqualified corpus."""
    from histdatacom.orchestration.reconstruction import artifact_ref_for_file
    from histdatacom.synthetic import benchmark_corpus as benchmark
    from histdatacom.synthetic.benchmark_gates import (
        load_default_benchmark_promotion_gate_policy,
    )
    from histdatacom.synthetic.motif_library import (
        MODERN_REFERENCE_MOTIF_LEAKAGE_SCHEMA_VERSION,
        MODERN_REFERENCE_MOTIF_MANIFEST_SCHEMA_VERSION,
        MODERN_REFERENCE_MOTIF_QUALIFICATION_SCHEMA_VERSION,
        ModernReferenceMotifProfileV1,
    )
    from histdatacom.synthetic.motifs import (
        ReferenceMotifIndexConfigV1,
        ReferenceMotifSourceEventV1,
        ReferenceMotifSourceWindowV1,
        ReferenceMotifSplitKind,
        ReferenceMotifSplitV1,
        build_reference_motif_index,
        reference_motif_condition_from_quotes,
    )

    def month_start(period: str) -> int:
        return (
            int(
                datetime(
                    int(period[:4]), int(period[4:]), 1, tzinfo=timezone.utc
                ).timestamp()
            )
            * SECOND
        )

    profile = ModernReferenceMotifProfileV1(
        split_periods={
            "train": ("201503",),
            "calibration": ("201504",),
            "validation": ("201505",),
            "final_holdout": ("201506",),
        },
        synchronized_windows_per_period=1,
        minimum_events_per_symbol=3,
        max_events_per_symbol=3,
    )
    splits = tuple(
        ReferenceMotifSplitV1(
            kind, month_start(period), month_start(next_period)
        )
        for kind, period, next_period in (
            (ReferenceMotifSplitKind.TRAIN, "201503", "201504"),
            (ReferenceMotifSplitKind.CALIBRATION, "201504", "201505"),
            (ReferenceMotifSplitKind.VALIDATION, "201505", "201506"),
            (ReferenceMotifSplitKind.FINAL_HOLDOUT, "201506", "201507"),
        )
    )
    train_windows = []
    for symbol in SYMBOLS:
        path = sources.root / symbol.lower() / "2015" / "3" / ".data"
        rows = benchmark._read_arrow_interval(
            path,
            start_ns=int(
                datetime(2015, 3, 24, 13, tzinfo=timezone.utc).timestamp()
            )
            * SECOND,
            end_ns=int(
                datetime(2015, 3, 24, 13, tzinfo=timezone.utc).timestamp()
            )
            * SECOND
            + 4 * SECOND,
            maximum=3,
        )
        # Source motif references point at the actual persisted bytes.
        events = tuple(
            ReferenceMotifSourceEventV1(
                event_time_ns=row.timestamp_ms * 1_000_000,
                event_sequence=index,
                bid=row.bid,
                ask=row.ask,
                source_row_id=row.row_id + 1,
            )
            for index, row in enumerate(rows)
        )
        condition = reference_motif_condition_from_quotes(
            symbol=symbol,
            feed_epoch_id=sources.definition.assign(
                symbol=symbol,
                timestamp_utc_ms=events[0].event_time_ns // 1_000_000,
            ).label,
            session_state="london",
            event_times_ns=tuple(item.event_time_ns for item in events),
            bids=tuple(item.bid for item in events),
            asks=tuple(item.ask for item in events),
            active_sessions=("london",),
            special_tags=("ordinary",),
        )
        train_windows.append(
            ReferenceMotifSourceWindowV1(
                source_series_id=f"synthetic-test-train:{symbol}:201503",
                period="201503",
                source_artifact=artifact_ref_for_file(
                    path, kind="histdata_ascii_tick_source"
                ),
                split_kind=ReferenceMotifSplitKind.TRAIN,
                condition=condition,
                events=events,
                first_known_at_ns=events[-1].event_time_ns,
                available_at_ns=events[-1].event_time_ns,
            )
        )
    index = build_reference_motif_index(
        tuple(train_windows),
        splits=splits,
        config=ReferenceMotifIndexConfigV1(min_cell_support=1),
    )
    sources.artifacts["motif_index"] = write_json(
        root / "artifacts",
        "modern-reference-motif-index",
        index,
        kind="modern_reference_motif_index_v1",
        metadata={"index_id": index.index_id},
    )
    gate = load_default_benchmark_promotion_gate_policy()
    gate_ref = write_json(
        root / "artifacts",
        "gate-policy",
        gate,
        kind="benchmark_promotion_gate_policy_v1",
    )
    bprofile = benchmark.ReverseDegradationCorpusProfileV1(
        split_periods={
            "calibration": "201503",
            "validation": "201504",
            "final_holdout": "201505",
        },
        synchronized_windows_per_split=1,
        window_duration_seconds=60,
        minimum_events_per_symbol=2,
        max_events_per_symbol=3,
        ensemble_member_ids=("fixture-a", "fixture-b"),
    )
    partitions = []
    windows = []
    for split, periods in bprofile.split_periods.items():
        period = periods[0]
        start = (
            int(
                datetime(
                    2015, int(period[4:]), 24, 13, tzinfo=timezone.utc
                ).timestamp()
            )
            * SECOND
        )
        selected = []
        hashes = {}
        rows_by_symbol = {}
        for symbol in SYMBOLS:
            path = (
                sources.root
                / symbol.lower()
                / "2015"
                / str(int(period[4:]))
                / ".data"
            )
            ref = artifact_ref_for_file(path, kind="histdata_ascii_tick_source")
            partition = benchmark.BenchmarkSourcePartitionV1(
                symbol=symbol,
                period=period,
                relative_path=path.relative_to(sources.root).as_posix(),
                size_bytes=ref.size_bytes,
                row_count=15,
                sha256=ref.sha256,
            )
            partitions.append(partition)
            selected.append(partition.partition_id)
            rows = benchmark._read_arrow_interval(
                path, start_ns=start, end_ns=start + 60 * SECOND, maximum=3
            )
            hashes[symbol] = benchmark._tick_rows_sha256(rows)
            rows_by_symbol[symbol] = rows
        windows.append(
            benchmark.BenchmarkWindowPartitionV1(
                split_kind=split,
                period=period,
                session="london",
                start_ns=start,
                end_ns=start + 60 * SECOND,
                epoch_label=sources.definition.assign(
                    symbol="EURUSD", timestamp_utc_ms=start // 1_000_000
                ).label,
                source_partition_ids=tuple(selected),
                symbol_event_counts=dict.fromkeys(SYMBOLS, 3),
                symbol_partition_sha256=hashes,
                event_state_counts=benchmark._tick_event_state_counts(
                    rows_by_symbol
                ),
                context_state="synthetic-fixture:unqualified",
                positioning_state="synthetic-fixture:unqualified",
                context_supported=False,
            )
        )
    corpus = benchmark.ReverseDegradationBenchmarkCorpusV1(
        profile=bprofile,
        sources=tuple(partitions),
        windows=tuple(windows),
        split_hashes=benchmark._split_hashes(windows),
        degradation_configs=benchmark._degradation_configs(
            sources.observation_operator.operator_id
        ),
        metric_registry=benchmark._required_metric_names(),
        dependency_artifacts={
            "feed_epochs": sources.artifacts["feed_epochs"],
            "observation_campaign": sources.artifacts["observation_operator"],
            "market_context": sources.artifacts["market_context"],
            "cftc_positioning": sources.artifacts["cftc_positioning"],
            "gate_policy": gate_ref,
        },
        feed_epoch_definition_id=sources.definition.definition_id,
        observation_operator_id=sources.observation_operator.operator_id,
        market_context_corpus_id=sources.market_context.corpus_id,
        cftc_positioning_corpus_id=sources.positioning.corpus_id,
        gate_policy_id=gate.policy_id,
        gate_policy_commit=benchmark.PREDECLARED_GATE_COMMIT,
        neighbor_leakage_count=0,
    )
    replay = benchmark.replay_reverse_degradation_benchmark_corpus(
        corpus, sources.root
    )
    if not replay["verified"]:
        raise AssertionError(
            "Generated corpus must replay its real source bytes"
        )
    sources.artifacts["benchmark_manifest"] = (
        benchmark.write_reverse_degradation_benchmark_corpus(
            corpus, root / "artifacts"
        )
    )
    library_id = (
        "synthetic-unqualified-motif-library:sha256:"
        + hashlib.sha256(index.to_json().encode()).hexdigest()
    )
    sources.artifacts["motif_manifest"] = write_json(
        root / "artifacts",
        "modern-reference-motif-manifest",
        {
            "schema_version": MODERN_REFERENCE_MOTIF_MANIFEST_SCHEMA_VERSION,
            "library_id": library_id,
            "profile": profile.to_dict(),
            "index_artifact": sources.artifacts["motif_index"].to_dict(),
            "limitations": [NONCLAIM],
            "candidate_promotion_eligible": False,
        },
        kind="modern_reference_motif_manifest_v1",
        metadata={"library_id": library_id},
    )
    sources.artifacts["motif_qualification"] = write_json(
        root / "artifacts",
        "modern-reference-motif-qualification",
        {
            "schema_version": MODERN_REFERENCE_MOTIF_QUALIFICATION_SCHEMA_VERSION,
            "library_id": library_id,
            "candidate_promotion_eligible": False,
            "candidate_provisional": True,
            "reason": "No scientific promotion campaign was executed.",
            "limitations": [NONCLAIM],
        },
        kind="modern_reference_motif_qualification_v1",
        metadata={"library_id": library_id},
    )
    sources.artifacts["motif_leakage_audit"] = write_json(
        root / "artifacts",
        "modern-reference-motif-leakage-audit",
        {
            "schema_version": MODERN_REFERENCE_MOTIF_LEAKAGE_SCHEMA_VERSION,
            "library_id": library_id,
            "indexed_splits": ["train"],
            "post_exclusion_cross_split_finding_count": 0,
            "retained_holdout_fragment_count": 0,
            "retained_nontrain_fragment_count": 0,
            "limitations": [NONCLAIM],
            "basis": "Only the three explicit TRAIN source windows were supplied to the real index builder.",
        },
        kind="modern_reference_motif_leakage_audit_v1",
        metadata={"library_id": library_id},
    )
    return index, profile


@dataclass(frozen=True)
class CampaignFixture:
    root: Path
    plan_set_path: Path
    support_map_path: Path
    plan: Any
    products: tuple[Any, ...]
    manifest_paths: tuple[Path, ...]
    sources: CampaignSources
    local_results: tuple[Any, ...]
    final_validations: tuple[Any, ...]


def build_campaign_fixture(root: Path) -> CampaignFixture:
    """Build the real, small native graph and two native committed products."""
    from histdatacom import reconstruction as public
    from histdatacom.datasets import DatasetOrigin, DatasetQueryScopeV1
    from histdatacom.orchestration.reconstruction import (
        ReconstructionStage,
        ReconstructionStageInvocationV1,
        ReconstructionWindowTaskV1,
        ReconstructionWorkflowRequestV1,
        artifact_ref_for_file,
    )
    from histdatacom.reconstruction_storage import (
        RECONSTRUCTION_STORAGE_ROOT_GUARD_ROLE,
        create_reconstruction_storage_root_guard,
    )
    from histdatacom.synthetic import reconstruction_plan as plans
    from histdatacom.synthetic.carving import HistoricalCarvingConstraintSetV1
    from histdatacom.synthetic.cross_currency import (
        eurusd_triangle_reconciliation_config,
    )
    from histdatacom.synthetic.delivery import ReconstructionDeliveryMode
    from histdatacom.synthetic.ensembles import (
        EnsembleCalibrationConfigV1,
        plan_reconstruction_ensemble,
    )
    from histdatacom.synthetic.generation import EmpiricalMotifGeneratorConfigV1
    from histdatacom.synthetic.information import (
        InformationMode,
        ReconstructionInformationPolicyV1,
    )
    from histdatacom.synthetic.persistence import (
        estimate_reconstruction_retention,
    )
    from histdatacom.synthetic.streaming import (
        ReconstructionStoragePolicyV1,
        ReconstructionWindowV1,
        ReconstructionResourceEstimateV1,
    )

    root = root.resolve()
    sources = build_campaign_sources(root)
    _, motif_profile = build_reference_inputs(root, sources)
    artifacts = sources.artifacts
    artifact_root = root / "artifacts"
    output_root = root / "output"
    scratch_root = root / "scratch"
    checkpoint_root = root / "checkpoints"
    _, guard_ref = create_reconstruction_storage_root_guard(
        output_root=output_root,
        scratch_root=scratch_root,
        artifact_root=artifact_root,
    )
    artifacts[RECONSTRUCTION_STORAGE_ROOT_GUARD_ROLE] = guard_ref
    inventory = plans._build_source_inventory(
        sources.root,
        definition=sources.definition,
        symbols=plans._symbols(SYMBOLS),
        periods=(PERIOD,),
        requested_start_ns=START_NS,
        requested_end_ns=END_NS,
    )
    artifacts["source_inventory"] = write_json(
        artifact_root,
        "reconstruction-source-inventory",
        inventory,
        kind=plans.SOURCE_INVENTORY_ARTIFACT_KIND,
        metadata={"inventory_id": inventory.inventory_id},
    )
    ledger = plans.current_histdata_reconstruction_scientific_ledger()
    artifacts["scientific_ledger"] = write_json(
        artifact_root,
        "reconstruction-scientific-ledger",
        ledger,
        kind=plans.RECONSTRUCTION_SCIENTIFIC_LEDGER_ARTIFACT_KIND,
        metadata={
            "ledger_id": ledger.ledger_id,
            "estimand_id": ledger.estimand.estimand_id,
            "scope": ledger.scope,
        },
    )
    evidence_policy = plans.ReconstructionEvidencePolicyV1()
    cross_policy = plans.CrossSeriesConstraintPolicyV1()
    artifacts["evidence_policy"] = write_json(
        artifact_root,
        "reconstruction-evidence-policy",
        evidence_policy,
        kind=plans.RECONSTRUCTION_EVIDENCE_POLICY_ARTIFACT_KIND,
        metadata={"policy_id": evidence_policy.policy_id},
    )
    artifacts["cross_series_constraint_policy"] = write_json(
        artifact_root,
        "cross-series-constraint-policy",
        cross_policy,
        kind=plans.CROSS_SERIES_CONSTRAINT_POLICY_ARTIFACT_KIND,
        metadata={"policy_id": cross_policy.policy_id},
    )
    constraints = HistoricalCarvingConstraintSetV1(
        fingerprint_constraint_id="synthetic-integrity-fixture:unqualified",
        require_fingerprint_validation=False,
        require_complete_calendar_profile=False,
    )
    configuration = plans.ReconstructionPlanConfigurationV1(
        delivery_mode=ReconstructionDeliveryMode.MODERN_REFERENCE,
        information_policy=ReconstructionInformationPolicyV1(
            InformationMode.EX_POST_RECONSTRUCTION
        ),
        generator_config=EmpiricalMotifGeneratorConfigV1(),
        carving_constraints=constraints,
        cross_currency_config=eurusd_triangle_reconciliation_config(),
        ensemble_config=EnsembleCalibrationConfigV1(
            member_count=2, retained_member_count=2
        ),
        storage_policy=ReconstructionStoragePolicyV1(),
        window_size_ns=END_NS - START_NS,
        left_halo_ns=sources.observation_operator.required_left_halo_ns,
        right_lookahead_ns=0,
        max_parallel_windows=1,
    )
    artifacts["configuration"] = write_json(
        artifact_root,
        "reconstruction-plan-configuration",
        configuration,
        kind=plans.PLAN_CONFIGURATION_ARTIFACT_KIND,
        metadata={"configuration_id": configuration.configuration_id},
    )
    catalog, catalog_path, _ = plans.build_legacy_histdata_catalog(
        sources.root,
        symbols=SYMBOLS,
        periods=(PERIOD,),
        qualification_evidence=(artifacts["feed_epochs"],),
        path=artifact_root / "dataset-catalog.json",
    )
    scope = DatasetQueryScopeV1(
        symbols=SYMBOLS, periods=(PERIOD,), origin=DatasetOrigin.OBSERVED
    )
    resolution = catalog.resolve(
        plans.CURRENT_EXPERIMENT_ALIAS, query_scope=scope
    )
    artifacts["dataset_catalog"] = artifact_ref_for_file(
        catalog_path, kind=plans.RECONSTRUCTION_EXPERIMENT_CATALOG_ARTIFACT_KIND
    )
    artifacts["dataset_resolution"] = write_json(
        artifact_root,
        "reconstruction-dataset-resolution",
        resolution,
        kind=plans.RECONSTRUCTION_EXPERIMENT_RESOLUTION_ARTIFACT_KIND,
        metadata={"dataset_version_id": resolution.dataset_version_id},
    )
    roles = (
        plans.ReconstructionExperimentRole.HISTORICAL_ANCHOR,
        plans.ReconstructionExperimentRole.PRODUCT_INPUT,
    )
    bindings = tuple(
        plans.ReconstructionExperimentArtifactBindingV1(
            name=name,
            domain=domain,
            artifact=artifacts[role],
            artifact_id=identity,
            artifact_identity_field=field,
            dataset_roles=roles,
            schema_versions=(schema,),
        )
        for name, domain, role, identity, field, schema in (
            (
                "scientific-ledger",
                "scientific-target",
                "scientific_ledger",
                ledger.ledger_id,
                "ledger_id",
                ledger.schema_version,
            ),
            (
                "configuration",
                "configuration",
                "configuration",
                configuration.configuration_id,
                "configuration_id",
                configuration.schema_version,
            ),
            (
                "feed-epochs",
                "evidence",
                "feed_epochs",
                sources.definition.definition_id,
                "definition_id",
                sources.definition.schema_version,
            ),
            (
                "market-context",
                "evidence",
                "market_context",
                sources.market_context.corpus_id,
                "corpus_id",
                sources.market_context.schema_version,
            ),
            (
                "cftc-positioning",
                "evidence",
                "cftc_positioning",
                sources.positioning.corpus_id,
                "corpus_id",
                sources.positioning.schema_version,
            ),
        )
    )
    experiment, experiment_ref = (
        plans.freeze_histdata_reconstruction_experiment(
            catalog_path=catalog_path,
            dataset_reference=plans.CURRENT_EXPERIMENT_ALIAS,
            query_scope=scope,
            resolution=resolution,
            roles=roles,
            output_directory=artifact_root / "experiments",
            artifact_bindings=bindings,
            evidence_policy_ids=(
                evidence_policy.policy_id,
                cross_policy.policy_id,
            ),
            preprocessing_ids=(configuration.configuration_id,),
            feature_schema_versions=(inventory.schema_version,),
            # These are declared gate/library identities, not passing results;
            # match the native planner's experiment binding exactly.
            benchmark_gate_ids=(
                plans.PREDECLARED_GATE_COMMIT,
                str(artifacts["motif_manifest"].metadata["library_id"]),
            ),
            limitations=(NONCLAIM,),
        )
    )
    if not plans.verify_reconstruction_experiment(experiment).verified:
        raise AssertionError("Generated native experiment must verify")
    artifacts["experiment_manifest"] = experiment_ref
    ensemble = plan_reconstruction_ensemble(
        symbols=SYMBOLS,
        source_artifact_hashes={
            resolution.dataset_version_id: resolution.manifest_sha256
        },
        configuration_artifact_hashes={
            configuration.configuration_id: artifacts["configuration"].sha256,
            configuration.information_policy.policy_id: plans._contract_sha256(
                configuration.information_policy
            ),
            configuration.generator_config.config_id: plans._contract_sha256(
                configuration.generator_config
            ),
            configuration.carving_constraints.constraint_set_id: plans._contract_sha256(
                configuration.carving_constraints
            ),
            configuration.cross_currency_config.config_id: plans._contract_sha256(
                configuration.cross_currency_config
            ),
        },
        base_seed=522,
        config=configuration.ensemble_config,
        storage_policy=configuration.storage_policy,
    )
    run = ensemble.run
    windows = tuple(
        ReconstructionWindowV1(
            run.run_id,
            member.member_id,
            SYMBOLS,
            START_NS,
            END_NS,
            left_halo_ns=configuration.left_halo_ns,
        )
        for member in ensemble.members
    )
    artifacts["ensemble_plan"] = write_json(
        artifact_root,
        "reconstruction-ensemble-plan",
        ensemble,
        kind="reconstruction_ensemble_plan_v1",
        metadata={"plan_id": ensemble.plan_id, "run_id": run.run_id},
    )
    information, audit = plans._build_information_evidence(
        run=run,
        policy=configuration.information_policy,
        windows=windows,
        artifacts=artifacts,
        motif_profile=motif_profile,
        requested_start_ns=START_NS,
        requested_end_ns=END_NS,
    )
    artifacts["information_manifest"] = write_json(
        artifact_root,
        "reconstruction-information-manifest",
        information,
        kind="reconstruction_information_manifest_v1",
        metadata={"manifest_id": information.manifest_id},
    )
    artifacts["information_audit"] = write_json(
        artifact_root,
        "reconstruction-information-audit",
        audit,
        kind="reconstruction_information_audit_v1",
        metadata={"audit_id": audit.audit_id},
    )
    retention = estimate_reconstruction_retention(
        run_id=run.run_id,
        primary_member_id=run.ensemble_member_ids[0],
        retained_member_event_counts={
            member: 32 for member in run.ensemble_member_ids
        },
        estimated_partition_count=6,
        estimated_product_count=2,
        storage_policy=run.storage_policy,
    )
    artifacts["retention_plan"] = write_json(
        artifact_root,
        "reconstruction-retention-plan",
        retention,
        kind="reconstruction_retention_plan_v1",
        metadata={"plan_id": retention.plan_id},
    )
    support = plans._build_exact_source_support(
        (windows[0],), inventory=inventory, cross_series_policy=cross_policy
    )
    source_support_map = plans.ReconstructionPlanSourceSupportMapV1(
        source_inventory_id=inventory.inventory_id,
        cross_series_policy_id=cross_policy.policy_id,
        windows=support,
    )
    artifacts["source_support_map"] = write_json(
        artifact_root,
        "reconstruction-plan-source-support-map",
        source_support_map,
        kind=plans.SOURCE_SUPPORT_MAP_ARTIFACT_KIND,
        metadata={"support_map_id": source_support_map.support_map_id},
    )
    execution = plans.ReconstructionPlanExecutionManifestV1(
        run_id=run.run_id,
        configuration_id=configuration.configuration_id,
        source_inventory_id=inventory.inventory_id,
        information_manifest_id=information.manifest_id,
        information_audit_id=audit.audit_id,
        ensemble_plan_id=ensemble.plan_id,
        retention_plan_id=retention.plan_id,
        delivery_mode=configuration.delivery_mode,
        artifacts=artifacts.copy(),
        output_root=str(output_root),
        checkpoint_root=str(checkpoint_root),
        scratch_root=str(scratch_root),
        planned_window_count=1,
        executable_window_count=1,
    )
    execution_ref = write_json(
        artifact_root,
        "reconstruction-plan-execution",
        execution,
        kind=plans.PLAN_EXECUTION_MANIFEST_ARTIFACT_KIND,
        metadata={"manifest_id": execution.manifest_id},
    )
    artifacts["execution_manifest"] = execution_ref
    estimate = ReconstructionResourceEstimateV1(
        6, 26, 2, 1, 32, 1_048_576, 1_048_576, 1_048_576, 1
    )
    requests = []
    for number, window in enumerate(windows):
        scratch = scratch_root / window.window_id.replace(":", "-")
        commands = plans._stage_commands(
            window,
            scratch=scratch,
            inventory=inventory,
            execution_manifest=execution,
            execution_ref=execution_ref,
        )
        task = ReconstructionWindowTaskV1(
            window, estimate, commands, str(scratch)
        )
        requests.append(
            ReconstructionWorkflowRequestV1(
                f"synthetic-integrity-{number}",
                run,
                (task,),
                str(checkpoint_root),
                str(output_root / "reports"),
                max_parallel_windows=1,
            )
        )
    resources = plans.ReconstructionPlanResourceSummaryV1(
        source_event_count=inventory.total_row_count,
        source_size_bytes=inventory.total_size_bytes,
        planned_window_count=1,
        executable_window_count=1,
        refused_window_count=0,
        ensemble_member_count=2,
        retained_member_count=2,
        workflow_request_count=2,
        estimated_input_event_count=12,
        estimated_candidate_event_count=52,
        estimated_candidate_bytes=26_624,
        estimated_peak_memory_bytes=1_048_576,
        estimated_peak_scratch_bytes=1_048_576,
        estimated_output_bytes=2_097_152,
        estimated_partition_count=6,
    )
    plan = plans.SyntheticInfillPlanV1(
        run=run,
        configuration_id=configuration.configuration_id,
        execution_manifest_id=execution.manifest_id,
        information_mode=configuration.information_policy.information_mode,
        delivery_mode=configuration.delivery_mode,
        requested_start_ns=START_NS,
        requested_end_ns=END_NS,
        workflow_requests=tuple(requests),
        artifact_graph=artifacts.copy(),
        resources=resources,
        source_support=support,
    )
    # This ordinary public writer performs the native execution-plan admission.
    plan_ref = plans.write_synthetic_infill_plan(plan, artifact_root)
    spec = public.ReconstructionPlanSpecV1(
        source_root=str(sources.root),
        **{
            f"{name}_path": artifacts[role].path
            for name, role in (
                ("feed_epoch_definition", "feed_epochs"),
                ("observation_operator", "observation_operator"),
                ("market_context_corpus", "market_context"),
                ("cftc_positioning_corpus", "cftc_positioning"),
                ("benchmark_manifest", "benchmark_manifest"),
                ("motif_manifest", "motif_manifest"),
                ("motif_index", "motif_index"),
                ("motif_qualification", "motif_qualification"),
                ("motif_leakage_audit", "motif_leakage_audit"),
            )
        },
        artifact_root=str(artifact_root),
        output_root=str(output_root),
        checkpoint_root=str(checkpoint_root),
        scratch_root=str(scratch_root),
        information_mode=configuration.information_policy.information_mode,
        start_period=PERIOD,
        end_period=PERIOD,
        requested_start_ns=START_NS,
        requested_end_ns=END_NS,
        window_size_ns=END_NS - START_NS,
    )
    shard = public.ReconstructionPlanShardV1(
        PERIOD,
        PERIOD,
        START_NS,
        END_NS,
        plan.plan_id,
        plan_ref,
        "ready",
        True,
        0,
        resources.to_dict(),
    )
    summaries, partitions = [], {}
    public._accumulate_plan_set_resources(
        plan, resource_summaries=summaries, source_partitions=partitions
    )
    plan_set = public.ReconstructionPlanSetV1(
        spec,
        (shard,),
        START_NS,
        END_NS,
        public._aggregate_plan_set_resources(summaries, partitions),
        "ready",
    )
    plan_set_ref = public.write_reconstruction_plan_set(plan_set, artifact_root)
    support_ref = public.ReconstructionClient().construct_plan_support_map(
        plan_set_ref.path, output_directory=artifact_root
    )
    products, local_results, validations = [], [], []
    for request in requests:
        task = request.tasks[0]
        command = next(
            item
            for item in task.commands
            if item.stage is ReconstructionStage.VALIDATION
        )
        invocation = ReconstructionStageInvocationV1(run, task, command, ())
        product, local, validation = _publish_member(invocation)
        products.append(product)
        local_results.extend(local)
        validations.append(validation)
    return CampaignFixture(
        root=root,
        plan_set_path=Path(plan_set_ref.path),
        support_map_path=Path(support_ref.path),
        plan=plan,
        products=tuple(products),
        manifest_paths=tuple(Path(item.manifest_path) for item in products),
        sources=sources,
        local_results=tuple(local_results),
        final_validations=tuple(validations),
    )


def _publish_member(invocation: Any) -> tuple[Any, tuple[Any, ...], Any]:
    """Execute native data-plane stages and persist truthful integrity evidence.

    This intentionally does not invoke the scientific-promotion handler.  It
    uses the ordinary publication API, which accepts real final validation and
    separately retained descriptive benchmark metadata.  No passing promotion
    field or stage outcome is fabricated.
    """
    from histdatacom.orchestration.reconstruction import (
        ReconstructionStage,
        ReconstructionStageInvocationV1,
        ReconstructionStageStatus,
    )
    from histdatacom.synthetic import reconstruction_handlers as handlers
    from histdatacom.synthetic.cross_currency import (
        CrossCurrencyConditionV1,
        CrossCurrencyValidationStage,
        validate_cross_currency_output,
    )
    from histdatacom.reconstruction_experiment import (
        read_reconstruction_experiment,
    )
    from histdatacom.synthetic.persistence import (
        ReconstructionRetentionPlanV1,
        commit_delivery_reconstruction_publication,
        stage_delivery_reconstruction_publication,
    )
    from histdatacom.synthetic.reconstruction_plan import (
        load_reconstruction_stage_plan,
    )

    stage_handlers = (
        (
            ReconstructionStage.SOURCE_ENRICHMENT,
            handlers.source_enrichment_handler,
        ),
        (ReconstructionStage.PROPOSAL, handlers.proposal_handler),
        (ReconstructionStage.CARVING, handlers.carving_handler),
        (
            ReconstructionStage.CROSS_SERIES_RECONCILIATION,
            handlers.cross_series_reconciliation_handler,
        ),
        (
            ReconstructionStage.BROKER_TRANSFER,
            handlers.delivery_projection_handler,
        ),
    )
    outcomes = []
    for stage, handler in stage_handlers:
        command = next(
            item for item in invocation.task.commands if item.stage is stage
        )
        current = ReconstructionStageInvocationV1(
            invocation.run, invocation.task, command, tuple(outcomes)
        )
        outcome = handler(current)
        # Preserve the native outcome before asserting, including a refusal.
        write_json(
            Path(invocation.task.scratch_directory) / "fixture-outcomes",
            stage.value,
            outcome,
            kind="synthetic_fixture_stage_outcome",
        )
        if outcome.status is not ReconstructionStageStatus.COMPLETED:
            raise AssertionError(
                f"Native {stage.value} refused: {outcome.refusal_reasons}: {outcome.message}"
            )
        outcomes.append(outcome)
    invocation = ReconstructionStageInvocationV1(
        invocation.run, invocation.task, invocation.command, tuple(outcomes)
    )
    plan = load_reconstruction_stage_plan(invocation.command)
    source = handlers._prior_manifest(
        invocation, handlers.SOURCE_STAGE_ARTIFACT_KIND
    )
    proposal = handlers._prior_manifest(
        invocation, handlers.PROPOSAL_STAGE_ARTIFACT_KIND
    )
    carved = handlers._prior_manifest(
        invocation, handlers.CARVING_STAGE_ARTIFACT_KIND
    )
    delivery = handlers._prior_manifest(
        invocation, handlers.DELIVERY_STAGE_ARTIFACT_KIND
    )
    delivered = handlers._restore_delivered_group(delivery)
    local = tuple(handlers._read_stream_map(carved, "stream_refs").values())
    core = handlers._read_stream_map(source, "core_stream_refs")
    anchors = tuple(
        event for stream in core.values() for event in stream.events
    )
    evidence_by_symbol = handlers._read_source_evidence(source)
    final_use = handlers.reconstruction_evidence_use(
        tuple(
            item
            for symbol in sorted(evidence_by_symbol)
            for item in evidence_by_symbol[symbol]
        ),
        stage="validation",
        used_at_ns=handlers._evidence_stage_used_at(plan, invocation),
        policy=handlers._read_evidence_policy(plan),
    )
    bundles = handlers._read_cross_series_constraints(source)
    cross_use = handlers.cross_series_constraint_use(
        bundles,
        stage="validation",
        used_at_ns=handlers._evidence_stage_used_at(plan, invocation),
        policy=handlers._read_cross_series_constraint_policy(plan),
    )
    if (
        final_use.status.value == "refused"
        or cross_use.status.value == "refused"
    ):
        raise AssertionError(
            "Native point-in-time/cross-series validation refused"
        )
    support = handlers._require_planned_cross_series_support(
        invocation, plan, bundles[0]
    )
    join, maximum_age = handlers._cross_currency_join_contract(plan, support)
    final_validation = validate_cross_currency_output(
        run=invocation.run,
        window=invocation.task.window,
        streams={stream.symbol: stream for stream in delivered.streams},
        config=plan.configuration.cross_currency_config,
        stage=CrossCurrencyValidationStage.POST_BROKER,
        observed_anchors=anchors,
        conditions=(
            CrossCurrencyConditionV1.from_dict(source["cross_condition"]),
        ),
        join_policy=join,
        nearest_prior_max_age_ns=maximum_age,
    )
    write_json(
        Path(invocation.task.scratch_directory) / "fixture-outcomes",
        "final-validation",
        final_validation,
        kind="synthetic_fixture_final_validation",
    )
    if not final_validation.passed:
        raise AssertionError(
            f"Native final validation refused: {final_validation.failure_reasons}"
        )
    scientific = handlers._validated_source_scientific_conditioning(
        plan, source
    )
    runtime = {
        name: proposal[name]
        for name in (
            "proposal_engine_id",
            "proposal_engine_registry_id",
            "proposal_portfolio_id",
            "proposal_binding_id",
            "proposal_eligibility_audit_id",
            "historical_product_observation_conditioning",
            "observation_uncertainty_ensemble_id",
            "observation_scenario_id",
            "observation_scenario_kind",
            "observation_path_seed",
            "feed_epoch_transition_policy_id",
            "transition_scenario_id",
            "transition_scenario_kind",
            "transition_boundary_id",
        )
    }
    runtime.update(
        generator_config_id=proposal["generator_config"]["config_id"],
        generation_scenario=proposal["proposal_generation_scenario"],
        generation_evidence=(
            None
            if proposal["proposal_generation_evidence"] is None
            else handlers._scientific_generation_evidence(
                proposal["proposal_generation_evidence"]
            )
        ),
    )
    artifacts = plan.execution_manifest.artifacts
    benchmark = {
        "scientific_promotion_evaluated": False,
        "candidate_promotion_eligible": False,
        "limitations": [NONCLAIM],
        **{
            name: scientific[name]
            for name in (
                "scientific_ledger_id",
                "estimand_id",
                "conditioning_state_ids",
                "invalid_for_backtest",
                "invalid_for_backtest_reason",
            )
        },
        "runtime_proposal_evidence": runtime,
    }
    retention = ReconstructionRetentionPlanV1.from_dict(
        json.loads(Path(artifacts["retention_plan"].path).read_text())
    )
    staged = stage_delivery_reconstruction_publication(
        plan.execution_manifest.output_root,
        delivered,
        final_validation=final_validation,
        benchmark_artifact_ids=tuple(
            ref.sha256
            for name, ref in sorted(artifacts.items())
            if name
            in {
                "benchmark_manifest",
                "scientific_ledger",
                "motif_qualification",
                "motif_leakage_audit",
                "information_audit",
            }
        ),
        benchmark_evidence=benchmark,
        point_in_time_evidence_projection_ids=tuple(
            delivery["point_in_time_evidence_projection_ids"]
        ),
        point_in_time_evidence_decision_ids=(
            *delivery["point_in_time_evidence_decision_ids"],
            final_use.decision_id,
        ),
        cross_series_constraint_bundle_ids=tuple(
            delivery["cross_series_constraint_bundle_ids"]
        ),
        cross_series_constraint_window_ids=tuple(
            delivery["cross_series_constraint_window_ids"]
        ),
        cross_series_constraint_decision_ids=(
            *delivery["cross_series_constraint_decision_ids"],
            cross_use.decision_id,
        ),
        immutable_source_anchors=anchors,
        immutable_source_artifacts={
            f"ascii-tick:{partition.symbol}:{partition.period}:sha256:{partition.artifact.sha256}": partition.artifact
            for partition in plan.source_inventory.partitions_for_window(
                invocation.task.window
            )
        },
        symbol_group_id=invocation.task.window.synchronization_unit_id,
        retention_plan=retention,
        storage_policy=plan.configuration.storage_policy,
        staging_root=Path(invocation.task.scratch_directory) / "publication",
        storage_guard_ref=artifacts["storage_root_guard"],
        experiment_id=read_reconstruction_experiment(
            artifacts["experiment_manifest"].path
        ).experiment_id,
    )
    product = commit_delivery_reconstruction_publication(
        staged, storage_guard_ref=artifacts["storage_root_guard"]
    )
    return product, local, final_validation


@dataclass(frozen=True)
class VerifiedCampaignFixture:
    native: CampaignFixture
    index_ref: ArtifactRef
    verification: Any
    publication_ref: ArtifactRef


@pytest.fixture(scope="session")
def native_campaign(
    tmp_path_factory: pytest.TempPathFactory,
) -> CampaignFixture:
    """One real generated cohort per process, shared by the oracle controls."""
    return build_campaign_fixture(tmp_path_factory.mktemp("campaign-native"))


@pytest.fixture(scope="session")
def verified_campaign(
    native_campaign: CampaignFixture,
) -> VerifiedCampaignFixture:
    from histdatacom.campaign_verification import verify_campaign_product_index
    from histdatacom.reconstruction import ReconstructionClient

    client = ReconstructionClient()
    ref = client.construct_verified_campaign_product_index(
        native_campaign.plan_set_path,
        native_campaign.support_map_path,
        output_directory=native_campaign.root / "verified-index",
    )
    verification = verify_campaign_product_index(ref.path)
    publication = client.publish_campaign_dataset(
        ref.path,
        output_directory=native_campaign.root / "published-dataset",
        dataset_id="synthetic-integrity-fixture",
    )
    return VerifiedCampaignFixture(
        native_campaign, ref, verification, publication
    )
