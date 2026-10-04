"""Actual native products for scenario-view tests, never promoted history.

The campaign fixture below is an empirical-motif *baseline*. Its native
observation/transition axes are not applicable; this wrapper never invents
low/central/high labels for it. A separate replayable research source is needed
to exercise those axes without fabricating a campaign eligibility decision.
"""

from __future__ import annotations

import json
from fractions import Fraction
from pathlib import Path
from typing import Any

import pytest

pytest_plugins = ("tests.fixtures.campaign_verification",)


@pytest.fixture(scope="session")
def scenario_campaign(native_campaign: Any) -> dict[str, Any]:
    """Reuse one generated cohort and execute both genuine native readers."""
    from histdatacom.campaign_receipt_runner import run_campaign_verification
    from histdatacom.data_quality.training_contracts import (
        TrainingSourceV1,
        training_json,
    )
    from histdatacom.data_quality.training_lineage import (
        build_training_ownership,
    )
    from histdatacom.reconstruction import ReconstructionClient

    index = ReconstructionClient().construct_verified_campaign_product_index(
        native_campaign.plan_set_path,
        native_campaign.support_map_path,
        output_directory=native_campaign.root / "scenario-index",
    )
    store = native_campaign.root / "scenario-receipts"
    receipt = run_campaign_verification(
        index.path, output_directory=store, products_per_shard=1
    )
    catalog_ref = native_campaign.plan.artifact_graph["dataset_catalog"]
    source = TrainingSourceV1(
        catalog_json=training_json(
            json.loads(Path(catalog_ref.path).read_bytes())
        ),
        dataset_version_id=native_campaign.plan.run.source_version_ids[0],
        product_manifest_paths=tuple(
            sorted(str(path) for path in native_campaign.manifest_paths)
        ),
    )
    ownership = build_training_ownership(source)
    return {
        "native": native_campaign,
        "index": index,
        "store": store,
        "receipt": receipt,
        "source": source,
        "ownership": ownership,
    }


def exact_population_summary(
    values: tuple[Fraction, ...],
    masses: tuple[Fraction, ...],
    probabilities: tuple[Fraction, ...],
) -> tuple[Fraction, Fraction, tuple[Fraction, ...]]:
    """Independent finite-CDF oracle, with no production reducer calls.

    Quantiles are the first retained value whose cumulative mass reaches the
    requested probability. Probability zero returns the smallest supported
    value. Zero-weight values do not become quantile endpoints.
    """
    assert len(values) == len(masses) and values
    assert all(mass >= 0 for mass in masses)
    total = sum(masses, Fraction())
    assert total > 0
    mean = (
        sum((value * mass for value, mass in zip(values, masses)), Fraction())
        / total
    )
    variance = (
        sum(
            (mass * (value - mean) ** 2 for value, mass in zip(values, masses)),
            Fraction(),
        )
        / total
    )
    ordered = sorted(
        (value, mass) for value, mass in zip(values, masses) if mass > 0
    )
    quantiles = []
    for probability in probabilities:
        assert 0 <= probability <= 1
        cumulative = Fraction()
        for value, mass in ordered:
            cumulative += mass
            if cumulative >= probability * total:
                quantiles.append(value)
                break
    assert len(quantiles) == len(probabilities)
    return mean, variance, tuple(quantiles)


def build_research_scenario_fixture(root: Path) -> dict[str, Any]:
    """One prospectively fixed generated-source 3 × 3 × 2 native cohort.

    Jan–Apr are sparse month-end slices; May–Jul are dense month-start
    slices. The product bridges the actual last April / first May anchors.
    Within-regime count variation is declared before fitting, so native
    monthly empirical bounds—not supplied low/high labels—define retention.
    No search, calibration-readiness, historical-causality or promotion claim.
    """
    from datetime import datetime, timedelta, timezone

    import polars as pl

    from histdatacom.data_analytics import (
        FeedEpochFitConfigV2,
        scan_active_time_evidence,
    )
    from histdatacom.data_quality.training_contracts import (
        TrainingSourceV1,
        training_json,
    )
    from histdatacom.data_quality.training_lineage import (
        build_training_ownership,
    )
    from histdatacom.data_quality.training_scenario_contracts import (
        TrainingScenarioResearchBindingV1,
    )
    from histdatacom.data_quality.training_scenario_research import (
        RECIPE_SCHEMA,
        execute_training_scenario_research_recipe,
        research_recipe_id,
    )
    from histdatacom.datasets import (
        DatasetCatalog,
        DatasetDescriptorV1,
        DatasetOrigin,
        HistDataProviderAdapter,
        build_observed_dataset_version,
        histdata_cache_path,
    )
    from histdatacom.orchestration.reconstruction import artifact_ref_for_file
    from histdatacom.synthetic.carving import HistoricalCarvingConstraintSetV1
    from histdatacom.synthetic.cross_currency import (
        eurusd_triangle_reconciliation_config,
    )
    from histdatacom.synthetic.feed_epoch_transition import (
        FeedEpochTransitionPolicyV1,
    )
    from histdatacom.synthetic.marked_hawkes import (
        HawkesExcitationStructure,
        MarkedHawkesConfigV1,
        MarkedHawkesResourceLimitsV1,
    )
    from histdatacom.synthetic.observation import ObservationOperatorFitConfigV1
    from histdatacom.synthetic.observation_uncertainty import (
        ObservationUncertaintyPolicyV1,
    )
    from histdatacom.synthetic.persistence import (
        commit_delivery_reconstruction_publication,
        estimate_reconstruction_retention,
        stage_delivery_reconstruction_publication,
    )
    from histdatacom.synthetic.streaming import ReconstructionStoragePolicyV1

    root = root.resolve()
    raw_root = root / "ASCII" / "T"
    periods = (
        "201501",
        "201502",
        "201503",
        "201504",
        "201505",
        "201506",
        "201507",
    )
    symbols = ("EURGBP", "EURUSD", "GBPUSD")
    counts = (16, 18, 20, 22, 128, 144, 160)
    spacing_ms = (900, 1100, 800, 1200, 100, 80, 120)
    for ordinal, (period, count) in enumerate(zip(periods, counts)):
        year, month = int(period[:4]), int(period[4:])
        if ordinal < 4:
            if month in (1, 2):
                last = int(
                    datetime(
                        year,
                        month,
                        30 if month == 1 else 27,
                        20,
                        tzinfo=timezone.utc,
                    ).timestamp()
                    * 1000
                )
            else:
                next_month = datetime(year, month + 1, 1, tzinfo=timezone.utc)
                last = int(next_month.timestamp() * 1000) - 1000
            times = tuple(
                last - (count - 1 - i) * spacing_ms[ordinal]
                for i in range(count)
            )
        else:
            first = (
                int(
                    datetime(year, month, 1, tzinfo=timezone.utc).timestamp()
                    * 1000
                )
                + 1000
            )
            times = tuple(first + i * spacing_ms[ordinal] for i in range(count))
        for symbol in symbols:
            bids, asks = [], []
            for i in range(count):
                usd = 1.2 + (i % 4) * 0.00001
                gbp = 1.5 + (i % 3) * 0.00001
                bid, ask = {
                    "EURUSD": (usd - 0.0001, usd + 0.0001),
                    "GBPUSD": (gbp - 0.0001, gbp + 0.0001),
                    "EURGBP": (
                        (usd - 0.0001) / (gbp + 0.0001),
                        (usd + 0.0001) / (gbp - 0.0001),
                    ),
                }[symbol]
                bids.append(bid)
                asks.append(ask)
            path = histdata_cache_path(raw_root, symbol, period)
            path.parent.mkdir(parents=True, exist_ok=True)
            # Native calibration expands every update-state × UTC-session
            # cell. Retain actual input coverage instead of supplying nonzero
            # estimates for absent cells. Monday–Thursday avoids closed FX
            # weekend hours; four fixed blocks cover native session bins.
            day = datetime(year, month, 15, tzinfo=timezone.utc)
            while day.weekday() >= 4:
                day -= timedelta(days=1)
            rows = list(zip(times, bids, asks))
            for hour in (2, 9, 15, 23):
                block_start = int(day.replace(hour=hour).timestamp() * 1000)
                for j in range(16):
                    cycle, phase = divmod(j, 4)
                    bid_step = cycle * 2 + (
                        0 if phase == 0 else 1 if phase < 3 else 2
                    )
                    ask_step = cycle * 2 + (
                        0 if phase < 2 else 1 if phase == 2 else 2
                    )
                    rows.append(
                        (
                            block_start + j * spacing_ms[ordinal],
                            bids[0] + bid_step * 0.000001,
                            asks[0] + ask_step * 0.000001,
                        )
                    )
            rows.sort()
            pl.DataFrame(
                {
                    "datetime": [r[0] for r in rows],
                    "bid": [r[1] for r in rows],
                    "ask": [r[2] for r in rows],
                    "vol": [0] * len(rows),
                },
                schema={
                    "datetime": pl.Int64,
                    "bid": pl.Float64,
                    "ask": pl.Float64,
                    "vol": pl.Int32,
                },
            ).write_ipc(path)
    provenance = root / "generated-source.json"
    provenance.write_text(
        training_json(
            {
                "scope": "invented-source-unqualified-research",
                "empirical": False,
                "promoted": False,
                "periods": list(periods),
                "core_near_counts_per_symbol": list(counts),
                "row_counts_per_symbol": [count + 64 for count in counts],
                "calibration_session_block_hours_utc": [2, 9, 15, 23],
                "calibration_update_cycle": [
                    "bid_only",
                    "ask_only",
                    "joint",
                    "unchanged",
                ],
                "spacing_ms": list(spacing_ms),
            }
        ),
        encoding="ascii",
    )
    adapter = HistDataProviderAdapter()
    descriptor = DatasetDescriptorV1(
        "training-scenario-generated-source",
        "Scenario research source",
        "Invented source; no acquired history or scientific readiness claim.",
        (DatasetOrigin.OBSERVED,),
    )
    version = build_observed_dataset_version(
        adapter,
        raw_root,
        descriptor,
        symbols=symbols,
        periods=periods,
        qualification_evidence=(
            artifact_ref_for_file(
                provenance, kind="synthetic-research-source-provenance"
            ),
        ),
    )
    catalog = DatasetCatalog(
        (adapter.provider,), (adapter.descriptor,), (descriptor,), (version,)
    )
    observed = TrainingSourceV1(catalog.to_json(), version.dataset_version_id)
    boundary = (
        int(datetime(2015, 5, 1, tzinfo=timezone.utc).timestamp()) * 10**9
    )
    start, end = boundary - 10**9, boundary + 10**9 + 1
    storage = ReconstructionStoragePolicyV1(
        max_candidate_amplification=64.0,
        max_retained_ensemble_members=18,
        max_events_per_batch=512,
        max_memory_bytes=128 * 1024**2,
        max_scratch_bytes=128 * 1024**2,
        max_output_bytes=128 * 1024**2,
    )
    configs = {
        "feed_epoch_fit_config": FeedEpochFitConfigV2(
            feature_names=("log_interarrival_median_ms", "burst_interval_rate"),
            min_evidence_periods=6,
            min_segment_periods=2,
            penalty_multiplier=0.5,
        ),
        "observation_fit_config": ObservationOperatorFitConfigV1(),
        "uncertainty_policy": ObservationUncertaintyPolicyV1(
            minimum_path_realizations_per_scenario=2
        ),
        "transition_policy": FeedEpochTransitionPolicyV1(
            minimum_path_realizations_per_crossed_cell=2
        ),
        "marked_config": MarkedHawkesConfigV1(
            HawkesExcitationStructure.DIAGONAL,
            decay_candidates_per_second=(2.0,),
            base_seed=609,
            limits=MarkedHawkesResourceLimitsV1(
                max_fit_events=4096,
                max_fit_windows=8,
                max_iterations=64,
                max_generated_events_per_interval=256,
                max_generated_events_per_window=256,
                max_ogata_proposals=8192,
                max_candidate_amplification=64.0,
            ),
        ),
        "carving_constraints": HistoricalCarvingConstraintSetV1(
            fingerprint_constraint_id="unqualified-research-no-fingerprint",
            require_fingerprint_validation=False,
            require_complete_calendar_profile=False,
            max_input_candidate_events=256,
        ),
        "cross_currency_config": eurusd_triangle_reconciliation_config(),
        "storage_policy": storage,
    }
    native_coverage = tuple(
        scan_active_time_evidence(
            p.artifact.path,
            symbol=p.symbol,
            period=p.period,
            config=configs["feed_epoch_fit_config"],
        )
        for p in version.partitions
    )
    coverage_features = (
        "bid_only_rate",
        "ask_only_rate",
        "joint_move_rate",
        "unchanged_rate",
        *(
            f"session_activity_share_{s}"
            for s in ("asia", "london", "new_york", "off_session")
        ),
    )
    (root / "native-input-coverage.json").write_text(
        training_json(
            {
                "rows": [
                    {
                        "symbol": e.symbol,
                        "period": e.period,
                        "row_count": e.row_count,
                        "features": {
                            name: e.feature_values[name]
                            for name in coverage_features
                        },
                    }
                    for e in native_coverage
                ]
            }
        ),
        encoding="ascii",
    )
    if any(
        e.feature_values[name] <= 0
        for e in native_coverage
        for name in coverage_features
    ):
        raise AssertionError(
            "Fixed synthetic inputs lack actual native session/update coverage"
        )
    text = training_json(
        {
            "schema_version": RECIPE_SCHEMA,
            "dataset_version_id": version.dataset_version_id,
            "start_ns": start,
            "end_ns": end,
            "calibration_periods": list(periods),
            "base_seed": 609,
            "paths_per_cell": 2,
            **{name: config.to_dict() for name, config in configs.items()},
        }
    )
    binding = TrainingScenarioResearchBindingV1(text, research_recipe_id(text))
    (root / "recipe.json").write_text(text, encoding="ascii")
    cells = execute_training_scenario_research_recipe(observed, binding)
    # Preserve actual outcomes before any positive-test assertion.
    (root / "native-outcomes.json").write_text(
        training_json(
            {
                "cells": [
                    {
                        "member": c.window.ensemble_member_id,
                        "observation": c.observation_kind,
                        "transition": c.transition_kind,
                        "status": c.status,
                        "reasons": list(c.reasons),
                        "scientific": json.loads(c.scientific_json),
                    }
                    for c in cells
                ]
            }
        ),
        encoding="ascii",
    )
    counts_by_member = {
        c.window.ensemble_member_id: sum(
            len(s.events) for s in c.delivered.streams
        )
        for c in cells
        if c.delivered is not None
    }
    products = []
    if counts_by_member:
        retention = estimate_reconstruction_retention(
            run_id=cells[0].run.run_id,
            primary_member_id=min(counts_by_member),
            retained_member_event_counts=counts_by_member,
            estimated_partition_count=6 * len(counts_by_member),
            estimated_product_count=len(counts_by_member),
            storage_policy=storage,
        )
        for cell in cells:
            if cell.delivered is None:
                continue
            staged = stage_delivery_reconstruction_publication(
                root / "products",
                cell.delivered,
                final_validation=cell.validation,
                benchmark_artifact_ids=cell.evidence_ids,
                benchmark_evidence=cell.benchmark_evidence,
                immutable_source_anchors=cell.anchors,
                symbol_group_id=cell.window.synchronization_unit_id,
                retention_plan=retention,
                storage_policy=storage,
                staging_root=root / "scratch",
            )
            products.append(commit_delivery_reconstruction_publication(staged))
    source = TrainingSourceV1(
        catalog.to_json(),
        version.dataset_version_id,
        tuple(sorted(str(p.manifest_path) for p in products)),
    )
    return {
        "root": root,
        "source": source,
        "observed_source": observed,
        "ownership": build_training_ownership(source),
        "binding": binding,
        "recipe_json": text,
        "cells": cells,
        "products": tuple(products),
        "manifest_paths": tuple(Path(p.manifest_path) for p in products),
        "start_ns": start,
        "end_ns": end,
        "symbols": symbols,
        "version": version,
    }


@pytest.fixture(scope="session")
def scenario_research(
    tmp_path_factory: pytest.TempPathFactory,
) -> dict[str, Any]:
    return build_research_scenario_fixture(
        tmp_path_factory.mktemp("scenario-research")
    )
