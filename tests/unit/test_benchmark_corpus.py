"""Contracts and failure modes for the real reverse-degradation corpus."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

import histdatacom.synthetic.benchmark_corpus as corpus_module
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic.benchmark import BenchmarkEventV1
from histdatacom.synthetic.benchmark_corpus import (
    PREDECLARED_GATE_COMMIT,
    BenchmarkWindowPartitionV1,
    ReverseDegradationBenchmarkCorpusV1,
    ReverseDegradationCorpusProfileV1,
    audit_holdout_neighbor_leakage,
    read_reverse_degradation_benchmark_corpus,
)
from histdatacom.synthetic.benchmark_gates import (
    load_default_benchmark_promotion_gate_policy,
)
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.event_clock import default_event_clock_configs
from histdatacom.synthetic.marked_hawkes import default_marked_hawkes_configs
from histdatacom.synthetic.regime_hawkes import default_regime_hawkes_configs


def _corpus() -> ReverseDegradationBenchmarkCorpusV1:
    profile = ReverseDegradationCorpusProfileV1()
    sources = []
    by_axis = {}
    for period in profile.split_periods.values():
        for symbol in profile.symbols:
            source = corpus_module.BenchmarkSourcePartitionV1(
                symbol=symbol,
                period=period,
                relative_path=f"{symbol.lower()}/{period}/.data",
                size_bytes=1024,
                row_count=1000,
                sha256=hashlib.sha256(
                    f"{symbol}:{period}".encode()
                ).hexdigest(),
            )
            sources.append(source)
            by_axis[(period, symbol)] = source
    windows = []
    split_offsets = {
        "calibration": 1_200_000_000_000_000_000,
        "validation": 1_600_000_000_000_000_000,
        "final_holdout": 1_700_000_000_000_000_000,
    }
    for split, period in profile.split_periods.items():
        for index in range(profile.synchronized_windows_per_split):
            start = split_offsets[split] + index * 86_400_000_000_000
            windows.append(
                BenchmarkWindowPartitionV1(
                    split_kind=split,
                    period=period,
                    session=("asia", "london", "new_york")[index % 3],
                    start_ns=start,
                    end_ns=start + 600_000_000_000,
                    epoch_label="technology_epoch_03",
                    source_partition_ids=tuple(
                        by_axis[(period, symbol)].partition_id
                        for symbol in profile.symbols
                    ),
                    symbol_event_counts=dict.fromkeys(profile.symbols, 64),
                    symbol_partition_sha256={
                        symbol: hashlib.sha256(
                            f"{split}:{index}:{symbol}".encode()
                        ).hexdigest()
                        for symbol in profile.symbols
                    },
                    event_state_counts={"update_joint": 192},
                    context_state="market_context:none:no_matching_event",
                    positioning_state="cftc_positioning:weekly:2020-01-01",
                    context_supported=True,
                )
            )
    policy = load_default_benchmark_promotion_gate_policy()
    dependencies = {
        name: ArtifactRef(
            kind=name,
            path=f"{name}.json",
            size_bytes=10,
            sha256=hashlib.sha256(name.encode()).hexdigest(),
        )
        for name in (
            "feed_epochs",
            "observation_campaign",
            "market_context",
            "cftc_positioning",
            "gate_policy",
        )
    }
    return ReverseDegradationBenchmarkCorpusV1(
        profile=profile,
        sources=tuple(sources),
        windows=tuple(windows),
        split_hashes=corpus_module._split_hashes(windows),
        degradation_configs=corpus_module._degradation_configs("operator-1"),
        metric_registry=corpus_module._required_metric_names(),
        dependency_artifacts=dependencies,
        feed_epoch_definition_id="feed-epochs-1",
        observation_operator_id="operator-1",
        market_context_corpus_id="context-1",
        cftc_positioning_corpus_id="positioning-1",
        gate_policy_id=policy.policy_id,
        gate_policy_commit=PREDECLARED_GATE_COMMIT,
        neighbor_leakage_count=0,
    )


def test_profile_and_corpus_round_trip_are_content_addressed() -> None:
    corpus = _corpus()

    assert (
        ReverseDegradationBenchmarkCorpusV1.from_json(corpus.to_json())
        == corpus
    )
    assert corpus.corpus_id.startswith("reverse-degradation-corpus:sha256:")
    assert len(corpus.windows) == 18
    assert set(corpus.split_hashes) == {
        "calibration",
        "validation",
        "final_holdout",
    }
    assert corpus.to_dict()["dense_and_holdout_rows_persisted"] is False

    with pytest.raises(ValueError, match="chronological"):
        ReverseDegradationCorpusProfileV1(
            split_periods={
                "calibration": "202501",
                "validation": "202401",
                "final_holdout": "202601",
            }
        )


def test_manifest_reader_rejects_tamper(tmp_path: Path) -> None:
    corpus = _corpus()
    payload = {
        "schema_version": "histdatacom.reverse-degradation-manifest.v1",
        "corpus": corpus.to_dict(),
        "artifact_contract": {
            "content_addressed": True,
            "dense_rows_embedded": False,
            "holdout_rows_embedded": False,
            "replay_required": True,
        },
    }
    encoded = canonical_contract_json(payload).encode() + b"\n"
    digest = hashlib.sha256(encoded).hexdigest()
    path = tmp_path / f"reverse-degradation-manifest-{digest}.json"
    path.write_bytes(encoded)

    assert read_reverse_degradation_benchmark_corpus(path) == corpus

    bad = tmp_path / f"reverse-degradation-manifest-{'0' * 64}.json"
    bad.write_bytes(encoded)
    with pytest.raises(ValueError, match="content hash differs"):
        read_reverse_degradation_benchmark_corpus(bad)


def test_neighbor_leakage_detects_cross_split_overlap() -> None:
    corpus = _corpus()
    calibration = next(
        item for item in corpus.windows if item.split_kind == "calibration"
    )
    holdout = next(
        item for item in corpus.windows if item.split_kind == "final_holdout"
    )
    overlapping = BenchmarkWindowPartitionV1(
        split_kind=holdout.split_kind,
        period=holdout.period,
        session=holdout.session,
        start_ns=calibration.start_ns + 1,
        end_ns=calibration.end_ns + 1,
        epoch_label=holdout.epoch_label,
        source_partition_ids=holdout.source_partition_ids,
        symbol_event_counts=holdout.symbol_event_counts,
        symbol_partition_sha256=holdout.symbol_partition_sha256,
        event_state_counts=holdout.event_state_counts,
        context_state=holdout.context_state,
        positioning_state=holdout.positioning_state,
        context_supported=True,
    )

    assert (
        audit_holdout_neighbor_leakage(
            (calibration, overlapping), guard_seconds=1800
        )
        == 1
    )


def test_dense_identity_passes_and_anchor_drop_fails() -> None:
    partition = _corpus().windows[0]
    reference = tuple(
        BenchmarkEventV1(
            source_event_id=f"{symbol}-{index}",
            symbol=symbol,
            event_time_ns=partition.start_ns + index * 1_000_000_000,
            event_sequence=index,
            bid=1.0 + index / 10_000,
            ask=1.0002 + index / 10_000,
            epoch_id=partition.epoch_label,
            session=partition.session,
            event_state="update_joint",
            sparsity="dense-reference",
            anchor_id=f"anchor-{symbol}-{index}" if index in {0, 2} else None,
        )
        for symbol in ("EURGBP", "EURUSD", "GBPUSD")
        for index in range(3)
    )
    dense = corpus_module._compare_streams(reference, reference, partition)
    negative = corpus_module._compare_streams(
        reference, corpus_module._drop_first_anchor(reference), partition
    )

    dense_accumulator = corpus_module._CandidateAccumulator()
    dense_accumulator.consume(dense)
    dense_report = corpus_module._candidate_report(
        subject_id="dense-subject",
        method_name="dense_identity",
        role="baseline",
        accumulator=dense_accumulator,
        policy=load_default_benchmark_promotion_gate_policy(),
        ensemble_member_count=1,
        evaluated_window_count=1,
        provisional=False,
    )
    negative_accumulator = corpus_module._CandidateAccumulator()
    negative_accumulator.consume(negative)
    negative_report = corpus_module._candidate_report(
        subject_id="negative-subject",
        method_name="negative_anchor_drop",
        role="negative_control",
        accumulator=negative_accumulator,
        policy=load_default_benchmark_promotion_gate_policy(),
        ensemble_member_count=1,
        evaluated_window_count=1,
        provisional=False,
    )

    assert dense_report.gate_decision.promotion_eligible
    assert dense_report.metrics["immutable_anchor_violation_count"] == 0
    assert not negative_report.gate_decision.promotion_eligible
    assert negative_report.metrics["immutable_anchor_violation_count"] == 1


@pytest.mark.parametrize(
    ("name", "parameter", "quantum"),
    (
        ("timestamp_quantization", "quantum_ns", 1_000_000_000),
        ("batching", "batch_width_ns", 2_000_000_000),
    ),
)
def test_time_degradations_preserve_protected_anchor_timestamps(
    name: str, parameter: str, quantum: int
) -> None:
    corpus = _corpus()
    partition = corpus.windows[0]
    anchor = BenchmarkEventV1(
        source_event_id="EURUSD-anchor",
        symbol="EURUSD",
        event_time_ns=partition.start_ns + 123_456_789,
        event_sequence=0,
        bid=1.1,
        ask=1.1002,
        epoch_id=partition.epoch_label,
        session=partition.session,
        event_state="update_joint",
        sparsity="dense-reference",
        anchor_id="anchor-EURUSD-first",
    )
    ordinary = BenchmarkEventV1(
        source_event_id="EURUSD-ordinary",
        symbol="EURUSD",
        event_time_ns=partition.start_ns + 1_234_567_890,
        event_sequence=1,
        bid=1.1001,
        ask=1.1003,
        epoch_id=partition.epoch_label,
        session=partition.session,
        event_state="update_joint",
        sparsity="dense-reference",
    )

    degraded = corpus_module._apply_degradation(
        (anchor, ordinary),
        config={"name": name, parameter: quantum},
        corpus=corpus,
        partition=partition,
        operator=None,  # unused by deterministic time degradations
        run_id="run-anchor-regression",
    )
    by_source = {item.source_event_id: item for item in degraded}

    assert (
        by_source[anchor.source_event_id].event_time_ns == anchor.event_time_ns
    )
    assert by_source[anchor.source_event_id].anchor_id == anchor.anchor_id
    assert (
        by_source[ordinary.source_event_id].event_time_ns
        == (ordinary.event_time_ns // quantum) * quantum
    )
    assert (
        corpus_module._compare_streams((anchor, ordinary), degraded, partition)[
            "immutable_anchor_violation_count"
        ]
        == 0
    )


def test_cli_exposes_installed_real_corpus_command() -> None:
    from histdatacom.data_analytics.cli import build_parser

    args = build_parser().parse_args(
        [
            "reverse-degradation-benchmark-corpus",
            "--source-root",
            "ticks",
            "--definition",
            "epochs.json",
            "--observation-campaign",
            "operator.json",
            "--market-context-corpus",
            "context.json",
            "--cftc-positioning-corpus",
            "positioning.json",
            "--artifact-dir",
            "artifacts",
        ]
    )

    assert args.analytics_command == "reverse-degradation-benchmark-corpus"
    assert args.gate_policy_commit == PREDECLARED_GATE_COMMIT
    assert args.windows_per_split == 6


def test_event_clock_campaign_requires_one_config_per_family() -> None:
    configs = default_event_clock_configs()

    assert corpus_module._validated_event_clock_configs(configs) == configs
    with pytest.raises(ValueError, match="exactly one config per family"):
        corpus_module._validated_event_clock_configs(configs[:-1])
    with pytest.raises(ValueError, match="exactly one config per family"):
        corpus_module._validated_event_clock_configs((*configs, configs[0]))


def test_marked_hawkes_campaign_requires_one_config_per_ablation() -> None:
    configs = default_marked_hawkes_configs()

    assert corpus_module._validated_marked_hawkes_configs(configs) == configs
    with pytest.raises(ValueError, match="exactly one config per ablation"):
        corpus_module._validated_marked_hawkes_configs(configs[:-1])
    with pytest.raises(ValueError, match="exactly one config per ablation"):
        corpus_module._validated_marked_hawkes_configs((*configs, configs[0]))


def test_regime_hawkes_campaign_requires_one_config_per_ablation() -> None:
    configs = default_regime_hawkes_configs()

    assert corpus_module._validated_regime_hawkes_configs(configs) == configs
    with pytest.raises(ValueError, match="exactly one config per ablation"):
        corpus_module._validated_regime_hawkes_configs(configs[:-1])
    with pytest.raises(ValueError, match="exactly one config per ablation"):
        corpus_module._validated_regime_hawkes_configs((*configs, configs[0]))
