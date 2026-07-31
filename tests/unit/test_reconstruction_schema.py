"""Tests for reconstruction schema discovery and compatibility policy."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from pyarrow import ipc

from histdatacom import reconstruction_cli
from histdatacom.data_quality.training_features import (
    SYNTHETIC_PLACEHOLDER_COLUMNS,
    TRAINING_SCHEMA_VERSION,
    training_feature_definitions,
)
from histdatacom.reconstruction import (
    ReconstructionClient,
    ReconstructionPlanSpecV1,
    ReconstructionRefusedError,
)
from histdatacom.reconstruction_evidence import (
    ReconstructionEvidencePolicyV1,
)
from histdatacom.reconstruction_schema import (
    _AUDITED_MODULES,
    CURRENT_PLAN_SCHEMA_VERSION,
    HISTDATA_PROVIDER_ID,
    LEGACY_HISTDATA_CACHE_SCHEMA_VERSION,
    PORTFOLIO_PLAN_SCHEMA_VERSION,
    ReconstructionCompatibilityStatus,
    ReconstructionContractStatus,
    evaluate_reconstruction_compatibility,
    reconstruction_schema_registry,
)
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic.delivery import ReconstructionDeliveryMode
from histdatacom.synthetic.information import InformationMode

_SYMBOLS = ("eurgbp", "eurusd", "gbpusd")
_EXPECTED_ARTIFACT_SCHEMAS = {
    "feed_epoch_definition_path": "histdatacom.feed-epoch-definition.v2",
    "observation_operator_path": "histdatacom.observation-operator.v1",
    "market_context_corpus_path": "histdatacom.market-context-corpus.v1",
    "cftc_positioning_corpus_path": ("histdatacom.cftc-positioning-corpus.v1"),
    "benchmark_manifest_path": (
        "histdatacom.reverse-degradation-benchmark-manifest.v1"
    ),
    "motif_manifest_path": "histdatacom.modern-reference-motif-manifest.v1",
    "motif_index_path": "histdatacom.reference-motif-index.v1",
    "motif_qualification_path": (
        "histdatacom.modern-reference-motif-qualification.v1"
    ),
    "motif_leakage_audit_path": (
        "histdatacom.modern-reference-motif-leakage-audit.v1"
    ),
}


def _write_cache(root: Path, symbol: str, values: dict[str, list[Any]]) -> None:
    path = root / symbol / "2020" / "1" / ".data"
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(values)
    with (
        pa.OSFile(str(path), "wb") as sink,
        ipc.new_file(sink, table.schema) as writer,
    ):
        writer.write_table(table)


def _legacy_source(tmp_path: Path) -> Path:
    root = tmp_path / "ASCII" / "T"
    for ordinal, symbol in enumerate(_SYMBOLS):
        _write_cache(
            root,
            symbol,
            {
                "datetime": [1_578_268_800_000 + ordinal],
                "bid": [1.0 + ordinal / 100],
                "ask": [1.001 + ordinal / 100],
                "vol": [0],
            },
        )
    return root


def _enriched_values(
    *, schema_version: str = TRAINING_SCHEMA_VERSION
) -> dict[str, list[Any]]:
    values: dict[str, list[Any]] = {
        "datetime": [1_578_268_800_000],
        "bid": [1.0],
        "ask": [1.001],
        "vol": [0],
    }
    fallback = {
        "Utf8": "value",
        "Int64": 1,
        "Int32": 1,
        "Float64": 1.0,
        "Boolean": False,
    }
    for item in training_feature_definitions():
        value = (
            item.default if item.default is not None else fallback[item.dtype]
        )
        values[item.name] = [value]
    values.update(
        {
            "training_schema_version": [schema_version],
            "series_id": ["ascii:T:eurgbp:histdata.com"],
            "period": ["202001"],
            "row_id": [1],
            "source_row_number": [1],
            "event_seq": [0],
            "symbol": ["eurgbp"],
            "format": ["ascii"],
            "timeframe": ["T"],
            "source": ["histdata.com"],
        }
    )
    return values


def _enriched_source(
    tmp_path: Path, *, schema_version: str = TRAINING_SCHEMA_VERSION
) -> Path:
    root = tmp_path / "ASCII" / "T"
    for symbol in _SYMBOLS:
        values = _enriched_values(schema_version=schema_version)
        values["series_id"] = [f"ascii:T:{symbol}:histdata.com"]
        values["symbol"] = [symbol]
        _write_cache(root, symbol, values)
    return root


def _plan(tmp_path: Path, source_root: Path) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": CURRENT_PLAN_SCHEMA_VERSION,
        "source_root": str(source_root),
        "source_provider_id": HISTDATA_PROVIDER_ID,
        "artifact_root": str(tmp_path / "artifacts"),
        "output_root": str(tmp_path / "output"),
        "checkpoint_root": str(tmp_path / "checkpoints"),
        "scratch_root": str(tmp_path / "scratch"),
        "information_mode": "ex_post_reconstruction",
        "source_format": "ascii",
        "timeframe": "T",
        "symbols": list(_SYMBOLS),
        "delivery_mode": "modern_reference",
        "broker_delivery_artifact": None,
    }
    inputs = tmp_path / "inputs"
    inputs.mkdir(parents=True, exist_ok=True)
    for field_name, schema_version in _EXPECTED_ARTIFACT_SCHEMAS.items():
        path = inputs / f"{field_name}.json"
        path.write_text(
            json.dumps({"schema_version": schema_version}), encoding="utf-8"
        )
        payload[field_name] = str(path)
    return payload


def test_registry_is_deterministic_complete_and_matches_golden() -> None:
    registry = reconstruction_schema_registry()
    restored = json.loads(registry.to_json())
    golden_path = (
        Path(__file__).parents[1]
        / "fixtures"
        / "reconstruction_schema_registry_summary_v1.json"
    )
    golden = json.loads(golden_path.read_text(encoding="utf-8"))

    assert restored == registry.to_dict()
    assert golden == {
        "schema_version": registry.schema_version,
        "registry_id": registry.registry_id,
        "contract_count": len(registry.contracts),
        "status_counts": registry.to_dict()["status_counts"],
    }
    registered = {item.contract_schema_version for item in registry.contracts}
    for module_name in _AUDITED_MODULES:
        module = importlib.import_module(module_name)
        versions = {
            value
            for name, value in vars(module).items()
            if name.endswith("SCHEMA_VERSION") and isinstance(value, str)
        }
        assert versions <= registered


def test_registry_explains_legacy_training_event_and_future_seams() -> None:
    contracts = {
        item.contract_schema_version: item
        for item in reconstruction_schema_registry().contracts
    }
    legacy = contracts[LEGACY_HISTDATA_CACHE_SCHEMA_VERSION]
    training = contracts[TRAINING_SCHEMA_VERSION]
    event = contracts["histdatacom.synthetic-event.v1"]
    evidence = contracts["histdatacom.reconstruction-evidence-projection.v1"]
    cross_constraint = contracts[
        "histdatacom.cross-series-constraint-window.v1"
    ]
    cross_bundle = contracts["histdatacom.cross-series-constraint-bundle.v1"]
    portfolio = contracts[PORTFOLIO_PLAN_SCHEMA_VERSION]

    assert "histdatacom.time-series-fingerprint.v1" in contracts
    assert {
        "benchmark",
        "certification",
        "context",
        "cross_series",
        "dataset",
        "ensemble",
        "feed_epoch",
        "observation",
        "plan",
        "product",
        "proposal",
        "runtime",
        "training",
    } <= {item.family for item in contracts.values()}
    assert legacy.status is ReconstructionContractStatus.DEPRECATED
    assert "row ordinal" in legacy.audit_note
    training_fields = {item.name: item for item in training.fields}
    assert all(
        training_fields[name].status is ReconstructionContractStatus.DEPRECATED
        for name in SYNTHETIC_PLACEHOLDER_COLUMNS
    )
    assert all(
        "not the variable-cardinality" in training_fields[name].description
        for name in SYNTHETIC_PLACEHOLDER_COLUMNS
    )
    event_fields = {item.name: item for item in event.fields}
    assert "Immutable for observed origin" in event_fields["bid"].description
    assert event_fields["event_sequence"].identity_role != "not_identity"
    assert evidence.family == "evidence"
    assert evidence.status is ReconstructionContractStatus.OPTIONAL
    assert evidence.consumer_stages == (
        "source_enrichment",
        "evidence_qualification",
        "proposal",
        "carving",
        "validation",
        "audit",
    )
    expected_cross_stages = (
        "source_enrichment",
        "proposal",
        "carving",
        "cross_series_reconciliation",
        "validation",
    )
    assert cross_constraint.status is ReconstructionContractStatus.REQUIRED
    assert cross_constraint.family == "cross_series_constraint"
    assert cross_constraint.consumer_stages == expected_cross_stages
    assert cross_bundle.status is ReconstructionContractStatus.REQUIRED
    assert cross_bundle.consumer_stages == expected_cross_stages
    assert {
        "support_start_ns",
        "support_end_ns",
        "available_at_ns",
        "as_of_ns",
        "alignment",
        "limiting_symbols",
        "status",
    } <= {item.name for item in cross_constraint.fields}
    assert portfolio.status is ReconstructionContractStatus.RESERVED
    assert "#489" in portfolio.audit_note
    for contract in contracts.values():
        assert contract.publication_safety
        assert len(contract.fields) <= 1024
        for field in contract.fields:
            assert field.dtype
            assert field.grain
            assert field.lineage
            assert field.basis
            assert field.source_derived_status
            assert field.availability
            assert field.consumer_stages


def test_legacy_and_enriched_histdata_caches_have_explicit_compatibility(
    tmp_path: Path,
) -> None:
    legacy = evaluate_reconstruction_compatibility(
        _plan(tmp_path / "legacy", _legacy_source(tmp_path / "legacy"))
    )
    enriched = evaluate_reconstruction_compatibility(
        _plan(tmp_path / "enriched", _enriched_source(tmp_path / "enriched"))
    )

    assert legacy.executable
    assert (
        legacy.status
        is ReconstructionCompatibilityStatus.COMPATIBLE_TRANSLATION
    )
    assert {item.cache_schema_version for item in legacy.cache_schemas} == {
        LEGACY_HISTDATA_CACHE_SCHEMA_VERSION
    }
    assert enriched.executable
    assert (
        enriched.status
        is ReconstructionCompatibilityStatus.COMPATIBLE_TRANSLATION
    )
    assert {item.status for item in enriched.cache_schemas} == {
        ReconstructionCompatibilityStatus.EXACT
    }
    assert enriched.registry_id == reconstruction_schema_registry().registry_id


@pytest.mark.parametrize(
    ("updates", "code", "status"),
    (
        (
            {"source_provider_id": "oanda"},
            "alternate_provider_later_milestone",
            ReconstructionCompatibilityStatus.UNSUPPORTED,
        ),
        (
            {
                "evidence_policy": ReconstructionEvidencePolicyV1(
                    supported_provider_ids=("histdata.com", "oanda")
                ).to_dict()
            },
            "alternate_evidence_provider_later_milestone",
            ReconstructionCompatibilityStatus.UNSUPPORTED,
        ),
        (
            {"evidence_policy": {"schema_version": "invalid"}},
            "invalid_evidence_policy",
            ReconstructionCompatibilityStatus.INVALID,
        ),
        (
            {"source_format": "csv"},
            "non_ascii_source",
            ReconstructionCompatibilityStatus.INVALID,
        ),
        (
            {"timeframe": "M1"},
            "non_tick_grain",
            ReconstructionCompatibilityStatus.INVALID,
        ),
        (
            {"delivery_mode": "broker_conditioned"},
            "broker_input_later_milestone",
            ReconstructionCompatibilityStatus.RESEARCH_ONLY,
        ),
        (
            {"unexpected": True},
            "unknown_plan_field",
            ReconstructionCompatibilityStatus.INVALID,
        ),
    ),
)
def test_current_scope_fails_closed_with_precise_reasons(
    tmp_path: Path,
    updates: dict[str, Any],
    code: str,
    status: ReconstructionCompatibilityStatus,
) -> None:
    plan = _plan(tmp_path, _legacy_source(tmp_path))
    plan.update(updates)

    report = evaluate_reconstruction_compatibility(plan)

    assert not report.executable
    assert report.status is status
    assert code in {item.code for item in report.findings}


def test_portfolio_plan_is_discoverable_but_not_yet_executable() -> None:
    report = evaluate_reconstruction_compatibility(
        {"schema_version": PORTFOLIO_PLAN_SCHEMA_VERSION}
    )

    assert not report.executable
    assert report.status is ReconstructionCompatibilityStatus.RESEARCH_ONLY
    assert report.findings[0].code == "portfolio_plan_not_executable"


def test_unversioned_and_mismatched_artifacts_fail_closed(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path, _legacy_source(tmp_path))
    target = Path(plan["observation_operator_path"])
    target.write_text(
        json.dumps({"operator_id": "unversioned"}), encoding="utf-8"
    )

    unversioned = evaluate_reconstruction_compatibility(plan)
    assert not unversioned.executable
    assert "unversioned_artifact" in {
        item.code for item in unversioned.findings
    }

    target.write_text(
        json.dumps({"schema_version": "histdatacom.observation-operator.v0"}),
        encoding="utf-8",
    )
    mismatched = evaluate_reconstruction_compatibility(plan)
    assert not mismatched.executable
    assert "artifact_schema_mismatch" in {
        item.code for item in mismatched.findings
    }


def test_stale_incomplete_and_unknown_cache_schemas_fail_closed(
    tmp_path: Path,
) -> None:
    stale_root = _enriched_source(
        tmp_path / "stale",
        schema_version="histdatacom.ascii-tick-training-features.v0",
    )
    stale = evaluate_reconstruction_compatibility(
        _plan(tmp_path / "stale", stale_root)
    )
    assert not stale.executable
    assert stale.status is ReconstructionCompatibilityStatus.STALE
    assert "stale_training_cache" in {item.code for item in stale.findings}

    incomplete_root = tmp_path / "incomplete" / "ASCII" / "T"
    for symbol in _SYMBOLS:
        values = {
            key: value
            for key, value in _enriched_values().items()
            if key
            in {
                "datetime",
                "bid",
                "ask",
                "training_schema_version",
                "series_id",
                "period",
                "row_id",
                "source_row_number",
                "event_seq",
                "symbol",
                "format",
                "timeframe",
                "source",
            }
        }
        values["symbol"] = [symbol]
        _write_cache(incomplete_root, symbol, values)
    incomplete = evaluate_reconstruction_compatibility(
        _plan(tmp_path / "incomplete", incomplete_root)
    )
    assert not incomplete.executable
    assert "invalid_tick_cache_schema" in {
        item.code for item in incomplete.findings
    }
    assert any("incomplete" in item.message for item in incomplete.findings)

    unknown_root = _legacy_source(tmp_path / "unknown")
    _write_cache(
        unknown_root,
        "eurgbp",
        {
            "datetime": [1_578_268_800_000],
            "bid": [1.0],
            "ask": [1.001],
            "mystery": [1],
        },
    )
    unknown = evaluate_reconstruction_compatibility(
        _plan(tmp_path / "unknown", unknown_root)
    )
    assert not unknown.executable
    assert any("unknown fields" in item.message for item in unknown.findings)


def test_planner_consumes_compatibility_and_refuses_broker_milestone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = _legacy_source(tmp_path)
    broker_path = tmp_path / "broker.json"
    broker_path.write_text("{}", encoding="utf-8")
    plan = _plan(tmp_path, source_root)
    spec = ReconstructionPlanSpecV1.from_dict(
        {
            **plan,
            "delivery_mode": ReconstructionDeliveryMode.BROKER_CONDITIONED.value,
            "broker_delivery_artifact": ArtifactRef(
                kind="broker_delivery_artifact_v1",
                path=str(broker_path),
                size_bytes=broker_path.stat().st_size,
                sha256="0" * 64,
            ).to_dict(),
            "information_mode": InformationMode.EX_POST_RECONSTRUCTION.value,
        }
    )
    called = False

    def should_not_build(*_args: Any, **_kwargs: Any) -> Any:
        nonlocal called
        called = True
        raise AssertionError("planner ran after compatibility refusal")

    monkeypatch.setattr(
        "histdatacom.reconstruction.build_synthetic_infill_plan",
        should_not_build,
    )

    with pytest.raises(ReconstructionRefusedError, match="later_milestone"):
        ReconstructionClient().construct_plan(spec)
    assert not called


def test_installed_cli_and_api_expose_the_same_registry(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = reconstruction_cli.main(["schemas", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload == ReconstructionClient().schemas().to_dict()

    assert reconstruction_cli.main(["schemas"]) == 0
    output = capsys.readouterr().out
    assert (
        f"{len(ReconstructionClient().schemas().contracts)} contracts" in output
    )
    assert "histdata.com/ascii/T" in output
    assert "broker/OANDA: later milestone" in output


def test_installed_compatibility_preflight_matches_api(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    plan = _plan(tmp_path, _legacy_source(tmp_path))
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan), encoding="utf-8")

    code = reconstruction_cli.main(
        ["compatibility", "--plan", str(path), "--json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload == ReconstructionClient().compatibility(path).to_dict()
    assert payload["status"] == "compatible_translation"
    assert payload["executable"] is True
