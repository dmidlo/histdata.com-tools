"""Independent source coverage and actual current-reader canaries."""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

from histdatacom.schema_compatibility import (
    EvidenceKind,
    can_read,
    schema_compatibility_registry,
)
from histdatacom.schema_compatibility.inventory import (
    build_registry,
    render_documentation,
)

ROOT = Path(__file__).resolve().parents[2]


def test_generated_asset_and_documentation_match_independent_current_source() -> (
    None
):
    actual = schema_compatibility_registry()
    fresh = build_registry(ROOT)
    assert actual == fresh
    assert (
        ROOT / "docs/schema-compatibility.md"
    ).read_text() == render_documentation(fresh)
    assert len(actual.schemas) > 1000
    assert sum(bool(item.readers) for item in actual.schemas) > 600
    assert not actual.migrations  # No invented production migration evidence.
    evidence = {item.evidence_id: item for item in actual.evidence}
    assert all(
        any(
            evidence[key].kind is EvidenceKind.TEST_DEFINITION
            for key in item.evidence
        )
        for item in actual.schemas
    )


def test_all_explicit_serialization_classes_have_inventory_or_exemption() -> (
    None
):
    registry = schema_compatibility_registry()
    families = {item.family for item in registry.schemas}
    exempt = {item.qualified_name for item in registry.exemptions}
    # This sweep is deliberately independent of generator inheritance logic.
    for path in (ROOT / "src/histdatacom").rglob("*.py"):
        parts = path.relative_to(ROOT / "src").with_suffix("").parts
        module = ".".join(parts[:-1] if parts[-1] == "__init__" else parts)
        for node in ast.parse(path.read_text()).body:
            if isinstance(node, ast.ClassDef) and any(
                isinstance(child, ast.FunctionDef)
                and child.name in {"to_dict", "to_json", "payload", "as_dict"}
                for child in node.body
            ):
                assert module + "." + node.name in families | exempt


def test_current_real_reader_roundtrips_preserve_original_wire_identities() -> (
    None
):
    from histdatacom.broker_plugins import BrokerPluginMetadataV1
    from histdatacom.data_quality.training_contracts import (
        TrainingRootV1,
        TrainingVerificationLevel,
    )
    from histdatacom.forecasting.feature_contracts import FeaturePeriodV1

    metadata = BrokerPluginMetadataV1(
        "org.example.registry", "1.0.0", "Fixture"
    )
    assert BrokerPluginMetadataV1.from_json(metadata.to_json()) == metadata
    assert metadata.schema_version == "histdatacom.broker-plugin.metadata.v1"
    assert can_read(metadata.schema_version)
    root = TrainingRootV1(
        "fixture", "fixture-id", "0" * 64, next(iter(TrainingVerificationLevel))
    )
    assert TrainingRootV1.from_json(root.to_json()) == root
    assert can_read("histdatacom.training-root.v1")
    # Forecast wire versions are shared; qualified family IDs disambiguate.
    assert can_read(
        "histdatacom.forecasting.feature_contracts.FeaturePeriodV1@1.0.0"
    )
    period = FeaturePeriodV1("fixture", 0, 10)
    assert FeaturePeriodV1.from_dict(period.to_dict()) == period


def test_provider_policy_wire_families_have_native_reader_inventory() -> None:
    from histdatacom.broker_plugin_policy import BrokerPolicyContextV1
    from tests.fixtures.broker_provider_policy import (
        legacy_policy_inputs,
        policy_context,
    )
    from histdatacom.broker_plugin_policy import legacy_capture_binding

    context = policy_context(
        legacy_capture_binding(legacy_policy_inputs().session)
    )
    assert BrokerPolicyContextV1.from_json(context.to_json()) == context
    assert can_read(context.schema_version())
    registry = schema_compatibility_registry()
    assert not registry.migrations
    families = {item.family for item in registry.schemas}
    assert (
        "histdatacom.broker_plugin_policy.storage.BrokerPolicyArtifactReceiptV1"
        in families
    )
    assert (
        "histdatacom.broker_plugin_policy.contracts.BrokerProviderPolicyV1"
        in families
    )


def test_current_synthetic_json_arrow_parquet_shapes_roundtrip() -> None:
    from histdatacom.synthetic.contracts import (
        SyntheticEventStreamV1,
        SyntheticEventV1,
        synthetic_event_stream_from_arrow,
        synthetic_event_stream_from_parquet_bytes,
        synthetic_event_stream_to_arrow,
        synthetic_event_stream_to_parquet_bytes,
    )

    event = SyntheticEventV1.observed(
        symbol="eurusd",
        event_time_ns=1_000_000,
        event_sequence=0,
        bid=1.0,
        ask=1.1,
        run_id="fixture-run",
        ensemble_member_id="fixture-member",
        source_version_id="fixture-source",
        source_series_id="ascii:T:eurusd",
        source_period="197001",
        source_row_id=1,
    )
    assert SyntheticEventV1.from_json(event.to_json()) == event
    stream = SyntheticEventStreamV1.merge(
        run_id="fixture-run",
        ensemble_member_id="fixture-member",
        symbol="eurusd",
        observed_events=(event,),
        synthetic_events=(),
    )
    assert SyntheticEventStreamV1.from_json(stream.to_json()) == stream
    assert (
        synthetic_event_stream_from_arrow(
            synthetic_event_stream_to_arrow(stream)
        )
        == stream
    )
    assert (
        synthetic_event_stream_from_parquet_bytes(
            synthetic_event_stream_to_parquet_bytes(stream)
        )
        == stream
    )
    assert can_read("histdatacom.synthetic.contracts.SyntheticEventV1@1.0.0")


def test_permission_and_host_health_families_have_native_reader_inventory():
    from histdatacom.broker_plugin_health import BrokerHostHealthPolicyV1
    from histdatacom.broker_plugin_permissions import BrokerPermissionContextV1

    policy = BrokerHostHealthPolicyV1()
    context = BrokerPermissionContextV1(())
    assert BrokerHostHealthPolicyV1.from_json(policy.to_json()) == policy
    assert BrokerPermissionContextV1.from_json(context.to_json()) == context
    assert can_read("histdatacom.broker-host-health.policy.v1")
    assert can_read(context.schema_version())
    registry = schema_compatibility_registry()
    families = {item.family for item in registry.schemas}
    assert {
        "histdatacom.broker_plugin_health.contracts.BrokerHostHealthAuditV1",
        "histdatacom.broker_plugin_health.qualification.BrokerHostHealthQualificationV1",
        "histdatacom.broker_plugin_permissions.provenance.BrokerPermissionExecutionV1",
        "histdatacom.broker_plugin_permissions.contracts.BrokerPermissionManifestV1",
    } <= families
    assert not registry.migrations


def test_new_training_and_old_synthetic_physical_formats_are_registered() -> (
    None
):
    families = {item.family for item in schema_compatibility_registry().schemas}
    assert "histdatacom.synthetic.contracts.SyntheticEventV1" in families
    assert (
        "histdatacom.synthetic.persistence._synthetic_delta_arrow_schema"
        in families
    )
    assert (
        "histdatacom.synthetic.persistence._SYNTHETIC_DELTA_STORAGE_SCHEMA_VERSION"
        in families
    )
    assert (
        "histdatacom.synthetic.contracts.synthetic_event_arrow_schema"
        in families
    )
    assert "histdatacom.synthetic.contracts._write_parquet_table" in families
    assert (
        "histdatacom.synthetic.benchmark_source_projection._write_projection"
        in families
    )
    assert (
        "histdatacom.data_quality.training_temporal_contracts.TrainingTemporalBatchV1"
        in families
    )


def test_every_literal_schema_declaration_is_registered_or_specifically_exempt() -> (
    None
):
    registry = schema_compatibility_registry()
    wires = {item.wire_schema for item in registry.schemas}
    exemptions = {item.qualified_name: item for item in registry.exemptions}
    for path in (ROOT / "src/histdatacom").rglob("*.py"):
        parts = path.relative_to(ROOT / "src").with_suffix("").parts
        module = ".".join(parts[:-1] if parts[-1] == "__init__" else parts)
        for node in ast.parse(path.read_text()).body:
            if (
                not isinstance(node, (ast.Assign, ast.AnnAssign))
                or not isinstance(node.value, ast.Constant)
                or not isinstance(node.value.value, str)
            ):
                continue
            targets = (
                node.targets if isinstance(node, ast.Assign) else [node.target]
            )
            for target in targets:
                if (
                    not isinstance(target, ast.Name)
                    or "SCHEMA_VERSION" not in target.id
                ):
                    continue
                if node.value.value not in wires:
                    exemption = exemptions[module + "." + target.id]
                    assert exemption.wire_schema == node.value.value
                    assert len(exemption.reason) > 40


def test_public_query_imports_no_producer_modules() -> None:
    program = """
import sys
from histdatacom.schema_compatibility import can_read, can_migrate, migration_path, exact_semantics
key = 'histdatacom.broker-plugin.metadata.v1'
assert can_read(key)
assert can_migrate(key, key)
assert migration_path(key, key) == ()
assert exact_semantics(key, key)
for prefix in ('histdatacom.synthetic', 'histdatacom.forecasting', 'histdatacom.data_quality', 'histdatacom.broker_plugins', 'histdatacom.reconstruction_schema'):
    assert not any(name == prefix or name.startswith(prefix + '.') for name in sys.modules), prefix
assert 'histdatacom.schema_compatibility.inventory' not in sys.modules
"""
    subprocess.run([sys.executable, "-c", program], check=True, timeout=30)
