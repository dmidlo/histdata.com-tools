"""Independent source coverage and actual current-reader canaries."""

from __future__ import annotations

import ast
import hashlib
import subprocess
import sys
from pathlib import Path

from histdatacom.schema_compatibility import (
    EvidenceKind,
    SupportStatus,
    can_read,
    schema_compatibility_registry,
)
from histdatacom.schema_compatibility.inventory import (
    build_registry,
    render_documentation,
)

ROOT = Path(__file__).resolve().parents[2]


def test_cross_feed_artifacts_have_native_versioned_codec_inventory() -> None:
    registry = schema_compatibility_registry()
    families = {item.family: item for item in registry.schemas}
    expected = {
        "capture_contracts.NativeCaptureV1": "native-capture",
        "clock_contracts.ClockFitPolicyV1": "clock-fit-policy",
        "clock_contracts.ClockFitRequestV1": "clock-fit-request",
        "clock_contracts.ClockModelV1": "clock-model",
        "clock_contracts.ClockSegmentV1": "clock-segment",
        "matching_contracts.MatchingPolicyV1": "matching-policy",
        "matching_contracts.MatchingReportV1": "matching-report",
        "matching_contracts.MatchingTruthV1": "matching-truth",
        "matching_contracts.MatchingEvaluationV1": "matching-evaluation",
        "matching_contracts.MatchingSensitivityV1": "matching-sensitivity",
    }
    for name, suffix in expected.items():
        schema = families["histdatacom.cross_feed." + name]
        assert schema.wire_schema == f"histdatacom.cross-feed-{suffix}.v1"
        assert schema.version == "1.0.0"
        assert schema.status is SupportStatus.SUPPORTED
        assert schema.readers and schema.writers
        assert can_read(schema.wire_schema)
    assert "histdatacom.cross_feed._wire.Artifact" not in families
    assert "histdatacom.cross_feed._wire.Artifact" in {
        item.qualified_name for item in registry.exemptions
    }
    # Embedded plain records do not invent separate versioned envelopes.
    assert (
        families["histdatacom.cross_feed._wire.RationalV1"].version
        == "unversioned"
    )


def test_campaign_and_vendor_m1_wires_have_real_versioned_codecs() -> None:
    """Shared serializers remain inventoried under concrete public wires."""
    registry = schema_compatibility_registry()
    families = {item.family: item for item in registry.schemas}
    implementations = {
        item.implementation_id: item for item in registry.implementations
    }
    expected = {
        "histdatacom.campaign_index_contracts.CampaignStructuralVerificationV1": "histdatacom.campaign-structural-verification.v1",
        "histdatacom.campaign_index_contracts.CampaignDeepVerificationV1": "histdatacom.campaign-deep-verification.v1",
        "histdatacom.campaign_index_contracts.CampaignProductVerificationV1": "histdatacom.campaign-product-verification.v1",
        "histdatacom.data_quality.vendor_m1_contracts.VendorM1PolicyV1": "histdatacom.vendor-m1-policy.v1",
        "histdatacom.data_quality.vendor_m1_contracts.VendorM1ReferenceV1": "histdatacom.vendor-m1-reference.v1",
        "histdatacom.data_quality.vendor_m1_contracts.VendorM1DifferenceV1": "histdatacom.vendor-m1-difference.v1",
        "histdatacom.data_quality.vendor_m1_contracts.VendorM1MinuteV1": "histdatacom.vendor-m1-minute.v1",
        "histdatacom.data_quality.vendor_m1_contracts.VendorM1ReportV1": "histdatacom.vendor-m1-report.v1",
    }
    for family, wire in expected.items():
        schema = families[family]
        assert schema.wire_schema == wire
        assert schema.version == "1.0.0"
        assert schema.status is SupportStatus.SUPPORTED
        assert can_read(wire)
        assert {
            implementations[key].qualified_name for key in schema.readers
        } == {
            family + ".from_dict",
            family + ".from_json",
        }
        assert {
            implementations[key].qualified_name for key in schema.writers
        } == {
            family + ".to_dict",
            family + ".to_json",
        }
    exemptions = {item.qualified_name for item in registry.exemptions}
    assert "histdatacom.campaign_index_contracts._Record" in exemptions
    assert "histdatacom.data_quality.vendor_m1_contracts._Wire" in exemptions
    assert not registry.migrations


def test_account_inherited_wires_are_exact_reader_writer_inventory() -> None:
    registry = schema_compatibility_registry()
    families = {item.family: item for item in registry.schemas}
    implementations = {
        item.implementation_id: item for item in registry.implementations
    }
    evidence = {item.evidence_id: item for item in registry.evidence}
    prefix = "histdatacom.synthetic.traders."
    codec = "src/histdatacom/synthetic/traders/account_codec.py"
    contracts = "src/histdatacom/synthetic/traders/account_contracts.py"
    expected = {
        "PolicySourceV1": "histdatacom.trader-account-policy-source.v1",
        "AccountPolicyV1": "histdatacom.trader-account-policy.v1",
        "AccountSpecV1": "histdatacom.trader-account-spec.v1",
        "AccountFillV1": "histdatacom.trader-account-fill.v1",
        "AccountLotV1": "histdatacom.trader-account-lot.v1",
        "AccountAllocationV1": "histdatacom.trader-account-allocation.v1",
        "CurrencyExposureV1": "histdatacom.trader-currency-exposure.v1",
        "AccountStateV1": "histdatacom.trader-account-state.v1",
        "AccountFillReceiptV1": "histdatacom.trader-account-fill-receipt.v1",
        "AccountLedgerV1": "histdatacom.trader-account-ledger.v1",
    }
    for name, wire in expected.items():
        family = prefix + "account_contracts." + name
        schema = families[family]
        assert schema.wire_schema == wire
        assert schema.version == "1.0.0"
        assert schema.status is SupportStatus.SUPPORTED
        assert can_read(wire)
        assert {
            implementations[key].qualified_name for key in schema.readers
        } == {family + ".from_dict", family + ".from_json"}
        assert {
            implementations[key].qualified_name for key in schema.writers
        } == {family + ".to_dict", family + ".to_json"}
        assert {
            implementations[key].source_sha256
            for key in (*schema.readers, *schema.writers)
        } == {hashlib.sha256((ROOT / codec).read_bytes()).hexdigest()}
        assert {
            evidence[key].locator
            for key in schema.evidence
            if evidence[key].kind is EvidenceKind.SOURCE_DEFINITION
        } == {codec, contracts}
    exemptions = {item.qualified_name: item for item in registry.exemptions}
    for name in ("AccountRecord", "_record_wire"):
        key = prefix + "account_codec." + name
        assert key in exemptions
        assert key not in families
    # Implemented codecs alone do not establish migration or accounting proof.
    assert not registry.migrations
    assert not registry.compositions


def test_account_exact_amount_keeps_its_unversioned_native_shape() -> None:
    from histdatacom.synthetic.traders.account_codec import ExactAmountV1

    family = "histdatacom.synthetic.traders.account_codec.ExactAmountV1"
    registry = schema_compatibility_registry()
    schema = next(item for item in registry.schemas if item.family == family)
    assert schema.version == "unversioned"
    assert schema.wire_schema == "unversioned:" + family
    assert schema.status is SupportStatus.SUPPORTED
    assert len(schema.readers) == len(schema.writers) == 2
    amount = ExactAmountV1(3, 5)
    assert amount.to_dict() == {"numerator": 3, "denominator": 5}
    assert ExactAmountV1.from_dict(amount.to_dict()) == amount
    assert ExactAmountV1.from_json(amount.to_json()) == amount
    assert can_read(schema.schema_id)


def test_account_shared_schema_field_does_not_reclassify_other_constants(
    tmp_path: Path,
) -> None:
    package = tmp_path / "src/histdatacom"
    account = package / "synthetic/traders"
    account.mkdir(parents=True)
    (package / "__init__.py").write_text("__version__ = '3.0.0'\n")
    (account / "account_codec.py").write_text(
        "class AccountRecord:\n"
        "    KIND: str\n"
        "    def to_dict(self): return {}\n"
        "    def to_json(self): return '{}'\n"
        "    @classmethod\n"
        "    def from_dict(cls, value): return cls()\n"
        "    @classmethod\n"
        "    def from_json(cls, value): return cls()\n"
    )
    (account / "account_contracts.py").write_text(
        "from .account_codec import AccountRecord as Base\n"
        "class FixtureV1(Base):\n"
        "    KIND = 'fixture'\n"
        "    SCHEMA = 'histdatacom.account-inventory-fixture.v1'\n"
        "class Unrelated:\n"
        "    SCHEMA = 'not-an-emitted-wire.v7'\n"
        "    def to_dict(self): return {}\n"
    )
    registry = build_registry(tmp_path)
    schemas = {item.family.rsplit(".", 1)[1]: item for item in registry.schemas}
    assert set(schemas) == {"FixtureV1", "Unrelated"}
    assert schemas["FixtureV1"].wire_schema == (
        "histdatacom.account-inventory-fixture.v1"
    )
    assert len(schemas["FixtureV1"].readers) == 2
    assert len(schemas["FixtureV1"].writers) == 2
    assert schemas["Unrelated"].version == "unversioned"
    assert schemas["Unrelated"].wire_schema.startswith("unversioned:")


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


def test_capture_provenance_and_conformance_are_versioned_native_families():
    from histdatacom.broker_plugin_conformance import broker_conformance_catalog
    from histdatacom.broker_plugin_provenance import BrokerProvenancePolicyV1

    policy = BrokerProvenancePolicyV1()
    catalog = broker_conformance_catalog()
    assert type(policy).from_json(policy.to_json()) == policy
    assert type(catalog).from_json(catalog.to_json()) == catalog
    assert can_read(policy.schema_version())
    assert can_read("histdatacom.broker-conformance.catalog.v1")
    families = {item.family for item in schema_compatibility_registry().schemas}
    assert {
        "histdatacom.broker_plugin_provenance.contracts.BrokerProvenanceHeaderV1",
        "histdatacom.broker_plugin_provenance.contracts.BrokerProvenanceSealV1",
        "histdatacom.broker_plugin_conformance.contracts.BrokerConformanceReportV1",
        "histdatacom.broker_capture.fingerprint_v2.BrokerDeliveryFingerprintV2",
    } <= families
    assert not schema_compatibility_registry().migrations


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
