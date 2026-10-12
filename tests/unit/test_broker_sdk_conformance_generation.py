"""Exact permission identities select actual SDK generation; no native runs."""

from dataclasses import replace
import hashlib
import json

import pytest

from histdatacom.broker_plugin_compatibility import (
    BrokerConformanceReferenceV1,
    BrokerPluginDirectoryEntryV1,
)
from histdatacom.broker_plugin_conformance import (
    BrokerConformanceCaseResultV1,
    BrokerConformanceReason,
    BrokerConformanceReportV1,
    BrokerConformanceStatus,
    broker_conformance_catalog,
    build_broker_conformance_fixture,
)
from histdatacom.broker_plugin_conformance.assessment import (
    assess_broker_conformance,
)
from histdatacom.broker_plugin_conformance import drivers
from histdatacom.broker_plugin_conformance import catalog as catalog_module
from histdatacom.broker_plugin_conformance import contracts as contracts_module
from histdatacom.broker_plugin_conformance._wire import (
    canonical_conformance_json,
)
from histdatacom.broker_plugin_permissions import (
    BrokerPermissionError,
    BrokerPermissionManifestV1,
)
from histdatacom.broker_plugin_registry import BrokerPluginInventoryV1
from tests.unit.test_broker_plugin_conformance import _plan


def test_reference_binds_exact_kit_catalog_report_and_evidence(tmp_path):
    plan = _plan(tmp_path)
    report = assess_broker_conformance(
        plan,
        tuple(
            BrokerConformanceCaseResultV1(
                case,
                BrokerConformanceStatus.NOT_RUN,
                BrokerConformanceReason.DRIVER,
                plan.subject,
            )
            for case in plan.case_ids
        ),
    )
    catalog = broker_conformance_catalog()
    reference = BrokerConformanceReferenceV1(
        catalog.version, catalog.to_json(), report.to_json(), ()
    )
    assert not report.certified
    assert reference.status == "retained_report_not_independently_requalified"
    assert (
        BrokerConformanceReferenceV1.from_json(reference.to_json()) == reference
    )
    with pytest.raises(ValueError, match="kit version"):
        replace(reference, kit_version="3.0.0")
    with pytest.raises(ValueError, match="evidence inventory"):
        replace(
            reference,
            evidence_ids=("broker-conformance-evidence:sha256:" + "e" * 64,),
        )
    with pytest.raises(ValueError):
        replace(reference, status="certified")
    other = build_broker_conformance_fixture(
        tmp_path / "other", fault="false-health"
    )
    with pytest.raises(ValueError, match="another candidate"):
        BrokerPluginDirectoryEntryV1(
            other.candidate.to_json(),
            "Generated maintainer",
            "https://example.invalid/package",
            "https://example.invalid/source",
            "unverified",
            reference,
        )


def _retained_reference(tmp_path):
    plan = _plan(tmp_path)
    report = assess_broker_conformance(
        plan,
        tuple(
            BrokerConformanceCaseResultV1(
                case,
                BrokerConformanceStatus.NOT_RUN,
                BrokerConformanceReason.DRIVER,
                plan.subject,
            )
            for case in plan.case_ids
        ),
    )
    catalog = broker_conformance_catalog()
    return BrokerConformanceReferenceV1(
        catalog.version, catalog.to_json(), report.to_json(), ()
    )


def test_retained_report_survives_current_catalog_evolution(
    tmp_path, monkeypatch
):
    from histdatacom.broker_plugin_compatibility.provenance import (
        BrokerCaptureCompatibilityDeclarationV1,
    )
    from histdatacom.broker_plugin_compatibility import (
        BrokerSDKUse,
        negotiate_broker_sdk,
    )

    reference = _retained_reference(tmp_path)
    fixture = build_broker_conformance_fixture(tmp_path / "same")
    candidate = fixture.candidate
    assert (
        reference.retained_report.plan.driver.candidate_id
        == candidate.artifact_id
    )
    entry = BrokerPluginDirectoryEntryV1(
        candidate.to_json(),
        "Generated maintainer",
        "https://example.invalid/package",
        "https://example.invalid/source",
        "unverified",
        reference,
    )
    decision = negotiate_broker_sdk(
        candidate,
        host_version="2.5.0",
        use=BrokerSDKUse.LIVE,
        evaluated_at_utc_ns=1,
        requested_sdk_version="1.0.0",
    )
    declaration = BrokerCaptureCompatibilityDeclarationV1(
        "native-header",
        "a" * 64,
        "invocation",
        "b" * 64,
        candidate.registration.provider_ids[0],
        "histdatacom.broker-plugin.event.v1",
        decision,
        reference,
    )
    catalog = broker_conformance_catalog()
    changed = replace(catalog, cases=catalog.cases[1:])
    monkeypatch.setattr(
        catalog_module, "broker_conformance_catalog", lambda: changed
    )
    monkeypatch.setattr(contracts_module, "BROKER_CONFORMANCE_VERSION", "3.0.0")
    with pytest.raises(ValueError, match="required catalog inventory"):
        BrokerConformanceReportV1.from_json(reference.report_json)
    assert (
        BrokerConformanceReferenceV1.from_json(reference.to_json()) == reference
    )
    assert BrokerPluginDirectoryEntryV1.from_json(entry.to_json()) == entry
    assert (
        BrokerCaptureCompatibilityDeclarationV1.from_json(declaration.to_json())
        == declaration
    )
    assert not hasattr(reference.retained_report, "certified")
    with pytest.raises(ValueError, match="required catalog inventory"):
        replace(reference, catalog_json=changed.to_json())


@pytest.mark.parametrize(
    "mutation", ("identity", "unknown", "assessment", "omission")
)
def test_retained_reader_rejects_tampered_report_even_with_rehashed_outer_id(
    tmp_path, mutation
):
    reference = _retained_reference(tmp_path)
    value = json.loads(reference.report_json)
    if mutation == "unknown":
        value["invented_trust"] = True
    elif mutation == "assessment":
        value["capabilities"] = []
    elif mutation == "omission":
        value["results"] = value["results"][:-1]
    if mutation == "identity":
        value["artifact_id"] = "broker-conformance-report:sha256:" + "f" * 64
    else:
        payload = {
            key: item for key, item in value.items() if key != "artifact_id"
        }
        value["artifact_id"] = (
            "broker-conformance-report:sha256:"
            + hashlib.sha256(
                canonical_conformance_json(payload).encode("ascii")
            ).hexdigest()
        )
    with pytest.raises(ValueError):
        replace(reference, report_json=canonical_conformance_json(value))


@pytest.mark.parametrize("sdk_version", ["1.0.0", "1.1.0"])
def test_worker_resolves_generation_only_from_exact_permission_pin(
    tmp_path, monkeypatch, sdk_version
):
    fixture = build_broker_conformance_fixture(tmp_path / "fixture")
    candidate = fixture.candidate
    manifest = BrokerPermissionManifestV1(
        candidate.artifact_id,
        candidate.registration.distribution_name,
        candidate.registration.distribution_version,
        sdk_version,
        candidate.registration.provider_ids,
        ("emit:quotes",),
        resource_abi="none",
    )
    driver = replace(
        fixture.driver, permission_manifest_id=manifest.artifact_id
    )
    inventory = BrokerPluginInventoryV1((candidate,))
    called = []

    def permissions(actual, *, sdk_version):
        called.append(sdk_version)
        assert actual == candidate
        if sdk_version != manifest.sdk_version:
            raise BrokerPermissionError(
                "permission_manifest_candidate_mismatch"
            )
        return manifest

    monkeypatch.setattr(drivers, "discover_broker_plugins", lambda: inventory)
    monkeypatch.setattr(
        drivers, "read_installed_broker_permissions", permissions
    )
    resolved = drivers.resolve_conformance_inventory(driver)
    assert resolved.sdk_version == sdk_version
    assert resolved.candidates == inventory.candidates
    assert called[-1] == sdk_version
    wrong = replace(
        driver,
        permission_manifest_id="broker-permission-manifest:sha256:" + "f" * 64,
    )
    with pytest.raises(ValueError, match="exact installed permission"):
        drivers.resolve_conformance_inventory(wrong)
