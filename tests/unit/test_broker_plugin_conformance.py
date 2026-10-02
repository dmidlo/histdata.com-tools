"""Generated conformance contracts; no provider, account or historical input."""

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from zipfile import ZipFile

import pytest

from histdatacom.broker_plugin_conformance import (
    BrokerConformanceCaseResultV1,
    BrokerConformancePlanV1,
    BrokerConformanceProfile,
    BrokerConformanceReason,
    BrokerConformanceReportV1,
    BrokerConformanceStatus,
    BrokerConformanceSubject,
    broker_conformance_catalog,
    build_broker_conformance_fixture,
    run_broker_conformance,
    verify_broker_conformance,
)
from histdatacom.broker_plugin_conformance.assessment import (
    assess_broker_conformance,
)
from histdatacom.broker_plugin_conformance.catalog import (
    required_cases,
    verify_plan,
)
from histdatacom.broker_plugin_conformance.fixtures import FIXTURE_FAULTS


def _plan(
    tmp_path: Path,
    capability: str = "quotes.v1",
    subject=BrokerConformanceSubject.CANDIDATE,
):
    fixture = build_broker_conformance_fixture(tmp_path / "wheel")
    catalog = broker_conformance_catalog()
    profile = BrokerConformanceProfile.TRUSTED
    cases = tuple(
        sorted(
            {
                case.case_id
                for case in catalog.cases
                if case.capability == "common"
                and profile.value in case.profiles
            }
            | set(required_cases(profile, capability))
        )
    )
    return BrokerConformancePlanV1(
        fixture.driver,
        catalog.artifact_id,
        profile,
        (capability,),
        cases,
        subject,
    )


def test_catalog_has_unique_full_closed_profile_inventory():
    catalog = broker_conformance_catalog()
    assert catalog.version == "2.0.0"
    assert len(catalog.cases) == 44
    assert len({case.case_id for case in catalog.cases}) == 44
    for profile in BrokerConformanceProfile:
        required = required_cases(profile, "quotes.v1")
        assert {
            "discovery.metadata",
            "permissions.required-denial",
            "permissions.revocation",
            "policy.invoke-denial",
            "policy.publication-denial",
            "quotes.symbols",
            "quotes.decimals",
            "secrets.redaction",
            "session.freshness",
        } <= set(required)
    assert (
        required_cases(BrokerConformanceProfile.TRUSTED, "history.backfill.v1")
        == ()
    )


def test_honest_health_does_not_change_deterministic_timestamp_scenarios():
    cases = {case.case_id: case for case in broker_conformance_catalog().cases}
    assert cases["health.honest"].scenario_id == "honest-health"
    for case in (
        "replay.determinism",
        "execution.equivalence",
        "timestamps.source",
        "timestamps.receive",
        "lifecycle.finite",
        "session.freshness",
    ):
        assert cases[case].scenario_id == "finite"
    assert cases["timestamps.monotonic"].scenario_id == "bad-clock"
    assert cases["health.false-delay"].scenario_id == "healthy-delay"


def test_plan_roundtrip_and_independent_content_digest(tmp_path):
    plan = _plan(tmp_path)
    assert BrokerConformancePlanV1.from_json(plan.to_json()) == plan
    raw = json.loads(plan.to_json())
    claimed = raw.pop("artifact_id")
    data = json.dumps(
        raw, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    assert (
        claimed
        == "broker-conformance-plan:sha256:" + hashlib.sha256(data).hexdigest()
    )
    verify_plan(plan)
    with pytest.raises(ValueError, match="required catalog"):
        verify_plan(replace(plan, case_ids=plan.case_ids[:-1]))


@pytest.mark.parametrize(
    "status,reason",
    [
        (BrokerConformanceStatus.NOT_RUN, BrokerConformanceReason.DRIVER),
        (BrokerConformanceStatus.ERROR, BrokerConformanceReason.TIMEOUT),
        (BrokerConformanceStatus.UNSUPPORTED, BrokerConformanceReason.PLATFORM),
        (BrokerConformanceStatus.FAIL, BrokerConformanceReason.CONTRACT),
    ],
)
def test_unexecuted_or_failed_required_gate_never_certifies(
    tmp_path, status, reason
):
    plan = _plan(tmp_path)
    results = tuple(
        BrokerConformanceCaseResultV1(case, status, reason, plan.subject)
        for case in plan.case_ids
    )
    report = assess_broker_conformance(plan, results)
    assert not report.certified
    assert report.capabilities[0].status is status
    assert BrokerConformanceReportV1.from_json(report.to_json()) == report


def test_unknown_capability_retains_common_denominator(tmp_path):
    plan = _plan(tmp_path, "history.backfill.v1")
    assert "lifecycle.finite" in plan.case_ids
    results = tuple(
        BrokerConformanceCaseResultV1(
            case,
            BrokerConformanceStatus.PASS,
            BrokerConformanceReason.VERIFIED,
            plan.subject,
            ("native:sha256:" + "0" * 64,),
        )
        for case in plan.case_ids
    )
    report = assess_broker_conformance(plan, results)
    assert report.capabilities[0].status is BrokerConformanceStatus.UNSUPPORTED
    assert not report.certified


def test_reference_subject_never_certifies_installed_candidate(tmp_path):
    plan = _plan(tmp_path, subject=BrokerConformanceSubject.REFERENCE)
    results = tuple(
        BrokerConformanceCaseResultV1(
            case,
            BrokerConformanceStatus.PASS,
            BrokerConformanceReason.VERIFIED,
            plan.subject,
            ("native:sha256:" + "0" * 64,),
        )
        for case in plan.case_ids
    )
    assert not assess_broker_conformance(plan, results).certified


def test_forged_pass_and_driver_executable_content_refused(tmp_path):
    plan = _plan(tmp_path)
    with pytest.raises(ValueError):
        BrokerConformanceCaseResultV1(
            plan.case_ids[0],
            BrokerConformanceStatus.PASS,
            BrokerConformanceReason.VERIFIED,
            plan.subject,
        )
    raw = json.loads(plan.to_json())
    raw["driver"]["expected_verdict"] = "pass"
    with pytest.raises(ValueError):
        BrokerConformancePlanV1.from_json(json.dumps(raw))
    with pytest.raises(ValueError):
        BrokerConformancePlanV1.from_json(
            plan.to_json().replace(
                '"case_timeout_ms":',
                '"case_timeout_ms":1,"case_timeout_ms":',
                1,
            )
        )


@pytest.mark.parametrize("fault", FIXTURE_FAULTS)
def test_independent_fixture_wheels_bind_exact_installed_code(tmp_path, fault):
    first = build_broker_conformance_fixture(tmp_path / "first", fault=fault)
    second = build_broker_conformance_fixture(tmp_path / "second", fault=fault)
    assert first.wheel.read_bytes() == second.wheel.read_bytes()
    with ZipFile(first.wheel) as wheel:
        source = wheel.read("conformance_fixture/plugin.py")
        assert (
            hashlib.sha256(source).hexdigest()
            == first.candidate.implementation_sha256
        )
        assert b"tests." not in source
        assert b"broker_plugin_conformance" not in source
        assert ("FAULT = " + repr(fault)).encode("ascii") in source
        assert "conformance_fixture/driver.json" in wheel.namelist()


def test_cancelled_run_preserves_every_gate_and_deterministic_report(tmp_path):
    plan = _plan(tmp_path)
    directory = tmp_path / "run"
    report = run_broker_conformance(plan, directory, cancelled=lambda: True)
    assert len(report.results) == len(plan.case_ids)
    assert all(
        result.reason is BrokerConformanceReason.CANCELLED
        for result in report.results
    )
    assert verify_broker_conformance(directory).to_json() == report.to_json()
    with pytest.raises(FileExistsError):
        run_broker_conformance(plan, directory, cancelled=lambda: True)
    (directory / "unexpected.json").write_text("{}")
    with pytest.raises(ValueError, match="inventory"):
        verify_broker_conformance(directory)
