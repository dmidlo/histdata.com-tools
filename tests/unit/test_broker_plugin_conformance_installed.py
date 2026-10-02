"""Fresh-interpreter installed candidate probes using only generated inputs.

This source suite explicitly uses the current checkout host via a temporary
venv .pth. Separate wheel qualification removes that development path.
"""

import json
import site
import subprocess
import sys
import venv
from dataclasses import replace
from pathlib import Path
from zipfile import ZipFile

import pytest

from histdatacom.broker_plugin_capabilities import (
    BrokerAdmittedEventV1,
    BrokerCapabilityPlanV1,
    BrokerFieldSupport,
)
from histdatacom.broker_plugin_conformance import (
    BrokerConformancePlanV1,
    BrokerConformanceProfile,
    BrokerConformanceReason,
    BrokerConformanceStatus,
    broker_conformance_catalog,
    build_broker_conformance_fixture,
    verify_broker_conformance,
)
from histdatacom.broker_plugin_conformance.assessment import (
    assess_broker_conformance,
)
from histdatacom.broker_plugin_conformance.authority import (
    PolicySource,
    generated_policy_context,
)
from histdatacom.broker_plugin_conformance.catalog import required_cases
from histdatacom.broker_plugin_conformance.evidence import (
    BrokerConformanceEvidenceV1,
)
from histdatacom.broker_plugin_conformance.storage import (
    write_artifact,
    write_runner_outcome,
)
from histdatacom.broker_plugin_conformance.verification import (
    verify_conformance_equivalence,
    verify_conformance_evidence,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthAuditV1,
    BrokerHostHealthState,
)
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleCompletion,
    BrokerLifecycleManifestV1,
    BrokerLifecycleReason,
    BrokerLifecycleRecordV1,
    BrokerLifecycleSessionV1,
    BrokerLifecycleState,
    BrokerLifecycleTransitionV1,
    replay_broker_lifecycle,
)
from histdatacom.broker_plugin_permissions import (
    BrokerPermissionContextV1,
    BrokerPermissionDecisionV1,
    BrokerPermissionExecutionV1,
    BrokerPermissionManifestV1,
    BrokerPermissionReason,
    decide_broker_permissions,
    verify_permission_execution,
)
from histdatacom.broker_plugin_policy import (
    BrokerSDKInvocationV1,
    provider_policy_scope,
    sdk_invocation_binding,
)
from histdatacom.broker_plugin_provenance import (
    BrokerProvenanceSealV1,
    BrokerProvenanceVerificationReason,
    read_lifecycle_capture_provenance,
)
from histdatacom.broker_plugin_security import BrokerTrustedSecurityReceiptV1
from histdatacom.broker_plugins import BrokerEventKind
from tests.fixtures.broker_conformance_subprocess import (
    run_conformance_worker,
)

ROOT = Path(__file__).resolve().parents[2]


def _install(root, fault, profile="trusted_contract_v1"):
    environment = root / "venv"
    venv.EnvBuilder(with_pip=False, symlinks=True).create(environment)
    python = environment / "bin" / "python"
    package_site = Path(
        subprocess.check_output(
            [
                str(python),
                "-I",
                "-c",
                "import sysconfig; print(sysconfig.get_path('purelib'))",
            ],
            text=True,
        ).strip()
    )
    (package_site / "source-conformance-test.pth").write_text(
        "\n".join((str(ROOT / "src"), *site.getsitepackages())) + "\n"
    )
    fixture = build_broker_conformance_fixture(root / "wheel", fault=fault)
    with ZipFile(fixture.wheel) as archive:
        archive.extractall(package_site)
    write_artifact(root / "driver.json", fixture.driver)
    subprocess.run(
        [
            str(python),
            "-I",
            "-m",
            "histdatacom.broker_plugin_conformance",
            "plan",
            "--driver",
            str(root / "driver.json"),
            "--profile",
            profile,
            "--capability",
            "quotes.v1",
            "--capability",
            "timestamps.receive.v1",
            "--capability",
            "sizes.quoted.v1",
            "--capability",
            "raw-hashes.v1",
            "--capability",
            "health.v1",
            "--output",
            str(root / "plan.json"),
        ],
        cwd=root,
        check=True,
        capture_output=True,
        timeout=30,
    )
    return python, root, fault


@pytest.fixture(scope="module", params=("none", "secret-leak"))
def installed_candidate(tmp_path_factory, request):
    return _install(
        tmp_path_factory.mktemp("conformance-" + request.param), request.param
    )


@pytest.mark.parametrize(
    "case,detail",
    (
        ("lifecycle.exception", "none"),
        ("lifecycle.malformed", "event_type"),
        ("quotes.invalid-spread", "invalid_spread"),
        ("quotes.sequence", "sequence"),
        ("timestamps.monotonic", "monotonic_regression"),
        ("sizes.permission-denial", "none"),
        ("raw.permission-denial", "none"),
        ("secrets.redaction", "none"),
    ),
)
def test_actual_installed_negative_gate_requires_exact_host_stage(
    installed_candidate, case, detail
):
    python, root, fault = installed_candidate
    directory = root / case
    directory.mkdir()
    completed = subprocess.run(
        [
            str(python),
            "-I",
            "-B",
            "-m",
            "histdatacom.broker_plugin_conformance._worker",
            str(root / "plan.json"),
            case,
            str(directory),
        ],
        cwd=root,
        capture_output=True,
        check=False,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stderr.decode()
    plan = BrokerConformancePlanV1.from_json((root / "plan.json").read_text())
    evidence = BrokerConformanceEvidenceV1.from_json(
        (directory / "evidence.json").read_text()
    )
    result = verify_conformance_evidence(plan, evidence, directory)
    with pytest.raises(ValueError, match="metadata-only"):
        replace(evidence, native_family="none")
    if fault == "none" or case in {"lifecycle.exception", "secrets.redaction"}:
        assert result.status is BrokerConformanceStatus.PASS
        assert evidence.event_failure == detail
    else:
        assert result.status is not BrokerConformanceStatus.PASS
        assert json.loads(evidence.probe_json)["stage"] == "open"
    assert result.execution_backend == "trusted_probe"
    assert (
        "generated-conformance-secret-not-for-retention"
        not in evidence.to_json()
    )


@pytest.mark.parametrize(
    "fault",
    (
        "none",
        "fabricated-optional",
        "missing-capability",
        "overprivilege",
        "secret-leak",
        "reconnect-duplication",
        "malformed-event",
    ),
)
def test_real_installed_broken_candidate_fails_finite_gate(tmp_path, fault):
    python, root, _ = _install(tmp_path, fault)
    directory = root / "finite"
    directory.mkdir()
    subprocess.run(
        [
            str(python),
            "-I",
            "-m",
            "histdatacom.broker_plugin_conformance._worker",
            str(root / "plan.json"),
            "lifecycle.finite",
            str(directory),
        ],
        cwd=root,
        check=True,
        capture_output=True,
        timeout=90,
    )
    plan = BrokerConformancePlanV1.from_json((root / "plan.json").read_text())
    evidence = BrokerConformanceEvidenceV1.from_json(
        (directory / "evidence.json").read_text()
    )
    result = verify_conformance_evidence(plan, evidence, directory)
    assert (result.status is BrokerConformanceStatus.PASS) is (fault == "none")
    if fault == "missing-capability":
        refused = BrokerCapabilityPlanV1.from_json(
            evidence.capability_plan_json
        )
        assert not refused.admitted
        assert refused.missing_required == ("quotes.v1",)
        assert not evidence.invocation_json
        assert evidence.refusal_family == "capability"
        assert evidence.refusal_reason == "unsupported_capability"
    elif fault == "overprivilege":
        assert evidence.refusal_family == "permission"
        assert evidence.refusal_reason == "required_permission_denied"
        decision = BrokerPermissionDecisionV1.from_json(
            evidence.refusal_decision_json
        )
        manifest = BrokerPermissionManifestV1.from_json(
            evidence.permission_manifest_json
        )
        context = BrokerPermissionContextV1.from_json(
            evidence.permission_context_json
        )
        assert (
            decide_broker_permissions(
                manifest,
                decision.binding,
                decision.grant_id,
                context,
                decision.at_ns,
            )
            == decision
        )
        assert decision.reason is BrokerPermissionReason.REQUIRED_DENIED
        assert not decision.admitted
        assert decision.missing_required_atoms == ("subprocess:requested",)
    if fault == "none":
        profile = BrokerConformanceProfile.ISOLATED
        cases = {
            item.case_id
            for item in broker_conformance_catalog().cases
            if item.capability == "common" and profile.value in item.profiles
        }
        for capability in plan.capabilities:
            cases.update(required_cases(profile, capability))
        isolated_plan = replace(
            plan, profile=profile, case_ids=tuple(sorted(cases))
        )
        downgraded = replace(evidence, plan_id=isolated_plan.artifact_id)
        with pytest.raises(ValueError, match="backend"):
            verify_conformance_evidence(isolated_plan, downgraded, directory)


@pytest.mark.parametrize(
    "fault",
    (
        "none",
        "nondeterministic-replay",
        "reused-session-nonce",
        "reused-session-nonce-changing-time",
    ),
)
@pytest.mark.parametrize("case", ("replay.determinism", "session.freshness"))
def test_actual_two_interpreter_repeat_catches_nondeterministic_fixture(
    tmp_path, fault, case
):
    python, root, _ = _install(tmp_path, fault)
    directory = root / "paired"
    directory.mkdir()
    repeat = directory / "repeat"
    repeat.mkdir()
    for destination in (directory, repeat):
        subprocess.run(
            [
                str(python),
                "-I",
                "-m",
                "histdatacom.broker_plugin_conformance._worker",
                str(root / "plan.json"),
                case,
                str(destination),
            ],
            cwd=root,
            check=True,
            capture_output=True,
            timeout=90,
        )
    plan = BrokerConformancePlanV1.from_json((root / "plan.json").read_text())
    first = BrokerConformanceEvidenceV1.from_json(
        (directory / "evidence.json").read_text()
    )
    second = BrokerConformanceEvidenceV1.from_json(
        (repeat / "evidence.json").read_text()
    )
    assert first.artifact_id != second.artifact_id
    assert first.stopped_at_ns <= second.started_at_ns
    for evidence, destination in ((first, directory), (second, repeat)):
        assert evidence.refusal_family == evidence.refusal_reason == "none"
        prerequisite = verify_conformance_evidence(plan, evidence, destination)
        assert prerequisite.status is BrokerConformanceStatus.PASS
        assert prerequisite.reason is BrokerConformanceReason.VERIFIED
    result = verify_conformance_equivalence(plan, first, second, directory)
    passes = fault == "none" or (
        case == "session.freshness" and fault == "nondeterministic-replay"
    )
    assert result.status is (
        BrokerConformanceStatus.PASS if passes else BrokerConformanceStatus.FAIL
    )
    assert result.reason is (
        BrokerConformanceReason.VERIFIED
        if passes
        else BrokerConformanceReason.CONTRACT
    )
    assert result.execution_backend == "paired_runtime"
    if fault == "none":
        failed = root / "interrupted-report"
        failed.mkdir()
        write_artifact(failed / "plan.json", plan)
        outcomes = []
        for planned_case in plan.case_ids:
            case_dir = failed / planned_case
            case_dir.mkdir()
            if planned_case == case:
                # A real completed first interpreter, but no second evidence.
                write_artifact(case_dir / "evidence.json", first)
                outcomes.append(
                    write_runner_outcome(
                        plan,
                        planned_case,
                        BrokerConformanceStatus.ERROR,
                        BrokerConformanceReason.TIMEOUT,
                        case_dir,
                    )
                )
            else:
                outcomes.append(
                    write_runner_outcome(
                        plan,
                        planned_case,
                        BrokerConformanceStatus.NOT_RUN,
                        BrokerConformanceReason.CANCELLED,
                        case_dir,
                    )
                )
        report = assess_broker_conformance(plan, tuple(outcomes))
        write_artifact(failed / "report.json", report)
        assert not report.certified
        assert verify_broker_conformance(failed).to_json() == report.to_json()


@pytest.mark.parametrize(
    "fault",
    ("none", "reused-session-nonce", "reused-session-nonce-changing-time"),
)
def test_public_complete_trusted_profile_and_repeat_replay_pin_freshness(
    tmp_path, fault
):
    """Actual public orchestration; the source-host .pth is disclosed above."""
    python, root, _ = _install(tmp_path, fault)
    command = [
        str(python),
        "-I",
        "-B",
        "-m",
        "histdatacom.broker_plugin_conformance",
    ]
    plan_path = root / "complete-quotes-plan.json"
    subprocess.run(
        command
        + [
            "plan",
            "--driver",
            str(root / "driver.json"),
            "--profile",
            BrokerConformanceProfile.TRUSTED.value,
            "--capability",
            "quotes.v1",
            "--output",
            str(plan_path),
        ],
        cwd=root,
        check=True,
        capture_output=True,
        timeout=30,
    )
    plan = BrokerConformancePlanV1.from_json(plan_path.read_text())
    assert plan.case_ids == required_cases(plan.profile, "quotes.v1")
    assert "session.freshness" in plan.case_ids
    output = root / "complete-quotes-run"
    completed = subprocess.run(
        command
        + [
            "run",
            "--plan",
            str(plan_path),
            "--output",
            str(output),
            "--authorize-generated-execution",
        ],
        cwd=root,
        check=False,
        capture_output=True,
        timeout=900,
    )
    expected_exit = 0 if fault == "none" else 1
    assert completed.returncode == expected_exit, completed.stderr.decode()
    original = (output / "report.json").read_bytes()
    report = verify_broker_conformance(output)
    assert report.plan == plan
    assert report.certified is (fault == "none")
    assert tuple(result.case_id for result in report.results) == plan.case_ids
    nonpassing = {
        result.case_id
        for result in report.results
        if result.status is not BrokerConformanceStatus.PASS
    }
    assert nonpassing == (
        set()
        if fault == "none"
        else {"replay.determinism", "session.freshness"}
    )
    for result in report.results:
        if result.case_id in nonpassing:
            assert result.status is BrokerConformanceStatus.FAIL
            assert result.reason is BrokerConformanceReason.CONTRACT
            assert result.execution_backend == "paired_runtime"
            assert len(result.evidence_ids) == 2
    # Independent readers retain the old physical nonce; neither read opens
    # another session or consumes a mutable global uniqueness cache.
    for _ in range(2):
        replayed = subprocess.run(
            command + ["verify", str(output)],
            cwd=root,
            check=False,
            capture_output=True,
            timeout=120,
        )
        assert replayed.returncode == expected_exit, replayed.stderr.decode()
        assert json.loads(replayed.stdout) == json.loads(original)
        assert (output / "report.json").read_bytes() == original


@pytest.mark.parametrize("fault", ("none", "false-health"))
def test_installed_false_health_claim_fails_actual_native_host_audit(
    tmp_path, fault
):
    python, root, _ = _install(tmp_path, fault, "isolated_contract_v1")
    directory = root / "native-health"
    directory.mkdir()
    subprocess.run(
        [
            str(python),
            "-I",
            "-m",
            "histdatacom.broker_plugin_conformance._worker",
            str(root / "plan.json"),
            "health.false-gap",
            str(directory),
        ],
        cwd=root,
        check=True,
        capture_output=True,
        timeout=180,
    )
    plan = BrokerConformancePlanV1.from_json((root / "plan.json").read_text())
    evidence = BrokerConformanceEvidenceV1.from_json(
        (directory / "evidence.json").read_text()
    )
    result = verify_conformance_evidence(plan, evidence, directory)
    if sys.platform != "darwin":
        assert result.status is BrokerConformanceStatus.UNSUPPORTED
    else:
        assert evidence.refusal_family == evidence.refusal_reason == "none"
        assert result.status is (
            BrokerConformanceStatus.PASS
            if fault == "none"
            else BrokerConformanceStatus.FAIL
        )
        assert result.reason is (
            BrokerConformanceReason.VERIFIED
            if fault == "none"
            else BrokerConformanceReason.CONTRACT
        )
        assert evidence.health_json and evidence.provenance_json
        audit = BrokerHostHealthAuditV1.from_json(evidence.health_json)
        assert any(bucket.gap_count for bucket in audit.buckets)
        assert bool(
            any(bucket.healthy_claim_discrepancies for bucket in audit.buckets)
        ) == (fault == "false-health")
    assert result.execution_backend == "isolated"


def _assert_honest_source_absence(admitted):
    quotes = tuple(
        item for item in admitted if item.event.kind is BrokerEventKind.QUOTE
    )
    assert len(quotes) == 3
    assert all(item.event.source_time is None for item in quotes)
    assert all(item.event.receive_time is not None for item in quotes)
    assert all(
        next(
            field for field in item.fields if field.field == "source_time"
        ).support
        is BrokerFieldSupport.NOT_REPORTED
        for item in quotes
    )


def test_honest_health_is_receive_only_with_explicit_source_unknown(tmp_path):
    python, root, _ = _install(tmp_path, "none", "isolated_contract_v1")
    directory = root / "honest"
    directory.mkdir()
    subprocess.run(
        [
            str(python),
            "-I",
            "-m",
            "histdatacom.broker_plugin_conformance._worker",
            str(root / "plan.json"),
            "health.honest",
            str(directory),
        ],
        cwd=root,
        check=True,
        capture_output=True,
        timeout=180,
    )
    plan = BrokerConformancePlanV1.from_json((root / "plan.json").read_text())
    evidence = BrokerConformanceEvidenceV1.from_json(
        (directory / "evidence.json").read_text()
    )
    result = verify_conformance_evidence(plan, evidence, directory)
    if sys.platform != "darwin":
        assert result.status is BrokerConformanceStatus.UNSUPPORTED
        return
    assert result.status is BrokerConformanceStatus.PASS
    audit = BrokerHostHealthAuditV1.from_json(evidence.health_json)
    assert audit.state is BrokerHostHealthState.QUALIFIED
    # The HEALTH advisory and all three quotes have no source-clock evidence.
    assert sum(bucket.source_clock_missing for bucket in audit.buckets) == 4
    assert all(bucket.upstream_loss_unknown for bucket in audit.buckets)
    assert not any(
        bucket.clock_jump_count or bucket.healthy_claim_discrepancies
        for bucket in audit.buckets
    )
    # Inspect only the same generated native records independently verified
    # above; absent source clocks remain explicit in the SDK field receipts.
    admitted = []
    for partition in sorted((directory / "native").glob("partition-*.jsonl")):
        for line in partition.read_text().splitlines():
            record = BrokerLifecycleRecordV1.from_json(line)
            if record.kind == "event":
                admitted.append(
                    BrokerAdmittedEventV1.from_json(record.payload_json)
                )
    _assert_honest_source_absence(admitted)


def test_honest_health_absent_source_cannot_pass_source_timestamp_gate(
    tmp_path,
):
    python, root, _ = _install(tmp_path, "none")
    original = BrokerConformancePlanV1.from_json(
        (root / "plan.json").read_text()
    )
    driver = replace(
        original.driver,
        scenarios=tuple(
            (
                replace(item, configuration_json='{"mode":"honest-health"}')
                if item.scenario_id == "finite"
                else item
            )
            for item in original.driver.scenarios
        ),
    )
    capability = "timestamps.broker-event.v1"
    plan = replace(
        original,
        driver=driver,
        capabilities=(capability,),
        case_ids=required_cases(BrokerConformanceProfile.TRUSTED, capability),
    )
    write_artifact(root / "source-plan.json", plan)
    directory = root / "source"
    directory.mkdir()
    subprocess.run(
        [
            str(python),
            "-I",
            "-m",
            "histdatacom.broker_plugin_conformance._worker",
            str(root / "source-plan.json"),
            "timestamps.source",
            str(directory),
        ],
        cwd=root,
        check=True,
        capture_output=True,
        timeout=90,
    )
    evidence = BrokerConformanceEvidenceV1.from_json(
        (directory / "evidence.json").read_text()
    )
    result = verify_conformance_evidence(plan, evidence, directory)
    assert result.status is BrokerConformanceStatus.FAIL
    assert result.reason is BrokerConformanceReason.CONTRACT
    receipt = BrokerTrustedSecurityReceiptV1.from_json(evidence.native_json)
    _assert_honest_source_absence(
        tuple(
            BrokerAdmittedEventV1.from_json(text)
            for text in receipt.events_json
        )
    )


@pytest.mark.parametrize(
    "fault,case,stage,detail",
    (
        ("malformed-event", "lifecycle.malformed", "events", "event_type"),
        ("secret-leak", "secrets.redaction", "open", "none"),
    ),
)
def test_shipped_broken_variant_hits_exact_public_probe(
    tmp_path, fault, case, stage, detail
):
    python, root, _ = _install(tmp_path, fault)
    directory = root / "intended-gate"
    directory.mkdir()
    subprocess.run(
        [
            str(python),
            "-I",
            "-m",
            "histdatacom.broker_plugin_conformance._worker",
            str(root / "plan.json"),
            case,
            str(directory),
        ],
        cwd=root,
        check=True,
        capture_output=True,
        timeout=90,
    )
    plan = BrokerConformancePlanV1.from_json((root / "plan.json").read_text())
    evidence = BrokerConformanceEvidenceV1.from_json(
        (directory / "evidence.json").read_text()
    )
    result = verify_conformance_evidence(plan, evidence, directory)
    assert result.status is BrokerConformanceStatus.PASS
    assert result.reason is BrokerConformanceReason.VERIFIED
    assert result.execution_backend == "trusted_probe"
    assert json.loads(evidence.probe_json)["stage"] == stage
    assert evidence.event_failure == detail
    assert evidence.refusal_family == "capability"
    assert evidence.refusal_reason == (
        "capability_violation" if detail == "event_type" else "plugin_failure"
    )
    assert (
        "generated-conformance-secret-not-for-retention"
        not in evidence.to_json()
    )


def test_shipped_reconnect_duplicate_has_two_native_sessions_and_duplicates(
    tmp_path,
):
    python, root, _ = _install(
        tmp_path, "reconnect-duplication", "isolated_contract_v1"
    )
    directory = root / "reconnect"
    directory.mkdir()
    subprocess.run(
        [
            str(python),
            "-I",
            "-m",
            "histdatacom.broker_plugin_conformance._worker",
            str(root / "plan.json"),
            "lifecycle.reconnect",
            str(directory),
        ],
        cwd=root,
        check=True,
        capture_output=True,
        timeout=300,
    )
    plan = BrokerConformancePlanV1.from_json((root / "plan.json").read_text())
    evidence = BrokerConformanceEvidenceV1.from_json(
        (directory / "evidence.json").read_text()
    )
    result = verify_conformance_evidence(plan, evidence, directory)
    if sys.platform != "darwin":
        assert result.status is BrokerConformanceStatus.UNSUPPORTED
        return
    assert evidence.refusal_family == evidence.refusal_reason == "none"
    assert result.status is BrokerConformanceStatus.FAIL
    assert result.reason is BrokerConformanceReason.CONTRACT
    audit = BrokerHostHealthAuditV1.from_json(evidence.health_json)
    assert any(bucket.exact_quote_duplicates for bucket in audit.buckets)
    sessions = set()
    for partition in sorted((directory / "native").glob("partition-*.*")):
        for line in partition.read_text().splitlines():
            record = BrokerLifecycleRecordV1.from_json(line)
            if record.kind == "session":
                sessions.add(record.epoch)
    assert sessions == {0, 1}


@pytest.mark.parametrize(
    "fault",
    ("none", "reused-session-nonce", "reused-session-nonce-changing-time"),
)
def test_installed_isolated_reconnect_requires_fresh_session_nonces(
    tmp_path, fault
):
    """Actual two-worker openings; replay is not another session opening."""
    python, root, _ = _install(tmp_path, fault, "isolated_contract_v1")
    directory = root / "reconnect-freshness"
    directory.mkdir()
    completed = run_conformance_worker(
        [
            str(python),
            "-I",
            "-B",
            "-m",
            "histdatacom.broker_plugin_conformance._worker",
            str(root / "plan.json"),
            "lifecycle.reconnect",
            str(directory),
        ],
        cwd=root,
        timeout=300,
    )
    assert completed.returncode == 0, completed.stderr.decode()
    plan = BrokerConformancePlanV1.from_json((root / "plan.json").read_text())
    evidence_bytes = (directory / "evidence.json").read_bytes()
    evidence = BrokerConformanceEvidenceV1.from_json(evidence_bytes.decode())
    result = verify_conformance_evidence(plan, evidence, directory)
    assert result.execution_backend == "isolated"
    if sys.platform != "darwin":
        assert result.status is BrokerConformanceStatus.UNSUPPORTED
        assert result.reason is BrokerConformanceReason.PLATFORM
        assert evidence.refusal_reason == "security_backend_unsupported"
        return

    assert evidence.refusal_family == evidence.refusal_reason == "none"
    assert evidence.native_family == "isolated"
    request = BrokerSDKInvocationV1.from_json(evidence.invocation_json)
    native = BrokerLifecycleManifestV1.from_json(evidence.native_json)
    seal = BrokerProvenanceSealV1.from_json(evidence.provenance_json)
    permission = BrokerPermissionExecutionV1.from_json(
        evidence.permission_execution_json
    )
    verify_permission_execution(permission, request, native)
    assert permission.decision.admitted
    # Fresh generated-only material-use authority; the retained permission
    # proof above is historical evidence, never installed as current authority.
    source = PolicySource(
        generated_policy_context(sdk_invocation_binding(request))
    )
    with provider_policy_scope(source):
        records = tuple(
            replay_broker_lifecycle(
                directory / "native", provider_request=request
            )
        )
        verified = read_lifecycle_capture_provenance(
            directory / "native",
            native,
            provider_request=request,
            expected_root=seal,
        )
    # Reconnect deliberately exhausts its single retry. Its valid pinned
    # terminal chain is PARTIAL, not a complete capture or an anchored-True
    # certificate. Full native replay must succeed even for a rejected nonce.
    assert verified.reason is BrokerProvenanceVerificationReason.PARTIAL
    assert not verified.anchored and not verified.complete
    assert verified.seal == seal
    assert verified.header.artifact_id == seal.header_id
    assert verified.root_sha256 == seal.root_sha256
    assert verified.entry_count == seal.entry_count
    assert verified.checkpoint_count == seal.checkpoint_count
    assert native.completion is BrokerLifecycleCompletion.PARTIAL
    assert native.state is BrokerLifecycleState.FAILED
    assert native.worker_reaped
    assert native.error_diagnostics == native.discarded_known == 0
    assert native.header.policy.retry_delays_ms == (0,)
    assert native.appended_events == native.received_events == 8

    session_records = tuple(item for item in records if item.kind == "session")
    assert tuple(item.epoch for item in session_records) == (0, 1)
    assert (
        session_records[0].capture_sequence
        < session_records[1].capture_sequence
    )
    sessions = tuple(
        BrokerLifecycleSessionV1.from_json(item.payload_json).session
        for item in session_records
    )
    assert sessions[0].metadata_id == sessions[1].metadata_id
    assert (
        sessions[0].receive_clock_id
        == sessions[1].receive_clock_id
        == "generated-clock"
    )
    if fault == "none":
        assert sessions[0].instance_nonce != sessions[1].instance_nonce
        assert (
            sessions[0].opened_at_utc_ns == sessions[1].opened_at_utc_ns == 100
        )
    else:
        assert (
            sessions[0].instance_nonce == sessions[1].instance_nonce == "a" * 32
        )
        if fault == "reused-session-nonce":
            assert sessions[0] == sessions[1]
            assert sessions[0].opened_at_utc_ns == 100
        else:
            assert sessions[0].opened_at_utc_ns != sessions[1].opened_at_utc_ns
            assert sessions[0].artifact_id != sessions[1].artifact_id

    events = tuple(
        (item, BrokerAdmittedEventV1.from_json(item.payload_json).event)
        for item in records
        if item.kind == "event"
    )
    assert tuple((item.epoch, event.kind) for item, event in events) == tuple(
        (epoch, kind)
        for epoch in (0, 1)
        for kind in (BrokerEventKind.QUOTE,) * 3
        + (BrokerEventKind.DISCONNECTED,)
    )
    for item, event in events:
        assert event.session_id == sessions[item.epoch].artifact_id
    transitions = tuple(
        BrokerLifecycleTransitionV1.from_json(item.payload_json)
        for item in records
        if item.kind == "transition"
    )
    assert (
        sum(item.reason is BrokerLifecycleReason.RETRY for item in transitions)
        == 1
    )
    stopping, terminal = transitions[-2:]
    assert stopping.current is BrokerLifecycleState.STOPPING
    assert stopping.reason is BrokerLifecycleReason.RETRY_EXHAUSTED
    assert terminal.previous is BrokerLifecycleState.STOPPING
    assert terminal.current is BrokerLifecycleState.FAILED
    assert stopping.epoch == terminal.epoch == 1
    assert terminal.reason in {
        BrokerLifecycleReason.RETRY_EXHAUSTED,
        BrokerLifecycleReason.FORCED,
    }
    if terminal.reason is BrokerLifecycleReason.FORCED:
        assert native.forced_terminations > 0
    assert {item.reason for item in transitions} <= {
        BrokerLifecycleReason.CONFIGURED,
        BrokerLifecycleReason.STARTING,
        BrokerLifecycleReason.ACTIVE,
        BrokerLifecycleReason.RECONNECT,
        BrokerLifecycleReason.RETRY,
        BrokerLifecycleReason.RETRY_EXHAUSTED,
        BrokerLifecycleReason.FORCED,
    }
    audit = BrokerHostHealthAuditV1.from_json(evidence.health_json)
    assert audit.artifact_id == seal.terminal.health_audit_id
    assert not any(bucket.exact_quote_duplicates for bucket in audit.buckets)
    assert result.status is (
        BrokerConformanceStatus.PASS
        if fault == "none"
        else BrokerConformanceStatus.FAIL
    )
    assert result.reason is (
        BrokerConformanceReason.VERIFIED
        if fault == "none"
        else BrokerConformanceReason.CONTRACT
    )
    assert result.evidence_ids == (evidence.artifact_id,)
    assert (directory / "evidence.json").read_bytes() == evidence_bytes


@pytest.mark.parametrize("fault", ("none", "fabricated-optional"))
def test_actual_optional_field_refusal_is_observed_without_payload_retention(
    tmp_path, fault
):
    python, root, _ = _install(tmp_path, fault)
    directory = root / "observed-field"
    directory.mkdir()
    completed = subprocess.run(
        [
            str(python),
            "-I",
            "-B",
            str(
                ROOT
                / "tests"
                / "fixtures"
                / "broker_conformance_optional_probe.py"
            ),
            str(root / "plan.json"),
            str(directory),
        ],
        cwd=root,
        check=True,
        capture_output=True,
        timeout=90,
    )
    plan = BrokerConformancePlanV1.from_json((root / "plan.json").read_text())
    evidence = BrokerConformanceEvidenceV1.from_json(
        (directory / "evidence.json").read_text()
    )
    result = verify_conformance_evidence(plan, evidence, directory)
    observed = json.loads(completed.stdout)[
        "instrumented_noncertifying_observations"
    ]
    if fault == "none":
        assert result.status is BrokerConformanceStatus.PASS
        assert observed == []
    else:
        assert result.status is BrokerConformanceStatus.FAIL
        native_plan = BrokerCapabilityPlanV1.from_json(
            evidence.capability_plan_json
        )
        assert observed == [
            {
                "plan_id": native_plan.artifact_id,
                "gate": "undeclared_quoted_size",
                "field": "bid_size",
                "capability": "sizes.quoted.v1",
                "reason": "capability_violation",
            }
        ]
