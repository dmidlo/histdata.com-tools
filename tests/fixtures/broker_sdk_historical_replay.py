"""Read-only installed-host qualification of an actual pre-#622 SDK capture.

Copy this file and broker_provider_policy.py to a neutral directory. Supply a
retained generated operator bundle and independently retained manifest, seal,
and environment hashes. The original plugin must already be uninstalled. This
does not activate anything, rewrite old bytes, or qualify scientific fitness.
Fresh fixture declarations are used for current rights, never stored grants.
"""

from __future__ import annotations

import argparse
import hashlib
from dataclasses import replace
from importlib import metadata, util
import json
from pathlib import Path
from runpy import run_path
import stat
import sys
import time

import histdatacom
from histdatacom.broker_plugin_compatibility import (
    read_lifecycle_capture_compatibility,
)
from histdatacom.broker_plugin_lifecycle import BrokerLifecycleManifestV1
from histdatacom.broker_plugin_policy import (
    BrokerPolicyError,
    BrokerSDKInvocationV1,
    provider_policy_scope,
    sdk_invocation_binding,
)
from histdatacom.broker_plugin_policy.contracts import (
    BrokerPolicyContextV1,
    BrokerPolicyDecisionV1,
    BrokerPolicyOperation,
    BrokerPolicyRevocationV1,
)
from histdatacom.broker_plugin_provenance import (
    BrokerProvenanceSealV1,
    read_lifecycle_capture_provenance,
)

if __package__:
    from .broker_provider_policy import MutablePolicySource, policy_context
else:
    _POLICY_HELPERS = run_path(
        str(Path(__file__).resolve().with_name("broker_provider_policy.py"))
    )
    MutablePolicySource = _POLICY_HELPERS["MutablePolicySource"]
    policy_context = _POLICY_HELPERS["policy_context"]


def read_exact(path: Path, expected: str | None = None) -> bytes:
    info = path.lstat()
    assert stat.S_ISREG(info.st_mode) and info.st_size <= 8 * 1024 * 1024
    data = path.read_bytes()
    assert len(data) == info.st_size
    if expected is not None:
        assert hashlib.sha256(data).hexdigest() == expected, path.name
    return data


def assert_revoked_denial(error: BaseException) -> None:
    """Only an exact current MATERIAL_USE revocation is successful denial."""
    assert type(error) is BrokerPolicyError
    assert error.reason == "operation_not_allowed"
    assert str(error) == "provider policy refused: operation_not_allowed"
    decision = error.decision
    assert type(decision) is BrokerPolicyDecisionV1
    assert not decision.allowed
    assert decision.request.operation is BrokerPolicyOperation.MATERIAL_USE
    assert any("policy_revoked" in cell.reasons for cell in decision.cells)


def revoke_selected_policy(
    context: BrokerPolicyContextV1, *, at_ns: int
) -> tuple[BrokerPolicyContextV1, BrokerPolicyRevocationV1]:
    """Append one revocation; preserve the exact selected policy and ledger."""
    assert type(context) is BrokerPolicyContextV1
    assert len(context.selected_policy_ids) == 1
    selected = next(
        policy
        for policy in context.policies
        if policy.artifact_id == context.selected_policy_ids[0]
    )
    revoked = BrokerPolicyRevocationV1(
        selected.artifact_id, at_ns, at_ns, selected.evidence_ids, "withdrawn"
    )
    assert revoked.artifact_id not in {
        value.artifact_id for value in context.revocations
    }
    updated = replace(
        context,
        revocations=tuple(
            sorted(
                context.revocations + (revoked,),
                key=lambda value: value.artifact_id,
            )
        ),
    )
    return updated, revoked


def assert_plugin_absent(distribution: str, module: str) -> None:
    """Do not activate the selected entry module while proving its absence."""
    assert module and "." not in module
    try:
        metadata.distribution(distribution)
    except metadata.PackageNotFoundError:
        pass
    else:
        raise AssertionError("historical plugin must be uninstalled")
    assert (
        util.find_spec(module) is None
    ), "selected plugin module is importable"
    assert not any(
        name == module or name.startswith(module + ".") for name in sys.modules
    ), "selected plugin module remains loaded"


def retained_input_files(root: Path) -> dict[str, tuple[str, int, int]]:
    """Stream every regular retained file's exact bytes, size and mode."""
    assert stat.S_ISDIR(root.lstat().st_mode)
    result = {}
    for path in sorted(root.rglob("*")):
        info = path.lstat()
        if stat.S_ISDIR(info.st_mode):
            continue
        assert stat.S_ISREG(info.st_mode)
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as stream:
            while block := stream.read(1024 * 1024):
                digest.update(block)
                size += len(block)
        assert size == info.st_size
        result[path.relative_to(root).as_posix()] = (
            digest.hexdigest(),
            size,
            stat.S_IMODE(info.st_mode),
        )
    assert result, "historical bundle has no retained inputs"
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--seal-sha256", required=True)
    parser.add_argument("--environment-sha256", required=True)
    args = parser.parse_args()
    assert histdatacom.__file__ and "site-packages" in histdatacom.__file__
    bundle = args.bundle.resolve(strict=True)
    retained_before = retained_input_files(bundle)
    native = bundle / "native"
    paths = (
        native / "manifest.json",
        bundle / "anchors" / "expected-seal.json",
        bundle / "native-provenance" / "environment.json",
        bundle / "anchors" / "request.json",
    )
    before = tuple(
        read_exact(path, digest)
        for path, digest in zip(
            paths,
            (
                args.manifest_sha256,
                args.seal_sha256,
                args.environment_sha256,
                None,
            ),
        )
    )
    manifest = BrokerLifecycleManifestV1.from_json(before[0].decode())
    seal = BrokerProvenanceSealV1.from_json(before[1].decode())
    environment = json.loads(before[2])
    request = BrokerSDKInvocationV1.from_json(before[3].decode())
    assert environment["schema_version"] == (
        "histdatacom.broker-provenance-environment.v1"
    )
    assert manifest.header.inventory.sdk_version == "1.0.0"
    assert request.plan.candidate.registration.plugin_id in (
        "org.histdatacom.reference-broker",
        "org.example.starter",
    ), "this qualification grants synthetic fixture rights only"
    registration = request.plan.candidate.registration
    plugin = registration.distribution_name
    module = registration.entry_point.split(":", 1)[0].split(".", 1)[0]
    assert_plugin_absent(plugin, module)
    binding = sdk_invocation_binding(request)
    source = MutablePolicySource(policy_context(binding))
    with provider_policy_scope(source):
        replay = read_lifecycle_capture_provenance(
            native, manifest, provider_request=request, expected_root=seal
        )
        assert replay.complete and replay.anchored and replay.seal == seal
        compatibility = read_lifecycle_capture_compatibility(
            native, manifest, provider_request=request, expected_root=seal
        )
        assert compatibility is None
        current = source.current
        reads_before_revocation = source.reads
        source.current, revoked = revoke_selected_policy(
            current, at_ns=time.time_ns()
        )
        assert source.current.selected_policy_ids == current.selected_policy_ids
        assert source.current.policies == current.policies
        assert source.current.execution == current.execution
        try:
            read_lifecycle_capture_provenance(
                native, manifest, provider_request=request, expected_root=seal
            )
        except BrokerPolicyError as error:
            assert_revoked_denial(error)
            assert source.reads > reads_before_revocation
            denied = True
        else:
            raise AssertionError("historical grants bypassed current denial")
    assert tuple(read_exact(path) for path in paths) == before
    assert retained_input_files(bundle) == retained_before
    assert_plugin_absent(plugin, module)
    print(
        json.dumps(
            {
                "installed_host": histdatacom.__file__,
                "sdk_version": "1.0.0",
                "environment_schema": environment["schema_version"],
                "manifest_id": manifest.artifact_id,
                "seal_id": seal.artifact_id,
                "plugin_uninstalled": plugin,
                "selected_module_absent": module,
                "anchored_replay": replay.anchored,
                "retained_compatibility": "historical_unavailable",
                "current_rights_denied": denied,
                "pinned_inputs_unchanged": True,
                "same_selection_current_ledger_reread": True,
                "revocation_id": revoked.artifact_id,
                "retained_input_file_count": len(retained_before),
                "retained_input_bytes_and_modes_unchanged": True,
                "nonclaim": "generated_historical_replay_not_scientific_fitness",
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
