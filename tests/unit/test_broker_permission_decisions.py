"""Independent subset enumeration and fresh synthetic operator-ledger tests."""

from __future__ import annotations

import itertools
from dataclasses import replace

import pytest

from histdatacom.broker_plugin_permissions import (
    BrokerPermissionAuthorityV1,
    BrokerPermissionBindingV1,
    BrokerPermissionContextV1,
    BrokerPermissionDecisionV1,
    BrokerPermissionError,
    BrokerPermissionGrantV1,
    BrokerPermissionManifestV1,
    BrokerPermissionReason,
    BrokerPermissionRevocationV1,
    decide_broker_permissions,
    decisions,
)


def _fixture(
    required: tuple[str, ...] = ("emit:quotes",),
    optional: tuple[str, ...] = ("emit:health",),
):
    manifest = BrokerPermissionManifestV1(
        "broker-plugin-candidate:sha256:" + "a" * 64,
        "fixture",
        "1.0.0",
        "1.0.0",
        ("fixture",),
        required,
        optional,
        resource_abi="none",
    )
    binding = BrokerPermissionBindingV1(
        manifest.candidate_id,
        manifest.artifact_id,
        "1.0.0",
        "fixture",
        "broker-provider-policy-provider-configuration:sha256:" + "b" * 64,
    )
    grant = BrokerPermissionGrantV1(
        binding, manifest.declared_atoms, "operator", 10, 30, "c" * 32
    )
    return manifest, binding, grant, BrokerPermissionContextV1((grant,))


class _Source:
    def __init__(self, context: BrokerPermissionContextV1):
        self.context = context
        self.reads = 0

    def read_context(self) -> BrokerPermissionContextV1:
        self.reads += 1
        return self.context


def _authority():
    manifest, binding, grant, context = _fixture()
    source = _Source(context)
    now = [15]
    authority = BrokerPermissionAuthorityV1(
        manifest, binding, grant.artifact_id, source, lambda: now[0]
    )
    return authority, source, now


def test_every_required_optional_granted_subset_matches_independent_boolean_reference() -> (
    None
):
    universe = ("emit:health", "emit:quotes", "emit:sizes")
    # Each atom is absent/required/optional; every possible host grant follows.
    # Reference uses simple booleans, not production subset or decision helpers.
    for roles in itertools.product(range(3), repeat=3):
        required = tuple(
            atom for atom, role in zip(universe, roles) if role == 1
        )
        optional = tuple(
            atom for atom, role in zip(universe, roles) if role == 2
        )
        manifest, binding, original, _ = _fixture(required, optional)
        for bits in itertools.product((False, True), repeat=3):
            granted = tuple(atom for atom, bit in zip(universe, bits) if bit)
            grant = replace(original, granted_atoms=granted)
            context = BrokerPermissionContextV1((grant,))
            result = decide_broker_permissions(
                manifest, binding, grant.artifact_id, context, 15
            )
            expected = all(role != 1 or bit for role, bit in zip(roles, bits))
            assert result.admitted is expected
            assert result.effective_atoms == (
                tuple(
                    atom
                    for atom, role, bit in zip(universe, roles, bits)
                    if role and bit
                )
                if expected
                else ()
            )
            assert result.denied_optional_atoms == tuple(
                atom
                for atom, role, bit in zip(universe, roles, bits)
                if role == 2 and not bit
            )
            assert (
                BrokerPermissionDecisionV1.from_json(result.to_json()) == result
            )


@pytest.mark.parametrize(
    "now,reason",
    [
        (9, BrokerPermissionReason.NOT_YET_VALID),
        (10, BrokerPermissionReason.ADMITTED),
        (29, BrokerPermissionReason.ADMITTED),
        (30, BrokerPermissionReason.EXPIRED),
    ],
)
def test_grant_interval_is_exactly_half_open(
    now: int, reason: BrokerPermissionReason
) -> None:
    manifest, binding, grant, context = _fixture()
    assert (
        decide_broker_permissions(
            manifest, binding, grant.artifact_id, context, now
        ).reason
        is reason
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"candidate_id": "broker-plugin-candidate:sha256:" + "d" * 64},
        {"manifest_id": "broker-permission-manifest:sha256:" + "d" * 64},
        {"sdk_version": "2.0.0"},
        {"provider_id": "other"},
        {
            "configuration_id": "broker-provider-policy-provider-configuration:sha256:"
            + "d" * 64
        },
    ],
)
def test_grant_for_another_exact_binding_cannot_be_reused(
    changes: dict[str, object],
) -> None:
    manifest, binding, grant, context = _fixture()
    result = decide_broker_permissions(
        manifest, replace(binding, **changes), grant.artifact_id, context, 15
    )
    assert result.reason is BrokerPermissionReason.BINDING
    assert not result.admitted and not result.effective_atoms


def test_undeclared_extra_grant_never_becomes_effective_or_a_runtime_resource() -> (
    None
):
    manifest, binding, grant, _ = _fixture()
    grant = replace(
        grant,
        granted_atoms=tuple(sorted((*grant.granted_atoms, "raw_payload:emit"))),
    )
    authority = BrokerPermissionAuthorityV1(
        manifest,
        binding,
        grant.artifact_id,
        _Source(BrokerPermissionContextV1((grant,))),
        lambda: 15,
    )
    assert authority.require("emit:quotes").admitted
    with pytest.raises(
        BrokerPermissionError, match="resource_permission_denied"
    ):
        authority.require("raw_payload:emit")
    with pytest.raises(
        BrokerPermissionError, match="unknown_resource_permission"
    ):
        authority.require("scientific_store:write")


def test_optional_denial_degrades_explicitly_without_blocking_required_surface() -> (
    None
):
    manifest, binding, grant, _ = _fixture()
    grant = replace(grant, granted_atoms=("emit:quotes",))
    authority = BrokerPermissionAuthorityV1(
        manifest,
        binding,
        grant.artifact_id,
        _Source(BrokerPermissionContextV1((grant,))),
        lambda: 15,
    )
    result = authority.require_admission()
    assert result.effective_atoms == ("emit:quotes",)
    assert result.denied_optional_atoms == ("emit:health",)
    with pytest.raises(
        BrokerPermissionError, match="resource_permission_denied"
    ):
        authority.require("emit:health")


def test_missing_grant_and_required_denial_remain_explicit() -> None:
    manifest, binding, grant, _ = _fixture()
    missing = decide_broker_permissions(
        manifest, binding, grant.artifact_id, BrokerPermissionContextV1(()), 15
    )
    assert missing.reason is BrokerPermissionReason.MISSING_GRANT
    denied_grant = replace(grant, granted_atoms=())
    denied = decide_broker_permissions(
        manifest,
        binding,
        denied_grant.artifact_id,
        BrokerPermissionContextV1((denied_grant,)),
        15,
    )
    assert denied.reason is BrokerPermissionReason.REQUIRED_DENIED
    assert denied.missing_required_atoms == ("emit:quotes",)


def test_each_effect_rereads_and_revocation_has_exact_effective_boundary() -> (
    None
):
    authority, source, now = _authority()
    assert authority.require("emit:quotes").admitted
    grant = source.context.grants[0]
    revoked = BrokerPermissionRevocationV1(
        grant.artifact_id, 20, "operator-revoked"
    )
    source.context = replace(source.context, revocations=(revoked,), revision=1)
    now[0] = 19
    assert authority.require("emit:quotes").admitted
    now[0] = 20
    with pytest.raises(BrokerPermissionError, match="permission_grant_revoked"):
        authority.require("emit:quotes")
    assert source.reads == 3
    # Even a refused check retains its observed append-only revocation.
    source.context = BrokerPermissionContextV1((grant,), revision=2)
    with pytest.raises(
        BrokerPermissionError, match="permission_ledger_removed_or_rewritten"
    ):
        authority.require("emit:quotes")


def test_stale_and_rewritten_ledger_cannot_remove_previous_denials() -> None:
    authority, source, _ = _authority()
    authority.require_admission()
    grant = replace(source.context.grants[0], nonce="d" * 32)
    appended = tuple(
        sorted((*source.context.grants, grant), key=lambda x: x.artifact_id)
    )
    source.context = replace(source.context, grants=appended)
    with pytest.raises(
        BrokerPermissionError, match="permission_ledger_revision_regressed"
    ):
        authority.require_admission()
    source.context = replace(source.context, revision=1)
    assert authority.require_admission().grant_id == authority.grant_id
    source.context = replace(source.context, revision=0)
    with pytest.raises(
        BrokerPermissionError, match="permission_ledger_revision_regressed"
    ):
        authority.require_admission()


def test_current_clock_regression_expiry_and_invalid_types_fail_closed() -> (
    None
):
    authority, _, now = _authority()
    authority.require_admission()
    now[0] = 14
    with pytest.raises(
        BrokerPermissionError, match="current_permission_clock_regressed"
    ):
        authority.require_admission()
    now[0] = True
    with pytest.raises(
        BrokerPermissionError, match="invalid_current_permission_clock"
    ):
        authority.read_context()
    now[0] = 30
    with pytest.raises(BrokerPermissionError, match="permission_grant_expired"):
        authority.require_admission()


def test_authority_is_process_local_and_public_selection_is_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority, _, _ = _authority()
    for name in ("manifest", "binding", "grant_id"):
        with pytest.raises(AttributeError):
            setattr(authority, name, None)
    previous = authority.manifest.to_json()
    object.__setattr__(authority.manifest, "candidate_id", "altered")
    assert authority.manifest.to_json() == previous
    with monkeypatch.context() as changed:
        changed.setattr(decisions.os, "getpid", lambda: -1)
        with pytest.raises(
            BrokerPermissionError,
            match="current_process_permission_authority_required",
        ):
            authority.require_admission()


def test_invalid_and_reentrant_sources_never_provide_a_decision() -> None:
    authority, source, _ = _authority()
    source.context = None
    with pytest.raises(
        BrokerPermissionError, match="invalid_current_permission_source"
    ):
        authority.require_admission()
    manifest, binding, grant, context = _fixture()

    class Reentrant:
        def read_context(self):
            nested.require("emit:quotes")
            return context

    nested = BrokerPermissionAuthorityV1(
        manifest, binding, grant.artifact_id, Reentrant(), lambda: 15
    )
    with pytest.raises(
        BrokerPermissionError, match="invalid_current_permission_source"
    ):
        nested.require_admission()


def test_source_returns_detached_context_and_constructor_cannot_forge_admission() -> (
    None
):
    authority, source, _ = _authority()
    result = authority.require_admission()
    fresh = authority.read_context()
    assert fresh is not source.context and fresh == source.context
    object.__setattr__(fresh, "revision", -1)
    assert authority.require_admission().admitted
    with pytest.raises(ValueError):
        replace(result, admitted=False)
    with pytest.raises(ValueError):
        replace(result, missing_required_atoms=("emit:quotes",))
