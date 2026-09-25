"""Synthetic closed-wire permission declarations, never real credentials."""

from __future__ import annotations

import hashlib
import json
from dataclasses import FrozenInstanceError, fields, replace

import pytest

from histdatacom.broker_plugin_permissions import (
    BrokerPermissionBindingV1,
    BrokerPermissionCacheV1,
    BrokerPermissionContextV1,
    BrokerPermissionEndpointV1,
    BrokerPermissionGrantV1,
    BrokerPermissionManifestV1,
    BrokerPermissionRevocationV1,
    validate_permission_atom,
)
from histdatacom.broker_plugin_permissions._wire import (
    canonical_json,
    load_json,
)


def _manifest() -> BrokerPermissionManifestV1:
    return BrokerPermissionManifestV1(
        "broker-plugin-candidate:sha256:" + "a" * 64,
        "synthetic-plugin",
        "1.0.0",
        "1.0.0",
        ("fixture",),
        (
            "emit:health",
            "emit:quotes",
            "network:provider:fixture",
            "secrets:read:paper",
        ),
        (
            "cache:plugin:prices",
            "emit:sizes",
            "raw_payload:emit",
            "subprocess:requested",
        ),
        endpoints=(
            BrokerPermissionEndpointV1(
                "quotes",
                "fixture",
                "https://example.invalid",
                "/v1/",
                ("GET",),
                0,
                4096,
                1000,
                ("paper",),
            ),
        ),
        secret_profiles=("paper",),
        caches=(BrokerPermissionCacheV1("prices", 4096, 10, 1024),),
        subprocess_mode="isolated",
    )


def _binding(manifest: BrokerPermissionManifestV1) -> BrokerPermissionBindingV1:
    return BrokerPermissionBindingV1(
        manifest.candidate_id,
        manifest.artifact_id,
        "1.0.0",
        "fixture",
        "broker-provider-policy-provider-configuration:sha256:" + "b" * 64,
    )


def test_all_artifacts_round_trip_and_hashes_are_independently_recomputed() -> (
    None
):
    manifest = _manifest()
    binding = _binding(manifest)
    grant = BrokerPermissionGrantV1(
        binding, manifest.declared_atoms, "operator", 10, 30, "c" * 32
    )
    revoked = BrokerPermissionRevocationV1(
        grant.artifact_id, 20, "operator-denied"
    )
    context = BrokerPermissionContextV1((grant,), (revoked,), 1)
    for value in (manifest, binding, grant, revoked, context):
        data = value.to_dict()
        encoded = json.dumps(
            {
                "schema_version": data["schema_version"],
                "payload": data["payload"],
            },
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        assert value.artifact_id.endswith(hashlib.sha256(encoded).hexdigest())
        assert type(value).from_dict(data) == value
        assert type(value).from_json(value.to_json()) == value
        with pytest.raises(FrozenInstanceError):
            setattr(value, fields(value)[0].name, None)
        with pytest.raises(ValueError):
            type(value).from_dict({**data, "hidden_grant": True})
        with pytest.raises(ValueError):
            type(value).from_dict({**data, "artifact_id": "0" * 64})
        with pytest.raises(ValueError):
            type(value).from_json(" " + value.to_json())
    assert "secret_value" not in context.to_json()
    assert "/Users/" not in context.to_json()


@pytest.mark.parametrize(
    "atom",
    [
        "emit:quotes",
        "emit:health",
        "emit:sizes",
        "raw_payload:emit",
        "subprocess:requested",
        "network:provider:fixture",
        "secrets:read:paper",
        "cache:plugin:prices",
    ],
)
def test_closed_permission_atom_vocabulary(atom: str) -> None:
    validate_permission_atom(atom)


@pytest.mark.parametrize(
    "atom",
    [
        "",
        "*",
        "filesystem:*",
        "store:write",
        "experiments:mutate",
        "secrets:list",
        "network:provider:*",
        "network:provider:",
        "network:provider:../fixture",
        "network:provider:https://example.invalid",
        "secrets:read:private/key",
        "cache:plugin:/tmp",
        "cache:plugin:prices\x00",
        "emit:quote",
        "emit:quotes:all",
        "subprocess:any",
        "subprocess:requested ",
        True,
        1,
        None,
    ],
)
def test_unknown_or_invalidly_scoped_atoms_refuse(atom: object) -> None:
    with pytest.raises(ValueError):
        validate_permission_atom(atom)


@pytest.mark.parametrize(
    "changes",
    [
        {"required_atoms": ("emit:health", "emit:health")},
        {"required_atoms": ("emit:quotes", "emit:health")},
        {"optional_atoms": ("emit:health",)},
        {"resource_abi": "arbitrary_import"},
        {"resource_abi": "none"},
        {"endpoints": ()},
        {"secret_profiles": ()},
        {"caches": ()},
        {"subprocess_mode": "none"},
        {"subprocess_mode": "host"},
        {"provider_ids": ("other",)},
        {"distribution_name": "Not_Normalized"},
        {"sdk_version": "01.0.0"},
        {"candidate_id": "broker-plugin-candidate:sha256:" + "g" * 64},
    ],
)
def test_resource_inventory_and_binding_are_closed(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        replace(_manifest(), **changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"origin": "http://provider.example"},
        {"origin": "https://u:p@example.invalid"},
        {"origin": "https://example.invalid/"},
        {"origin": "https://example.invalid?q=1"},
        {"origin": "https://EXAMPLE.invalid"},
        {"origin": "https://example.invalid:0"},
        {"origin": "https://example.invalid:65536"},
        {"origin": "https://example.invalid#x"},
        {"origin": "file:///etc"},
        {"origin": "https://exa mple.invalid"},
        {"path_prefix": "/api"},
        {"path_prefix": "/api/../"},
        {"path_prefix": "/%2e%2e/"},
        {"path_prefix": "//evil/"},
        {"path_prefix": "/api/?x/"},
        {"path_prefix": "/api/\\/"},
        {"methods": ()},
        {"methods": ("CONNECT",)},
        {"methods": ("get",)},
        {"max_request_bytes": -1},
        {"max_response_bytes": 0},
        {"timeout_ms": 0},
        {"timeout_ms": 60_001},
        {"timeout_ms": True},
    ],
)
def test_endpoint_descriptors_bound_actual_operations(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        replace(_manifest().endpoints[0], **changes)


@pytest.mark.parametrize(
    "origin",
    [
        "http://127.0.0.1:8080",
        "http://[::1]:8080",
        "https://example.invalid:443",
    ],
)
def test_explicit_loopback_fixture_and_https_origins(origin: str) -> None:
    assert replace(_manifest().endpoints[0], origin=origin).origin == origin


@pytest.mark.parametrize(
    "changes",
    [
        {"cache_id": "/data/products"},
        {"max_bytes": 0},
        {"max_bytes": 1_048_577},
        {"max_items": 0},
        {"max_items": 1025},
        {"max_item_bytes": 4097},
        {"max_item_bytes": True},
    ],
)
def test_cache_descriptor_is_opaque_and_bounded(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        replace(_manifest().caches[0], **changes)


def test_none_abi_is_only_emission_and_unused_secrets_cannot_be_declared() -> (
    None
):
    simple = BrokerPermissionManifestV1(
        _manifest().candidate_id,
        "synthetic-plugin",
        "1.0.0",
        "1.0.0",
        ("fixture",),
        ("emit:quotes",),
        resource_abi="none",
    )
    assert simple.resource_abi == "none"
    with pytest.raises(ValueError, match="opaque secrets"):
        replace(
            _manifest(),
            endpoints=(replace(_manifest().endpoints[0], secret_profiles=()),),
        )


def test_constructor_detaches_mutable_sequences_and_nested_records() -> None:
    endpoint = _manifest().endpoints[0]
    endpoints = [endpoint]
    manifest = replace(_manifest(), endpoints=endpoints)
    expected = manifest.to_json()
    endpoints.clear()
    object.__setattr__(endpoint, "origin", "https://changed.invalid")
    assert manifest.to_json() == expected
    payload = manifest.to_payload()
    restored = BrokerPermissionManifestV1.from_payload(payload)
    payload["endpoints"].clear()
    assert restored == manifest


@pytest.mark.parametrize(
    "changes",
    [
        {"issued_at_ns": True},
        {"issued_at_ns": -1},
        {"expires_at_ns": 10},
        {"expires_at_ns": 2**63},
        {"operator_id": "../../operator"},
        {"nonce": "short"},
        {"granted_atoms": ("secrets:*",)},
    ],
)
def test_grant_clocks_identity_and_atoms_fail_closed(
    changes: dict[str, object],
) -> None:
    grant = BrokerPermissionGrantV1(
        _binding(_manifest()), (), "operator", 10, 30, "c" * 32
    )
    with pytest.raises(ValueError):
        replace(grant, **changes)


def test_context_requires_retained_grants_and_unique_sorted_entries() -> None:
    grant = BrokerPermissionGrantV1(
        _binding(_manifest()), (), "operator", 10, 30, "c" * 32
    )
    revocation = BrokerPermissionRevocationV1(grant.artifact_id, 20, "revoked")
    with pytest.raises(ValueError):
        BrokerPermissionContextV1((), (revocation,))
    with pytest.raises(ValueError):
        BrokerPermissionContextV1((grant, grant))
    with pytest.raises(ValueError):
        BrokerPermissionContextV1(
            (grant,), (replace(revocation, revoked_at_ns=9),)
        )
    with pytest.raises(ValueError):
        BrokerPermissionContextV1((grant,), (), True)


def test_strict_wire_duplicate_keys_expanded_aliases_and_scalar_domains() -> (
    None
):
    with pytest.raises(ValueError, match="duplicate"):
        load_json('{"x":1,"x":2}')
    for value in (
        float("inf"),
        float("nan"),
        2**63,
        object(),
        {1: "not-a-key"},
    ):
        with pytest.raises(ValueError):
            canonical_json(value)
    with pytest.raises(ValueError, match="node/depth"):
        canonical_json([[0] * 4096] * 64)
    with pytest.raises(ValueError, match="byte bound"):
        canonical_json(["😀" * 65_536] * 16)
    recursive: list[object] = []
    recursive.append(recursive)
    with pytest.raises(ValueError, match="node/depth"):
        canonical_json(recursive)
