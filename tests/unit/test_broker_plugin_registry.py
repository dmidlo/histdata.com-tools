"""Metadata-only discovery, deterministic selection, and retained identities."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from importlib import metadata
import json
import os
from pathlib import Path
import runpy
import sys
from zipfile import ZipFile

import pytest

from histdatacom.broker_plugin_registry import (
    BROKER_PLUGIN_ENTRY_POINT_GROUP,
    BrokerPluginCandidateV1,
    BrokerPluginDiscoveryDiagnosticV1,
    BrokerPluginInventoryV1,
    BrokerPluginRegistrationV1,
    BrokerPluginRegistryError,
    BrokerPluginRegistryReason,
    canonical_plugin_registry_json,
    discover_broker_plugins,
    inspect_broker_plugins,
    read_plugin_inventory,
    registration_resource_path,
    select_broker_plugin,
    write_plugin_inventory,
)
from histdatacom.broker_plugin_registry import contracts, discovery
from histdatacom.broker_plugin_registry.cli import main

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = runpy.run_path(str(ROOT / "tests/fixtures/broker_plugin_wheel.py"))


def _registration(**changes: object) -> BrokerPluginRegistrationV1:
    return replace(FIXTURE["fixture_registration"](), **changes)


def _candidate(**changes: object) -> BrokerPluginCandidateV1:
    return BrokerPluginCandidateV1(_registration(**changes), "a" * 64, "b" * 64)


@pytest.mark.parametrize(
    "entry_point",
    [None, "a" * 300, "fixture.plugin:bad value", "fixture:factory"],
)
def test_public_resource_path_refuses_invalid_entry_points(
    entry_point: object,
) -> None:
    with pytest.raises(BrokerPluginRegistryError):
        registration_resource_path("org.example.fixture", entry_point)


def _installed(tmp_path: Path, **changes: object) -> metadata.PathDistribution:
    registration = _registration(**changes)
    wheel = FIXTURE["build_fixture_plugin_wheel"](
        tmp_path / "wheels", registration
    )
    site = tmp_path / "site"
    site.mkdir(parents=True)
    with ZipFile(wheel) as archive:
        archive.extractall(site)
    return next(metadata.distributions(path=[str(site)]))


def _use(
    monkeypatch: pytest.MonkeyPatch, *distributions: metadata.Distribution
) -> None:
    monkeypatch.setattr(
        discovery.metadata, "distributions", lambda: iter(distributions)
    )


def test_fresh_installed_inventory_never_imports_plugin_and_uninstall_refreshes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    distribution = _installed(tmp_path)
    _use(monkeypatch)
    empty = discover_broker_plugins()
    _use(monkeypatch, distribution)
    first = discover_broker_plugins()
    assert len(first.candidates) == 1 and not first.diagnostics
    assert first == discover_broker_plugins()
    assert not any(name.startswith("fixture_broker") for name in sys.modules)
    candidate = select_broker_plugin(first, provider_id="example")
    assert candidate.registration.entry_point == "fixture_broker.plugin:factory"
    _use(monkeypatch)
    assert discover_broker_plugins() == empty


def test_order_and_all_contract_bytes_are_deterministic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = _installed(tmp_path / "first", plugin_version="1.0.0-rc.2")
    second = _installed(
        tmp_path / "second",
        plugin_version="1.0.0-rc.10",
        distribution_name="other-distribution",
    )
    _use(monkeypatch, second, first)
    left = discover_broker_plugins()
    _use(monkeypatch, first, second)
    right = discover_broker_plugins()
    assert left.to_json() == right.to_json()
    assert [c.registration.plugin_version for c in left.candidates] == [
        "1.0.0-rc.2",
        "1.0.0-rc.10",
    ]
    assert BrokerPluginInventoryV1.from_json(left.to_json()) == left
    for candidate in left.candidates:
        assert (
            BrokerPluginCandidateV1.from_json(candidate.to_json()) == candidate
        )
        assert (
            BrokerPluginRegistrationV1.from_json(
                candidate.registration.to_json()
            )
            == candidate.registration
        )
    with pytest.raises(FrozenInstanceError):
        left.sdk_version = "9.0.0"


def test_semver_official_precedence_and_build_identity() -> None:
    versions = (
        "1.0.0-alpha",
        "1.0.0-alpha.1",
        "1.0.0-alpha.beta",
        "1.0.0-beta",
        "1.0.0-beta.2",
        "1.0.0-beta.11",
        "1.0.0-rc.1",
        "1.0.0",
        "1.0.1",
        "1.1.0",
        "2.0.0",
    )
    assert list(reversed(versions)) != sorted(
        reversed(versions), key=contracts.semver_key
    )
    assert sorted(reversed(versions), key=contracts.semver_key) == list(
        versions
    )
    assert contracts.semver_key("1.0.0+first") == contracts.semver_key(
        "1.0.0+second"
    )
    inventory = BrokerPluginInventoryV1(
        (
            _candidate(plugin_version="1.0.0+first"),
            _candidate(plugin_version="1.0.0+second"),
        )
    )
    assert (
        select_broker_plugin(
            inventory,
            plugin_id="org.example.fixture",
            version_constraint="==1.0.0+first",
        ).registration.plugin_version
        == "1.0.0+first"
    )


@pytest.mark.parametrize(
    "version",
    [
        "1",
        "1.0",
        "01.0.0",
        "1.0.0-01",
        "1.0.0-",
        "v1.0.0",
        "1.0.0\n",
        "9" * 200,
    ],
)
def test_bad_semver_fails_closed(version: str) -> None:
    with pytest.raises(BrokerPluginRegistryError):
        _registration(plugin_version=version)


def test_selection_rejects_unknown_ambiguous_duplicates_and_bad_constraints() -> (
    None
):
    one, two = _candidate(), _candidate(plugin_version="2.0.0")
    inventory = BrokerPluginInventoryV1((two, one))
    for kwargs, reason in (
        ({"plugin_id": "org.example.fixture"}, "ambiguous_selection"),
        ({"provider_id": "example"}, "ambiguous_selection"),
        ({"plugin_id": "org.example.unknown"}, "unknown_plugin"),
        ({"plugin_id": "module:factory"}, "invalid_selection"),
        (
            {"plugin_id": "org.example.fixture", "version_constraint": "~=1.0"},
            "invalid_selection",
        ),
    ):
        with pytest.raises(BrokerPluginRegistryError) as error:
            select_broker_plugin(inventory, **kwargs)
        assert error.value.reason.value == reason
        assert error.value.candidates
    assert (
        select_broker_plugin(
            inventory,
            provider_id="example",
            version_constraint=">=1.0.0,<2.0.0",
        )
        == one
    )
    duplicate = BrokerPluginInventoryV1(
        (one, replace(one, implementation_sha256="c" * 64))
    )
    with pytest.raises(BrokerPluginRegistryError, match="ambiguous_selection"):
        select_broker_plugin(
            duplicate,
            plugin_id="org.example.fixture",
            version_constraint="==1.0.0",
        )


def test_provider_filter_cannot_hide_duplicate_active_id() -> None:
    inventory = BrokerPluginInventoryV1(
        (
            _candidate(),
            _candidate(plugin_version="2.0.0", provider_ids=("other",)),
        )
    )
    with pytest.raises(BrokerPluginRegistryError, match="ambiguous_selection"):
        select_broker_plugin(inventory, provider_id="example")
    assert (
        select_broker_plugin(
            inventory, provider_id="example", version_constraint="<2.0.0"
        ).registration.plugin_version
        == "1.0.0"
    )


def test_incompatible_and_malformed_inventory_never_selects() -> None:
    inventory = BrokerPluginInventoryV1(
        (_candidate(sdk_min_version="2.0.0", sdk_max_version="3.0.0"),)
    )
    with pytest.raises(BrokerPluginRegistryError, match="incompatible_sdk"):
        select_broker_plugin(inventory, provider_id="example")
    assert (
        inspect_broker_plugins(inventory, provider_id="example")
        == inventory.candidates
    )
    invalid = BrokerPluginInventoryV1(
        (_candidate(),),
        (
            BrokerPluginDiscoveryDiagnosticV1(
                BrokerPluginRegistryReason.RESOURCE_UNAVAILABLE
            ),
        ),
    )
    with pytest.raises(BrokerPluginRegistryError, match="incomplete_inventory"):
        select_broker_plugin(invalid, provider_id="example")


@pytest.mark.parametrize(
    "changes",
    [
        {"plugin_id": "unsafe:module"},
        {"provider_ids": ["example"]},
        {"capabilities": ("z", "a")},
        {"sdk_min_version": "2.0.0"},
        {"source_repository": "https://user:SECRET@example.org"},
        {"source_repository": "https://example.org/?token=SECRET"},
        {"source_repository": "https://[invalid"},
        {"source_repository": "https://example.org:invalid/repository"},
        {"source_repository": "https://example.org/a b"},
        {"source_repository": "https://example.org/a\\b"},
        {"source_repository": False},
        {"build_id": False},
        {"source_revision": False},
        {"distribution_name": "Bearer SECRET"},
        {"entry_point": "../../evil:factory"},
    ],
)
def test_registration_refuses_unsafe_or_wrong_typed_fields(
    changes: dict[str, object],
) -> None:
    with pytest.raises(BrokerPluginRegistryError) as error:
        _registration(**changes)
    assert "SECRET" not in str(error.value)


@pytest.mark.parametrize(
    "mutation", ["unknown", "identity", "nested", "schema"]
)
def test_canonical_restoration_rejects_tampered_inventory(
    mutation: str,
) -> None:
    payload = BrokerPluginInventoryV1((_candidate(),)).to_dict()
    if mutation == "unknown":
        payload["extra"] = "value"
    elif mutation == "identity":
        payload["artifact_id"] = "broker-plugin-inventory:sha256:" + "0" * 64
    elif mutation == "schema":
        payload["schema_version"] = "histdatacom.broker-plugin-inventory.v2"
    else:
        payload["candidates"][0]["registration"]["plugin_version"] = "3.0.0"
    with pytest.raises(BrokerPluginRegistryError):
        BrokerPluginInventoryV1.from_dict(payload)


@pytest.mark.parametrize(
    "text",
    [
        '{"a":1,"a":2}',
        '{"a":NaN}',
        '{"a":Infinity}',
        "[1]",
        '{"a":1.5}',
        '{"a":9223372036854775808}',
        "[" * 1500 + "]" * 1500,
    ],
)
def test_json_refuses_duplicate_nonfinite_noncanonical_and_deep_values(
    text: str,
) -> None:
    with pytest.raises(BrokerPluginRegistryError):
        BrokerPluginInventoryV1.from_json(text)


def test_bounds_and_closed_error_diagnostics() -> None:
    with pytest.raises(BrokerPluginRegistryError):
        BrokerPluginInventoryV1(tuple(_candidate() for _ in range(129)))
    cycle = []
    cycle.append(cycle)
    with pytest.raises(BrokerPluginRegistryError):
        canonical_plugin_registry_json(cycle)
    for candidates in (("Bearer SECRET",), ("x" * 10000,), ["x"]):
        with pytest.raises(ValueError) as error:
            BrokerPluginRegistryError(
                BrokerPluginRegistryReason.UNKNOWN_PLUGIN, candidates
            )
        assert "SECRET" not in str(error.value)
    with pytest.raises(ValueError):
        BrokerPluginRegistryError("SECRET")


@pytest.mark.parametrize(
    "target",
    [
        "registration",
        "implementation",
        "missing_record",
        "wrong_owner",
        "symlink",
    ],
)
def test_record_and_package_association_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, target: str
) -> None:
    distribution = _installed(tmp_path)
    _use(monkeypatch, distribution)
    site = Path(distribution.locate_file(""))
    registration = _registration()
    resource = site / registration_resource_path(
        registration.plugin_id, registration.entry_point
    )
    if target == "registration":
        resource.write_bytes(resource.read_bytes() + b" ")
    elif target == "implementation":
        (site / "fixture_broker/plugin.py").write_text("changed")
    elif target == "missing_record":
        next(site.glob("*.dist-info/RECORD")).unlink()
    elif target == "wrong_owner":
        metadata_path = next(site.glob("*.dist-info/METADATA"))
        metadata_path.write_text(
            metadata_path.read_text().replace(
                "Name: histdatacom-fixture-broker", "Name: other-package"
            )
        )
    else:
        outside = tmp_path / "outside.json"
        outside.write_bytes(resource.read_bytes())
        resource.unlink()
        resource.symlink_to(outside)
    inventory = discover_broker_plugins()
    assert not inventory.candidates and len(inventory.diagnostics) == 1
    with pytest.raises(BrokerPluginRegistryError, match="incomplete_inventory"):
        select_broker_plugin(inventory, provider_id="example")


def test_malformed_distributions_and_capacity_are_order_independent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first, second = _installed(tmp_path / "a"), _installed(
        tmp_path / "b", distribution_name="other"
    )
    for dist in (first, second):
        next(Path(dist.locate_file("")).glob("*.dist-info/RECORD")).unlink()
    _use(monkeypatch, first, second)
    before = discover_broker_plugins().to_json()
    _use(monkeypatch, second, first)
    assert discover_broker_plugins().to_json() == before
    monkeypatch.setattr(discovery, "MAX_PLUGIN_REGISTRATIONS", 1)
    with pytest.raises(BrokerPluginRegistryError, match="inventory_limit"):
        discover_broker_plugins()
    _use(monkeypatch, object(), object())
    with pytest.raises(BrokerPluginRegistryError, match="inventory_limit"):
        discover_broker_plugins()


def test_persistence_verifies_hash_size_identity_and_never_overwrites(
    tmp_path: Path,
) -> None:
    inventory = BrokerPluginInventoryV1((_candidate(),))
    path = tmp_path / "inventory.json"
    reference = write_plugin_inventory(inventory, path)
    assert read_plugin_inventory(reference) == inventory
    assert reference == write_plugin_inventory(inventory, path)
    with pytest.raises(BrokerPluginRegistryError):
        write_plugin_inventory(BrokerPluginInventoryV1(), path)
    assert path.read_text() == inventory.to_json()
    for invalid in (
        replace(reference, sha256="0" * 64),
        replace(reference, size_bytes=0),
        replace(reference, metadata={}),
        replace(reference, kind="other"),
    ):
        with pytest.raises(BrokerPluginRegistryError):
            read_plugin_inventory(invalid)
    path.write_text(path.read_text() + " ")
    with pytest.raises(BrokerPluginRegistryError):
        read_plugin_inventory(reference)


def test_cli_offline_inspection_snapshot_and_safe_refusal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _use(monkeypatch, _installed(tmp_path))
    assert main(["--snapshot", str(tmp_path / "inventory.json"), "list"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert (
        output["inventory"]["entry_point_group"]
        == BROKER_PLUGIN_ENTRY_POINT_GROUP
    )
    assert main(["inspect", "--provider", "example"]) == 0
    assert not json.loads(capsys.readouterr().out)[
        "activation_or_scientific_admission"
    ]
    assert main(["select", "--plugin-id", "module:SECRET"]) == 2
    assert "SECRET" not in capsys.readouterr().err
    assert main(["--config", "no-SECRET-file.yaml", "list"]) == 2
    assert "SECRET" not in capsys.readouterr().err


def test_cli_inspects_incompatible_candidate_and_accepts_flags_in_both_positions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    distribution = _installed(
        tmp_path, sdk_min_version="2.0.0", sdk_max_version="3.0.0"
    )
    _use(monkeypatch, distribution)
    assert main(["--json", "list"]) == 0
    before = capsys.readouterr().out
    assert main(["list", "--json"]) == 0
    assert capsys.readouterr().out == before
    assert (
        main(
            [
                "inspect",
                "--provider",
                "example",
                "--json",
                "--snapshot",
                str(tmp_path / "snapshot.json"),
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert len(payload["inspected_declarations"]) == 1
    assert payload["compatible_candidate_ids"] == []
    assert main(["select", "--provider", "example", "--json"]) == 2
    failure = json.loads(capsys.readouterr().err)
    assert failure["reason"] == "incompatible_sdk"
    assert (
        failure["installed_candidates"][0]["plugin_id"] == "org.example.fixture"
    )


@pytest.mark.parametrize("kind", ["symlink", "directory", "fifo"])
def test_persistence_refuses_nonregular_files_without_following_or_blocking(
    tmp_path: Path, kind: str
) -> None:
    inventory = BrokerPluginInventoryV1((_candidate(),))
    reference = write_plugin_inventory(inventory, tmp_path / "real.json")
    bad = tmp_path / "bad.json"
    if kind == "symlink":
        bad.symlink_to(reference.path)
    elif kind == "directory":
        bad.mkdir()
    elif hasattr(os, "mkfifo"):
        os.mkfifo(bad)
    else:
        pytest.skip("FIFO is not supported on this platform")
    with pytest.raises(BrokerPluginRegistryError):
        read_plugin_inventory(replace(reference, path=str(bad)))
    with pytest.raises(BrokerPluginRegistryError):
        write_plugin_inventory(inventory, bad)
    assert Path(reference.path).read_text() == inventory.to_json()


def test_missing_metadata_and_resource_limits_stay_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first, second = _installed(tmp_path / "a"), _installed(tmp_path / "b")
    for distribution in (first, second):
        metadata_path = next(
            Path(distribution.locate_file("")).glob("*.dist-info/METADATA")
        )
        metadata_path.write_text("Metadata-Version: 2.1\nVersion: 1.0.0\n")
    _use(monkeypatch, first, second)
    monkeypatch.setattr(discovery, "MAX_PLUGIN_REGISTRATIONS", 1)
    with pytest.raises(BrokerPluginRegistryError, match="inventory_limit"):
        discover_broker_plugins()


@pytest.mark.parametrize(
    "limit",
    [
        "MAX_DISTRIBUTION_FILES",
        "MAX_IMPLEMENTATION_BYTES",
        "MAX_REGISTRATION_BYTES",
    ],
)
def test_discovery_resource_admission_limits_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, limit: str
) -> None:
    distribution = _installed(tmp_path)
    _use(monkeypatch, distribution)
    monkeypatch.setattr(discovery, limit, 1)
    inventory = discover_broker_plugins()
    assert not inventory.candidates and len(inventory.diagnostics) == 1
