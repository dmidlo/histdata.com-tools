"""Generated installed-wheel metadata: no plugin imports, broker or secrets."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import runpy
import sys
from dataclasses import replace
from importlib import metadata
from pathlib import Path
from zipfile import ZipFile

import pytest

from histdatacom.broker_plugin_permissions import (
    BrokerPermissionError,
    BrokerPermissionManifestV1,
    discovery,
    permission_resource_path,
    read_installed_broker_permissions,
)
from histdatacom.broker_plugin_registry import (
    BrokerPluginCandidateV1,
    BrokerPluginRegistrationV1,
    registration_resource_path,
)


def _write_record(site: Path, entries: dict[str, bytes]) -> None:
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    for path, body in sorted(entries.items()):
        digest = (
            base64.urlsafe_b64encode(hashlib.sha256(body).digest())
            .rstrip(b"=")
            .decode("ascii")
        )
        writer.writerow((path, "sha256=" + digest, len(body)))
        target = site / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
    record = "permission_fixture-1.0.0.dist-info/RECORD"
    writer.writerow((record, "", ""))
    (site / record).write_text(output.getvalue(), encoding="ascii")


def _installed(tmp_path: Path):
    registration = BrokerPluginRegistrationV1(
        "org.example.permissions",
        "1.0.0",
        "Synthetic permission fixture",
        "permission-fixture",
        "1.0.0",
        "permission_fixture.plugin:factory",
        "1.0.0",
        "2.0.0",
        ("fixture",),
        ("health.v1", "quotes.v1"),
    )
    implementation = b'raise RuntimeError("PLUGIN_MUST_NOT_BE_IMPORTED")\n'
    native_registration = registration.to_json().encode("ascii")
    candidate = BrokerPluginCandidateV1(
        registration,
        hashlib.sha256(native_registration).hexdigest(),
        hashlib.sha256(implementation).hexdigest(),
    )
    manifest = BrokerPermissionManifestV1(
        candidate.artifact_id,
        registration.distribution_name,
        registration.distribution_version,
        "1.0.0",
        registration.provider_ids,
        ("emit:health", "emit:quotes"),
        resource_abi="none",
    )
    info = "permission_fixture-1.0.0.dist-info"
    entries = {
        "permission_fixture/__init__.py": b'raise RuntimeError("PARENT_MUST_NOT_BE_IMPORTED")\n',
        "permission_fixture/plugin.py": implementation,
        registration_resource_path(
            registration.plugin_id, registration.entry_point
        ): native_registration,
        permission_resource_path(
            registration.plugin_id, registration.entry_point
        ): manifest.to_json().encode("ascii"),
        info
        + "/METADATA": b"Metadata-Version: 2.1\nName: permission-fixture\nVersion: 1.0.0\n",
        info
        + "/entry_points.txt": b"[histdatacom.broker_plugins.v1]\norg.example.permissions = permission_fixture.plugin:factory\n",
        info
        + "/WHEEL": b"Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
    }
    site = tmp_path / "site"
    _write_record(site, entries)
    return (
        metadata.PathDistribution(site / info),
        candidate,
        manifest,
        site,
        entries,
    )


def _use(
    monkeypatch: pytest.MonkeyPatch, *distributions: metadata.PathDistribution
) -> None:
    monkeypatch.setattr(
        discovery.metadata, "distributions", lambda: iter(distributions)
    )


def test_resource_path_is_canonical_sibling_and_cannot_be_executable_import() -> (
    None
):
    assert permission_resource_path(
        "org.example.permissions", "permission_fixture.plugin:factory"
    ) == (
        "permission_fixture/_histdatacom_broker_permissions/org.example.permissions.json"
    )
    for entry in (
        "../plugin:factory",
        "plugin:factory",
        "x.y:factory()",
        "a.b:factory ",
    ):
        with pytest.raises(BrokerPermissionError):
            permission_resource_path("org.example.permissions", entry)


def test_fresh_record_bound_discovery_without_imports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    distribution, candidate, manifest, _, _ = _installed(tmp_path)
    _use(monkeypatch, distribution)
    assert read_installed_broker_permissions(candidate) == manifest
    assert (
        read_installed_broker_permissions(candidate).to_json()
        == manifest.to_json()
    )
    assert not any(
        name.startswith("permission_fixture") for name in sys.modules
    )
    _use(monkeypatch)
    with pytest.raises(
        BrokerPermissionError, match="permission_installed_selection_changed"
    ):
        read_installed_broker_permissions(candidate)


@pytest.mark.parametrize(
    "changes",
    [
        {"candidate_id": "broker-plugin-candidate:sha256:" + "a" * 64},
        {"distribution_name": "another-plugin"},
        {"distribution_version": "1.0.1"},
        {"sdk_version": "2.0.0"},
        {"provider_ids": ("other",)},
    ],
)
def test_resealed_permissions_for_different_candidate_never_match(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    changes: dict[str, object],
) -> None:
    distribution, candidate, manifest, site, entries = _installed(tmp_path)
    path = permission_resource_path(
        candidate.registration.plugin_id, candidate.registration.entry_point
    )
    entries[path] = replace(manifest, **changes).to_json().encode("ascii")
    _write_record(site, entries)
    _use(monkeypatch, distribution)
    with pytest.raises(
        BrokerPermissionError, match="permission_manifest_candidate_mismatch"
    ):
        read_installed_broker_permissions(candidate)


@pytest.mark.parametrize(
    "target", ["permission", "implementation", "registration"]
)
def test_owned_bytes_cannot_change_without_record_and_candidate_reverification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
) -> None:
    distribution, candidate, _, site, _ = _installed(tmp_path)
    paths = {
        "permission": permission_resource_path(
            candidate.registration.plugin_id, candidate.registration.entry_point
        ),
        "registration": registration_resource_path(
            candidate.registration.plugin_id, candidate.registration.entry_point
        ),
        "implementation": "permission_fixture/plugin.py",
    }
    path = site / paths[target]
    path.write_bytes(path.read_bytes() + b"\n")
    _use(monkeypatch, distribution)
    with pytest.raises(BrokerPermissionError):
        read_installed_broker_permissions(candidate)


def test_missing_permission_declaration_does_not_infer_all_or_empty_grants(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    distribution, candidate, _, site, entries = _installed(tmp_path)
    path = permission_resource_path(
        candidate.registration.plugin_id, candidate.registration.entry_point
    )
    (site / path).unlink()
    del entries[path]
    _write_record(site, entries)
    _use(monkeypatch, distribution)
    with pytest.raises(
        BrokerPermissionError, match="invalid_installed_permission_manifest"
    ):
        read_installed_broker_permissions(candidate)


def test_duplicate_installed_owners_refuse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    distribution, candidate, _, _, _ = _installed(tmp_path)
    other, _, _, _, _ = _installed(tmp_path / "duplicate")
    _use(monkeypatch, distribution, other)
    with pytest.raises(BrokerPermissionError):
        read_installed_broker_permissions(candidate)


def test_permission_symlink_leaf_never_qualifies_even_if_bytes_match(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    distribution, candidate, _, site, _ = _installed(tmp_path)
    path = site / permission_resource_path(
        candidate.registration.plugin_id, candidate.registration.entry_point
    )
    outside = tmp_path / "same-bytes.json"
    outside.write_bytes(path.read_bytes())
    path.unlink()
    try:
        path.symlink_to(outside)
    except OSError:
        pytest.skip("symlink unavailable")
    _use(monkeypatch, distribution)
    with pytest.raises(BrokerPermissionError):
        read_installed_broker_permissions(candidate)


def test_changing_permission_resource_during_native_revalidation_refuses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    distribution, candidate, manifest, _, _ = _installed(tmp_path)
    _use(monkeypatch, distribution)
    native_read = discovery._read_owned_resource
    reads = 0

    def switched(*args, **kwargs):
        nonlocal reads
        reads += 1
        if reads == 2:
            return (
                replace(manifest, optional_atoms=("emit:sizes",))
                .to_json()
                .encode("ascii")
            )
        return native_read(*args, **kwargs)

    monkeypatch.setattr(discovery, "_read_owned_resource", switched)
    with pytest.raises(
        BrokerPermissionError, match="permission_manifest_changed_during_read"
    ):
        read_installed_broker_permissions(candidate)


@pytest.mark.parametrize(
    "filename,builder",
    [
        ("broker_plugin_wheel.py", "build_fixture_plugin_wheel"),
        ("broker_capability_wheel.py", "build_capability_wheel"),
        ("broker_lifecycle_wheel.py", "build_lifecycle_wheel"),
    ],
)
def test_existing_generated_wheels_retain_explicit_emission_only_declarations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    filename: str,
    builder: str,
) -> None:
    fixture = Path(__file__).resolve().parents[1] / "fixtures" / filename
    wheel = runpy.run_path(str(fixture))[builder](tmp_path / "wheels")
    site = tmp_path / "installed"
    with ZipFile(wheel) as archive:
        archive.extractall(site)
    distribution = next(metadata.distributions(path=[str(site)]))
    _use(monkeypatch, distribution)
    candidate = discovery.discover_broker_plugins().candidates[0]
    manifest = read_installed_broker_permissions(candidate)
    assert manifest.resource_abi == "none"
    assert manifest.required_atoms == (
        "emit:health",
        "emit:quotes",
        "emit:sizes",
        "raw_payload:emit",
    )
    assert (
        not manifest.endpoints
        and not manifest.caches
        and not manifest.secret_profiles
    )
