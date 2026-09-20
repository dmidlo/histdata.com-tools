"""Installed provenance sanitization, not trust inferred from installation."""

from __future__ import annotations

from importlib import metadata
import json
from pathlib import Path
import runpy
import secrets
from zipfile import ZipFile

import pytest

from histdatacom.broker_plugin_registry import discover_broker_plugins
from histdatacom.broker_plugin_security import (
    installed_software_provenance,
    BrokerSoftwareProvenanceV1,
    BrokerSecurityError,
)

ROOT = Path(__file__).resolve().parents[2]
BUILD = runpy.run_path(str(ROOT / "tests/fixtures/broker_security_wheel.py"))[
    "build_security_wheel"
]


def install(tmp_path, monkeypatch, origin):
    site = tmp_path / "site"
    site.mkdir()
    with ZipFile(
        BUILD(tmp_path / "wheel", direct_url=origin, installer="pip")
    ) as archive:
        archive.extractall(site)
    distribution = next(metadata.distributions(path=[str(site)]))
    monkeypatch.setattr(
        metadata, "distributions", lambda: iter((distribution,))
    )
    candidate = discover_broker_plugins().candidates[0]
    return site, candidate


@pytest.mark.parametrize(
    "kind,details",
    [
        ("archive", {"archive_info": {"hashes": {"sha256": "e" * 64}}}),
        ("vcs", {"vcs_info": {"vcs": "git", "commit_id": "f" * 40}}),
        ("local_directory", {"dir_info": {"editable": True}}),
    ],
)
def test_raw_origin_credentials_and_private_paths_are_never_retained(
    tmp_path, monkeypatch, kind, details
):
    private = secrets.token_urlsafe(24)
    origin = {
        "url": f"https://private-account:{private}@example.invalid/private-account/source?token={private}",
        **details,
    }
    site, candidate = install(tmp_path, monkeypatch, origin)
    software = installed_software_provenance(candidate)
    assert software.origin_kind == kind and software.installer == "pip"
    assert BrokerSoftwareProvenanceV1.from_json(software.to_json()) == software
    assert private not in software.to_json()
    assert "private-account" not in software.to_json()
    assert str(site) not in software.to_json()
    origin["url"] = "file:///private/other-account/" + secrets.token_hex(8)
    (
        site / "histdatacom_security_fixture-1.0.0.dist-info/direct_url.json"
    ).write_text(json.dumps(origin))
    assert (
        installed_software_provenance(candidate).to_json() == software.to_json()
    )


def test_software_provenance_rechecks_installed_module_and_bounds_attestation(
    tmp_path, monkeypatch
):
    site, candidate = install(tmp_path, monkeypatch, {})
    software = installed_software_provenance(
        candidate, attestation=b"synthetic-public-attestation"
    )
    assert software.attestation_sha256 and not software.attestation_verified
    with pytest.raises(BrokerSecurityError):
        installed_software_provenance(candidate, attestation=b"x" * 65_537)
    (site / "security_fixture/plugin.py").write_text(
        "raise RuntimeError('changed')"
    )
    with pytest.raises(BrokerSecurityError, match="security_identity_mismatch"):
        installed_software_provenance(candidate)
