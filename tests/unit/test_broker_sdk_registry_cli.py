"""Generation-aware metadata CLI selection remains offline and non-activating."""

import json
import sys

import pytest

from histdatacom.broker_plugin_permissions import BrokerPermissionManifestV1
from histdatacom.broker_plugin_registry.cli import main
from tests.unit.test_broker_plugin_registry import _installed, _use


@pytest.mark.parametrize(
    "command", ("list", "inspect", "select", "permissions")
)
@pytest.mark.parametrize("position", ("before", "after"))
def test_cli_keeps_selected_sdk_through_discovery_and_permission_inspection(
    tmp_path, monkeypatch, capsys, command, position
):
    distribution = _installed(
        tmp_path, sdk_min_version="1.1.0", sdk_max_version="2.0.0"
    )
    _use(monkeypatch, distribution)
    permission_reads = []

    def permissions(candidate, *, sdk_version):
        permission_reads.append(sdk_version)
        registration = candidate.registration
        return BrokerPermissionManifestV1(
            candidate.artifact_id,
            registration.distribution_name,
            registration.distribution_version,
            sdk_version,
            registration.provider_ids,
            ("emit:health", "emit:quotes"),
        )

    monkeypatch.setattr(
        "histdatacom.broker_plugin_permissions.read_installed_broker_permissions",
        permissions,
    )
    arguments = (
        ["--sdk-version", "1.1.0", command]
        if position == "before"
        else [command, "--sdk-version", "1.1.0"]
    )
    if command != "list":
        arguments.extend(("--provider", "example"))
    assert main(arguments) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["inventory"]["sdk_version"] == "1.1.0"
    assert len(payload["compatible_candidate_ids"]) == 1
    if command == "permissions":
        assert permission_reads == ["1.1.0"]
        manifest = BrokerPermissionManifestV1.from_dict(
            payload["permission_manifest"]
        )
        assert manifest.sdk_version == "1.1.0"
        assert payload["operator_grants_applied"] is False
    else:
        assert permission_reads == []
    if command != "list":
        assert payload["activation_or_scientific_admission"] is False
    assert not any(name.startswith("fixture_broker") for name in sys.modules)


def test_default_sdk_is_unchanged_and_does_not_silently_upgrade(
    tmp_path, monkeypatch, capsys
):
    _use(monkeypatch, _installed(tmp_path, sdk_min_version="1.1.0"))
    assert main(["select", "--provider", "example"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    failure = json.loads(captured.err)
    assert failure["reason"] == "incompatible_sdk"
    assert failure["installed_candidates"][0]["sdk_compatible"] is False


def test_unknown_sdk_refuses_before_inventory_discovery(monkeypatch, capsys):
    def unexpected(**_):
        pytest.fail("unsupported SDK must refuse before installed inventory IO")

    monkeypatch.setattr(
        "histdatacom.broker_plugin_registry.cli.discover_broker_plugins",
        unexpected,
    )
    assert main(["list", "--sdk-version", "2.0.0"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err)["reason"] == "invalid_selection"
