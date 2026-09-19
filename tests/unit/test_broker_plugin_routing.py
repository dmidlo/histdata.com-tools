"""Public plugin inventory routing does not enter the download/runtime path."""

from __future__ import annotations

import sys
from types import ModuleType

import pytest

from histdatacom import histdata_com
from histdatacom.readme_help import generated_main_help


@pytest.mark.parametrize(
    ("arguments", "forwarded"),
    [
        (["broker-plugins", "list", "--json"], ["list", "--json"]),
        (["broker-plugins", "--help"], ["--help"]),
        (
            ["--config", "must-not-be-read.yaml", "broker-plugins", "list"],
            ["--config", "must-not-be-read.yaml", "list"],
        ),
    ],
)
def test_plugin_command_routes_before_legacy_runtime(
    monkeypatch: pytest.MonkeyPatch,
    arguments: list[str],
    forwarded: list[str],
) -> None:
    """Dispatch preserves arguments and the registry's refusal exit code."""
    received: list[list[str]] = []

    def plugin_main(argv: list[str]) -> int:
        received.append(argv)
        return 2

    def forbidden_legacy_path(*args: object, **kwargs: object) -> None:
        pytest.fail("offline plugin routing entered legacy runtime")

    module = ModuleType("histdatacom.broker_plugin_registry.cli")
    module.main = plugin_main  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setattr(histdata_com, "_HistDataCom", forbidden_legacy_path)
    monkeypatch.setattr(histdata_com, "Options", forbidden_legacy_path)
    monkeypatch.setattr(sys, "argv", ["histdatacom", *arguments])

    assert histdata_com.main() == 2
    assert received == [forwarded]


def test_main_help_exposes_offline_plugin_inventory() -> None:
    text = generated_main_help()
    assert "broker-plugins  Inspect installed broker plugins offline" in text
    assert "histdatacom broker-plugins --help" in text


def test_real_plugin_parser_refuses_config_before_discovery(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from histdatacom.broker_plugin_registry import cli

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("invalid offline arguments reached discovery or runtime")

    monkeypatch.setattr(cli, "discover_broker_plugins", forbidden)
    monkeypatch.setattr(histdata_com, "Options", forbidden)
    monkeypatch.setattr(histdata_com, "_HistDataCom", forbidden)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--config",
            "must-not-be-read.yaml",
            "broker-plugins",
            "list",
        ],
    )
    assert histdata_com.main() == 2
    output = capsys.readouterr()
    assert '"reason": "invalid_selection"' in output.err
    assert "must-not-be-read" not in output.out + output.err
