"""Tests for instrument group discovery CLI commands."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from histdatacom import histdata_com
from histdatacom import groups_cli
from histdatacom.fx_enums import (
    MAJOR_TRIANGLE_PAIR_GROUPS,
    MAJOR_TRIANGLE_RELATIONSHIPS,
    MAJOR_TRIANGLE_SYMBOLS,
)


def test_groups_list_outputs_basket_names(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Default group listing should stay focused on broad user baskets."""
    assert groups_cli.main(["list"]) == 0

    output = capsys.readouterr().out

    assert "Instrument group baskets" in output
    assert "major-triangles" in output
    assert "28 symbols, 56 triangle relationships" in output
    assert "triangle-eurgbp-eurusd-gbpusd" not in output
    assert "groups list --triangles" in output


def test_groups_list_triangles_json(capsys: pytest.CaptureFixture[str]) -> None:
    """Individual triangle listing should expose names and readable rules."""
    assert groups_cli.main(["list", "--triangles", "--json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    groups = {group["name"]: group for group in payload["groups"]}
    triangle = groups["triangle-eurgbp-eurusd-gbpusd"]

    assert payload["mode"] == "triangles"
    assert payload["group_count"] == len(MAJOR_TRIANGLE_PAIR_GROUPS)
    assert triangle["symbols"] == ["eurgbp", "eurusd", "gbpusd"]
    assert triangle["relationship"] == {
        "direct": "eurgbp",
        "numerator": "eurusd",
        "denominator": "gbpusd",
        "rule": "EURUSD / GBPUSD ~= EURGBP",
    }


def test_groups_show_major_triangles_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The aggregate major-triangles group should report its full scope."""
    assert groups_cli.main(["show", "major-triangles", "--json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    group = payload["group"]

    assert group["name"] == "major-triangles"
    assert group["type"] == "basket"
    assert group["symbol_count"] == len(MAJOR_TRIANGLE_SYMBOLS)
    assert group["relationship_count"] == len(MAJOR_TRIANGLE_RELATIONSHIPS)
    assert group["symbols"] == list(MAJOR_TRIANGLE_SYMBOLS)


def test_groups_show_triangle_human_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Showing one triangle should explain the direct/numerator/denominator rule."""
    assert groups_cli.main(["show", "triangle-eurgbp-eurusd-gbpusd"]) == 0

    output = capsys.readouterr().out

    assert "Group: triangle-eurgbp-eurusd-gbpusd" in output
    assert "Type: triangle" in output
    assert "Symbols (3): eurgbp, eurusd, gbpusd" in output
    assert "Direct: eurgbp" in output
    assert "Numerator: eurusd" in output
    assert "Denominator: gbpusd" in output
    assert "Rule: EURUSD / GBPUSD ~= EURGBP" in output


def test_groups_command_accepts_config_defaults(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Groups should support the same --config routing as other subcommands."""
    config_path = tmp_path / "histdatacom.yaml"
    config_path.write_text(
        """
histdatacom:
  groups:
    command: show
    group: triangle-eurgbp-eurusd-gbpusd
    json: true
""".lstrip(),
        encoding="utf-8",
    )

    assert groups_cli.main(["--config", str(config_path)]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["group"]["name"] == "triangle-eurgbp-eurusd-gbpusd"
    assert payload["group"]["relationship"]["rule"] == (
        "EURUSD / GBPUSD ~= EURGBP"
    )


def test_histdatacom_main_dispatches_groups_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The top-level histdatacom command should route group discovery."""
    captured: dict[str, tuple[str, ...]] = {}

    def fake_groups_main(argv: list[str]) -> int:
        captured["argv"] = tuple(argv)
        return 0

    monkeypatch.setattr(groups_cli, "main", fake_groups_main)
    monkeypatch.setattr(
        sys,
        "argv",
        ["histdatacom", "groups", "list", "--triangles", "--json"],
    )

    assert histdata_com.main() == 0
    assert captured["argv"] == ("list", "--triangles", "--json")
