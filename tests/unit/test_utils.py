"""Pytest unit tests for histdatacom.utils.py."""

import os
from pathlib import Path

import pytest

from histdatacom.utils import (
    SUPPORTED_API_RETURN_TYPES,
    check_installed_module,
    load_influx_yaml,
    normalize_api_return_type,
    set_working_data_dir,
)


def test_utils() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


@pytest.mark.parametrize(
    ("return_type", "expected"),
    (
        ("polars", "polars"),
        ("POLARS", "polars"),
        ("pandas", "pandas"),
        ("arrow", "arrow"),
        ("pyarrow", "arrow"),
        (None, None),
        ("", None),
    ),
)
def test_normalize_api_return_type(
    return_type: str | None, expected: str | None
) -> None:
    """Normalize public API return type names and aliases."""
    assert normalize_api_return_type(return_type) == expected


@pytest.mark.parametrize("return_type", ("numpy", "sqlite"))
def test_normalize_api_return_type_rejects_unsupported_values(
    return_type: str,
) -> None:
    """Unsupported API return values should fail before module import checks."""
    with pytest.raises(ValueError) as err:
        normalize_api_return_type(return_type)

    assert f"unsupported api_return_type '{return_type}'" in str(err.value)
    assert "arrow, pandas, polars" in str(err.value)


def test_api_return_type_contract_is_explicit() -> None:
    """Keep the public return-type contract visible."""
    assert SUPPORTED_API_RETURN_TYPES == {
        "arrow",
        "pandas",
        "polars",
    }


def test_check_installed_module_accepts_polars_return_type() -> None:
    """Polars is now the default dataframe dependency."""
    assert check_installed_module("polars")


def test_set_working_data_dir_expands_relative_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Relative data directories resolve under the current working directory."""
    monkeypatch.chdir(tmp_path)

    assert set_working_data_dir("data/live") == (
        f"{tmp_path}{os.sep}data{os.sep}live{os.sep}"
    )


def test_set_working_data_dir_preserves_absolute_paths(
    tmp_path: Path,
) -> None:
    """Absolute data directories should not be relocated under cwd."""
    data_dir = tmp_path / "histdatacom-live"

    assert set_working_data_dir(str(data_dir)) == f"{data_dir}{os.sep}"


def test_load_influx_yaml_missing_config_exits_nonzero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Missing Influx config should be a failed CLI precondition."""
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit) as err:
        load_influx_yaml()

    assert err.value.code == 1


def test_influx_yaml_reads_safe_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Influx config should parse through the safe YAML loader.

    Args:
        tmp_path (Path): temporary test directory.
        monkeypatch (pytest.MonkeyPatch): pytest monkeypatch fixture.
    """
    monkeypatch.chdir(tmp_path)
    config_token = tmp_path.name
    Path("influxdb.yaml").write_text(
        "\n".join(
            (
                "influxdb:",
                "  org: histdata",
                "  bucket: forex",
                "  url: http://localhost:8086",
                f"  token: {config_token}",
            ),
        ),
        encoding="UTF-8",
    )

    loaded_yaml = load_influx_yaml()  # act

    assert loaded_yaml == {
        "influxdb": {
            "org": "histdata",
            "bucket": "forex",
            "url": "http://localhost:8086",
            "token": config_token,
        },
    }


def test_influx_yaml_blocks_python_tags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unsafe PyYAML object tags should not execute or deserialize.

    Args:
        tmp_path (Path): temporary test directory.
        monkeypatch (pytest.MonkeyPatch): pytest monkeypatch fixture.
    """
    monkeypatch.chdir(tmp_path)
    marker_path = tmp_path / "unsafe-yaml-executed"
    Path("influxdb.yaml").write_text(
        "\n".join(
            (
                "!!python/object/apply:os.system",
                f'- "touch {marker_path}"',
            ),
        ),
        encoding="UTF-8",
    )

    with pytest.raises(SystemExit):
        load_influx_yaml()  # act

    assert not marker_path.exists()
