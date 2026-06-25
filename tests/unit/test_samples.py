"""Tests for executable end-user samples."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import nbformat
from nbclient import NotebookClient

from samples import api_quickstart
from samples._testing import sample_polars_frame

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SAMPLES = PROJECT_ROOT / "samples"


def test_api_quickstart_script_options_are_copyable() -> None:
    """Script sample builders should expose the documented API paths."""
    extract_options = api_quickstart.build_extract_options()
    polars_options = api_quickstart.build_polars_options()

    assert extract_options.extract_csvs is True
    assert extract_options.api_return_type is None
    assert extract_options.pairs == {"eurusd"}
    assert polars_options.api_return_type == "polars"
    assert polars_options.formats == {"ascii"}
    assert polars_options.timeframes == {"1-minute-bar-quotes"}


def test_api_quickstart_script_calls_runner() -> None:
    """Script sample functions should call histdatacom with built Options."""
    captured: dict[str, Any] = {}

    def fake_runner(options: object) -> object:
        captured["options"] = options
        return sample_polars_frame()

    result = api_quickstart.load_polars_frame(fake_runner)

    assert result.shape == (2, 6)
    assert captured["options"].api_return_type == "polars"


def test_api_quickstart_notebook_executes_without_live_services(
    monkeypatch: Any,
) -> None:
    """Notebook API sample should execute without network or Temporal in tests."""
    monkeypatch.setenv("HISTDATACOM_SAMPLE_MODE", "hermetic")
    notebook_path = SAMPLES / "notebooks" / "api_quickstart.ipynb"
    with notebook_path.open(encoding="utf-8") as handle:
        notebook = nbformat.read(handle, as_version=4)

    client = NotebookClient(
        notebook,
        timeout=30,
        kernel_name="python3",
        resources={"metadata": {"path": str(PROJECT_ROOT)}},
    )

    client.execute()
