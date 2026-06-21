"""Pytest unit tests for histdatacom.scraper.repo."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from histdatacom import config
from histdatacom.scraper.repo import Repo


def test_repo() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def _repo_args(tmp_path: Path, *, pairs: set[str] | None = None) -> dict:
    """Return minimal legacy config args for repository tests."""
    return {
        "default_download_dir": f"{tmp_path}{os.sep}",
        "pairs": pairs or set(),
        "by": "pair_asc",
        "available_remote_data": True,
        "update_remote_data": False,
        "from_api": True,
    }


def test_get_available_repo_data_filters_api_result_without_queue(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The -A API path should keep returning filtered repo metadata."""
    monkeypatch.setattr(config, "ARGS", _repo_args(tmp_path, pairs={"eurusd"}))
    monkeypatch.setattr(
        config,
        "REPO_DATA",
        {
            "eurusd": {"start": "200005", "end": "202212"},
            "gbpusd": {"start": "200005", "end": "202212"},
            "hash": "hash",
            "hash_utc": 1.0,
        },
    )
    monkeypatch.setattr(config, "REPO_DATA_FILE_EXISTS", True)
    monkeypatch.setattr(config, "FILTER_PAIRS", None)

    result = Repo().get_available_repo_data()

    assert result == {"eurusd": {"start": "200005", "end": "202212"}}
    assert config.FILTER_PAIRS is None


def test_update_repo_from_github_uses_explicit_refresh_stage(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Remote repo refresh should write local metadata via stage helpers."""
    import histdatacom.scraper.repo as repo_module

    remote_repo = {
        "eurusd": {"start": "200005", "end": "202212"},
        "hash": "remote",
        "hash_utc": 10.0,
    }
    monkeypatch.setattr(config, "ARGS", _repo_args(tmp_path))
    monkeypatch.setattr(config, "REPO_DATA", {})
    monkeypatch.setattr(config, "REPO_DATA_FILE_EXISTS", False)
    monkeypatch.setattr(
        repo_module,
        "fetch_repository_data_from_url",
        lambda url: remote_repo,
    )

    Repo().update_repo_from_github()

    assert config.REPO_DATA["eurusd"] == {
        "start": "200005",
        "end": "202212",
    }
    assert config.REPO_DATA_FILE_EXISTS is True
    assert (tmp_path / ".repo").exists()


def test_update_remote_repo_data_preserves_api_return_shape(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The -U API path should still validate, write, and return repo data."""
    calls: list[str] = []

    args = _repo_args(tmp_path, pairs={"eurusd"})
    args["available_remote_data"] = False
    args["update_remote_data"] = True
    monkeypatch.setattr(config, "ARGS", args)
    monkeypatch.setattr(
        config,
        "REPO_DATA",
        {"eurusd": {"start": "200005", "end": "202212"}},
    )
    monkeypatch.setattr(config, "REPO_DATA_FILE_EXISTS", True)
    monkeypatch.setattr(config, "FILTER_PAIRS", None)
    monkeypatch.setattr(
        Repo,
        "_validate_repository_coverage",
        lambda self: calls.append("validate"),
    )

    result = Repo().get_available_repo_data()

    assert calls == ["validate"]
    assert result == {"eurusd": {"start": "200005", "end": "202212"}}
    assert (tmp_path / ".repo").exists()
