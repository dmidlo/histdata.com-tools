"""Pytest unit tests for histdatacom.scraper.repo."""

from __future__ import annotations

import os
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from histdatacom.records import Record
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
    tmp_path: Path,
) -> None:
    """The -A API path should keep returning filtered repo metadata."""
    args = _repo_args(tmp_path, pairs={"eurusd"})
    repo = Repo(args)
    repo.repo_data = {
        "eurusd": {"start": "200005", "end": "202212"},
        "gbpusd": {"start": "200005", "end": "202212"},
        "hash": "hash",
        "hash_utc": 1.0,
    }
    repo.repo_file_exists = True

    result = repo.get_available_repo_data()

    assert result == {"eurusd": {"start": "200005", "end": "202212"}}
    assert repo.filter_pairs is None


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
    args = _repo_args(tmp_path)
    monkeypatch.setattr(
        repo_module,
        "fetch_repository_data_from_url",
        lambda url: remote_repo,
    )

    repo = Repo(args)
    repo.update_repo_from_github()

    assert repo.repo_data["eurusd"] == {
        "start": "200005",
        "end": "202212",
    }
    assert repo.repo_file_exists is True
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
    monkeypatch.setattr(
        Repo,
        "_validate_repository_coverage",
        lambda self, args: calls.append("validate"),
    )

    repo = Repo(args)
    repo.repo_data = {"eurusd": {"start": "200005", "end": "202212"}}
    repo.repo_file_exists = True
    result = repo.get_available_repo_data()

    assert calls == ["validate"]
    assert result == {"eurusd": {"start": "200005", "end": "202212"}}
    assert repo.filter_pairs == {"eurusd"}
    assert (tmp_path / ".repo").exists()


def test_repo_instances_do_not_share_repository_state(tmp_path: Path) -> None:
    """Repository helpers should not share pair ranges through globals."""
    first = Repo(_repo_args(tmp_path / "first", pairs={"eurusd"}))
    second = Repo(_repo_args(tmp_path / "second", pairs={"gbpusd"}))

    first.set_repo_datum(Record(data_fxpair="EURUSD", data_datemonth="202201"))
    second.set_repo_datum(Record(data_fxpair="GBPUSD", data_datemonth="202203"))

    assert first.repo_data == {"eurusd": {"start": "202201", "end": "202201"}}
    assert second.repo_data == {"gbpusd": {"start": "202203", "end": "202203"}}


def test_repo_reloads_data_when_runtime_repo_path_changes(
    tmp_path: Path,
) -> None:
    """A reused repo helper should not carry metadata across repo paths."""
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first_dir.mkdir()
    second_dir.mkdir()
    (first_dir / ".repo").write_text(
        json.dumps({"eurusd": {"start": "200005", "end": "202606"}}),
        encoding="UTF-8",
    )
    (second_dir / ".repo").write_text(
        json.dumps({"gbpusd": {"start": "200005", "end": "202606"}}),
        encoding="UTF-8",
    )

    repo = Repo(_repo_args(first_dir, pairs={"eurusd"}))
    first_result = repo.get_available_repo_data()
    second_result = repo.get_available_repo_data(
        _repo_args(second_dir, pairs={"gbpusd"})
    )

    assert first_result == {"eurusd": {"start": "200005", "end": "202606"}}
    assert second_result == {"gbpusd": {"start": "200005", "end": "202606"}}
    assert repo.repo_data == {"gbpusd": {"start": "200005", "end": "202606"}}


def test_repo_instances_are_isolated_when_used_concurrently(
    tmp_path: Path,
) -> None:
    """Concurrent helper instances should keep independent repo data."""

    def build_repo(pair: str, datemonth: str) -> dict:
        repo = Repo(_repo_args(tmp_path / pair, pairs={pair}))
        repo.set_repo_datum(
            Record(data_fxpair=pair.upper(), data_datemonth=datemonth)
        )
        return repo.repo_data

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = executor.map(
            lambda item: build_repo(*item),
            (("eurusd", "202201"), ("gbpusd", "202203")),
        )

    assert first == {"eurusd": {"start": "202201", "end": "202201"}}
    assert second == {"gbpusd": {"start": "202203", "end": "202203"}}
