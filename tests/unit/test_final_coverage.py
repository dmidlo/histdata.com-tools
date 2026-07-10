"""Tests for the receipt-backed final coverage gate."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from types import ModuleType
from typing import Sequence

import pytest


def _module() -> ModuleType:
    script_path = (
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "run_final_coverage.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_final_coverage",
        script_path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FakeCoverageRunner:
    """Record coverage commands with optional failure or content mutation."""

    def __init__(
        self,
        *,
        fail_at: int = 0,
        mutate_at: int = 0,
    ) -> None:
        self.fail_at = fail_at
        self.mutate_at = mutate_at
        self.calls: list[tuple[str, ...]] = []

    def __call__(
        self,
        command: Sequence[str],
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        args = tuple(command)
        self.calls.append(args)
        call_number = len(self.calls)
        if self.mutate_at == call_number:
            (cwd / "tracked.py").write_text("changed\n", encoding="utf-8")
        return subprocess.CompletedProcess(
            args,
            1 if self.fail_at == call_number else 0,
            stdout="",
            stderr="coverage failed\n" if self.fail_at == call_number else "",
        )


def _paths(_: Path) -> tuple[str, ...]:
    return ("tracked.py",)


def _versions(version: str = "1") -> dict[str, str]:
    return {
        "python": f"3.14.{version}",
        "pytest": f"9.1.{version}",
        "coverage": f"7.13.{version}",
    }


def test_matching_receipt_reuses_one_successful_coverage_execution(
    tmp_path: Path,
) -> None:
    """Unchanged repository content should never rerun full coverage."""
    module = _module()
    (tmp_path / "tracked.py").write_text("stable\n", encoding="utf-8")
    runner = FakeCoverageRunner()

    first = module.ensure_final_coverage(
        tmp_path,
        runner=runner,
        path_provider=_paths,
        versions=_versions(),
    )
    second = module.ensure_final_coverage(
        tmp_path,
        runner=runner,
        path_provider=_paths,
        versions=_versions(),
    )

    assert first["decision"] == "executed"
    assert second["decision"] == "reused"
    assert first["evidence_key"] == second["evidence_key"]
    assert len(runner.calls) == 4
    assert [call[2:] for call in runner.calls] == [
        ("coverage", "erase"),
        ("coverage", "run"),
        ("coverage", "combine"),
        ("coverage", "report"),
    ]
    receipt = json.loads(
        (tmp_path / module.RECEIPT_PATH).read_text(encoding="utf-8")
    )
    assert receipt["state"] == "pass"
    assert receipt["evidence_key"] == first["evidence_key"]


def test_content_or_runtime_change_invalidates_coverage_receipt(
    tmp_path: Path,
) -> None:
    """Repository bytes and relevant tool versions belong to the cache key."""
    module = _module()
    tracked = tmp_path / "tracked.py"
    tracked.write_text("one\n", encoding="utf-8")
    runner = FakeCoverageRunner()

    first = module.ensure_final_coverage(
        tmp_path,
        runner=runner,
        path_provider=_paths,
        versions=_versions("1"),
    )
    tracked.write_text("two\n", encoding="utf-8")
    second = module.ensure_final_coverage(
        tmp_path,
        runner=runner,
        path_provider=_paths,
        versions=_versions("1"),
    )
    third = module.ensure_final_coverage(
        tmp_path,
        runner=runner,
        path_provider=_paths,
        versions=_versions("2"),
    )

    assert {first["decision"], second["decision"], third["decision"]} == {
        "executed"
    }
    assert (
        len(
            {
                first["evidence_key"],
                second["evidence_key"],
                third["evidence_key"],
            }
        )
        == 3
    )
    assert len(runner.calls) == 12


def test_failed_coverage_does_not_leave_reusable_evidence(
    tmp_path: Path,
) -> None:
    """Partial or failed coverage must never produce a passing receipt."""
    module = _module()
    (tmp_path / "tracked.py").write_text("stable\n", encoding="utf-8")
    (tmp_path / ".coverage").write_text("partial\n", encoding="utf-8")
    (tmp_path / ".coverage.worker").write_text("partial\n", encoding="utf-8")
    (tmp_path / ".coveragerc").write_text("[run]\n", encoding="utf-8")
    runner = FakeCoverageRunner(fail_at=3)

    with pytest.raises(module.FinalCoverageError) as error:
        module.ensure_final_coverage(
            tmp_path,
            runner=runner,
            path_provider=_paths,
            versions=_versions(),
        )

    assert error.value.code == "coverage-command-failed"
    assert not (tmp_path / module.RECEIPT_PATH).exists()
    assert not (tmp_path / ".coverage").exists()
    assert not (tmp_path / ".coverage.worker").exists()
    assert (tmp_path / ".coveragerc").exists()


def test_repository_mutation_during_coverage_rejects_receipt(
    tmp_path: Path,
) -> None:
    """Coverage evidence is valid only for unchanged repository content."""
    module = _module()
    (tmp_path / "tracked.py").write_text("before\n", encoding="utf-8")
    runner = FakeCoverageRunner(mutate_at=2)

    with pytest.raises(module.FinalCoverageError) as error:
        module.ensure_final_coverage(
            tmp_path,
            runner=runner,
            path_provider=_paths,
            versions=_versions(),
        )

    assert error.value.code == "repository-content-changed"
    assert not (tmp_path / module.RECEIPT_PATH).exists()


def test_active_lock_prevents_concurrent_coverage_execution(
    tmp_path: Path,
) -> None:
    """A second gate should stop instead of launching another full suite."""
    module = _module()
    (tmp_path / "tracked.py").write_text("stable\n", encoding="utf-8")
    lock_path = tmp_path / module.LOCK_PATH
    lock_path.parent.mkdir(parents=True)
    lock_path.write_text("pid=123\n", encoding="utf-8")
    runner = FakeCoverageRunner()

    with pytest.raises(module.FinalCoverageError) as error:
        module.ensure_final_coverage(
            tmp_path,
            runner=runner,
            path_provider=_paths,
            versions=_versions(),
            lock_timeout_seconds=0,
        )

    assert error.value.code == "coverage-lock-timeout"
    assert runner.calls == []


def test_old_lock_is_not_stale_while_its_owner_is_active(
    tmp_path: Path,
) -> None:
    """Long coverage runs retain the lock while their process is alive."""
    module = _module()
    lock_path = tmp_path / module.LOCK_PATH
    lock_path.parent.mkdir(parents=True)
    lock_path.write_text(f"pid={os.getpid()}\n", encoding="utf-8")
    old_time = time.time() - module.STALE_LOCK_SECONDS - 1
    os.utime(lock_path, (old_time, old_time))

    assert module._lock_is_stale(lock_path) is False
