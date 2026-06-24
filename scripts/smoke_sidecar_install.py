"""Smoke-test an installed histdatacom package and its sidecar resources."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
from importlib import metadata
from pathlib import Path
from typing import Any, Mapping, Sequence

EXPECTED_ASSETS = (
    "README.md",
    "manifest.json",
    "runtime-defaults.json",
    "temporal-runtime-index.json",
    "third-party/temporal-cli/LICENSE",
    "third-party/temporal-cli/NOTICE.md",
)
EXPECTED_CONSOLE_SCRIPTS = {
    "histdatacom": "histdatacom.histdata_com:main",
    "histdatacom-sidecar": "histdatacom.sidecar.cli:main",
    "histdatacom-sidecar-worker": "histdatacom.sidecar.worker:main",
}
QUALITY_REPORT_SCHEMA_VERSION = "histdatacom.quality-report.v1"
QUALITY_SMOKE_CLEAN_ROWS = (
    "20120201 000000;1.306600;1.306600;1.306560;1.306560;0",
    "20120201 000100;1.306570;1.306570;1.306470;1.306560;17",
    "20120201 000200;1.306520;1.306560;1.306520;1.306560;2147483647",
)
QUALITY_SMOKE_DIRTY_ROWS = (
    QUALITY_SMOKE_CLEAN_ROWS[0],
    "20120201 000100;$1.306570;1.306570;1.306470;1.306560;17",
)


def _single_wheel(wheel_dir: Path) -> Path:
    """Return the only built histdatacom wheel in a directory."""
    wheels = sorted(wheel_dir.glob("histdatacom-*.whl"))
    if len(wheels) != 1:
        raise SystemExit(f"expected exactly one wheel, found {wheels}")
    return wheels[0]


def _script_path(name: str) -> str:
    """Return an installed console script path."""
    script_path = shutil.which(name)
    if script_path is None:
        raise SystemExit(f"console script is not on PATH: {name}")
    return script_path


def _run(
    command: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
    expected_returncodes: tuple[int, ...] = (0,),
) -> subprocess.CompletedProcess[str]:
    """Run a smoke command and fail with useful output when it breaks."""
    completed = subprocess.run(
        command,
        capture_output=True,
        check=False,
        env=(dict(env) if env is not None else None),
        text=True,
    )
    if completed.returncode not in expected_returncodes:
        expected = ", ".join(str(code) for code in expected_returncodes)
        raise SystemExit(
            f"command returned exit {completed.returncode}; expected "
            f"{expected}: "
            f"{' '.join(command)}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return completed


def _run_json(
    command: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Run a command that emits JSON and return the decoded payload."""
    completed = _run(command, env=env)
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as err:
        raise SystemExit(
            f"command did not emit valid JSON: {' '.join(command)}\n"
            f"stdout:\n{completed.stdout}"
        ) from err
    if not isinstance(payload, dict):
        raise SystemExit(f"command emitted non-object JSON: {' '.join(command)}")
    return payload


def install_wheel(
    *,
    wheel_dir: Path | None = None,
    wheel_path: Path | None = None,
    install_temporal_extra: bool = False,
) -> Path:
    """Install the built wheel into the active Python environment."""
    resolved_wheel = wheel_path or _single_wheel(wheel_dir or Path("dist"))
    install_target = str(resolved_wheel)
    if install_temporal_extra:
        install_target = (
            "histdatacom[temporal] @ " f"{resolved_wheel.resolve().as_uri()}"
        )
    subprocess.check_call([sys.executable, "-m", "pip", "install", install_target])
    return resolved_wheel


def check_package_metadata(*, expect_temporal_extra: bool) -> dict[str, Any]:
    """Validate installed package metadata and console entry points."""
    import histdatacom
    from histdatacom.runtime_contracts import RunRequest as RuntimeRunRequest
    from histdatacom.sidecar.contracts import RunRequest

    dist = metadata.distribution("histdatacom")
    scripts = {
        entry.name: entry.value
        for entry in metadata.entry_points().select(group="console_scripts")
    }
    for script_name, expected_target in EXPECTED_CONSOLE_SCRIPTS.items():
        actual_target = scripts.get(script_name)
        if actual_target != expected_target:
            raise SystemExit(
                f"{script_name} entry point expected {expected_target!r}, "
                f"found {actual_target!r}"
            )
        _script_path(script_name)

    installed_version = metadata.version("histdatacom")
    if installed_version != histdatacom.__version__:
        raise SystemExit(
            "installed package metadata version does not match imported "
            f"package: {installed_version!r} != {histdatacom.__version__!r}"
        )
    if dist.metadata["Name"] != "histdatacom":
        raise SystemExit(f"unexpected installed name: {dist.metadata['Name']}")
    provides_extra = set(dist.metadata.get_all("Provides-Extra", []))
    if expect_temporal_extra and "temporal" not in provides_extra:
        raise SystemExit("temporal compatibility extra missing from metadata")

    temporalio_version = metadata.version("temporalio")
    if importlib.util.find_spec("temporalio") is None:
        raise SystemExit("temporalio distribution is installed but missing")
    if RunRequest is not RuntimeRunRequest:
        raise SystemExit("sidecar contract RunRequest does not match runtime contract")

    return {
        "name": dist.metadata["Name"],
        "version": installed_version,
        "console_scripts": sorted(EXPECTED_CONSOLE_SCRIPTS),
        "sidecar_contracts": ["RunRequest"],
        "temporalio_version": temporalio_version,
    }


def check_sidecar_resources(
    *,
    require_bundled_current_platform: bool = False,
    check_executable_version: bool = False,
) -> dict[str, Any]:
    """Validate installed sidecar resources for the current platform."""
    from histdatacom.sidecar import (
        SidecarExecutableUnavailable,
        current_platform_key,
        inspect_temporal_runtime_cache,
        load_sidecar_manifest,
        load_temporal_runtime_index,
        sidecar_asset,
        sidecar_executable_path,
        temporal_runtime_executable_path,
    )

    manifest = load_sidecar_manifest()
    runtime_index = load_temporal_runtime_index(manifest)
    for asset in EXPECTED_ASSETS:
        if not sidecar_asset(asset).is_file():
            raise SystemExit(f"sidecar asset is not a file: {asset}")

    platform_key = current_platform_key()
    platform_resource = manifest.platforms.get(platform_key)
    platform_artifact = runtime_index.platforms.get(platform_key)
    if platform_resource is None:
        supported = ", ".join(sorted(manifest.platforms))
        raise SystemExit(
            f"current platform {platform_key!r} is not declared in sidecar "
            f"manifest. Supported platforms: {supported}"
        )
    executable_version = ""
    resolver_source = ""
    if platform_resource.bundled:
        with sidecar_executable_path(platform_key) as executable_path:
            if not executable_path.is_file():
                raise SystemExit(
                    f"bundled sidecar executable is missing: {executable_path}"
                )
            if check_executable_version:
                completed = _run([str(executable_path), "--version"])
                executable_version = (
                    completed.stdout.strip() or completed.stderr.strip()
                )
                resolver_source = "packaged"
    else:
        if require_bundled_current_platform:
            raise SystemExit(
                f"current platform {platform_key!r} is not bundled in this wheel"
            )
        try:
            with sidecar_executable_path(platform_key):
                raise SystemExit("metadata-only sidecar resource exposed an executable")
        except SidecarExecutableUnavailable as err:
            if "not bundled in this distribution" not in str(err):
                raise
        if check_executable_version:
            with temporal_runtime_executable_path() as resolution:
                completed = _run([str(resolution.executable), "--version"])
                executable_version = (
                    completed.stdout.strip() or completed.stderr.strip()
                )
                resolver_source = resolution.source

    return {
        "sidecar": manifest.sidecar,
        "distribution_strategy": manifest.distribution_strategy,
        "runtime_index_version": runtime_index.version,
        "embedded_binary": manifest.embedded_binary,
        "platform": platform_key,
        "platform_bundled": platform_resource.bundled,
        "platform_artifact": (
            {
                "archive_name": platform_artifact.archive_name,
                "archive_sha256": platform_artifact.archive_sha256,
                "archive_size_bytes": platform_artifact.archive_size_bytes,
            }
            if platform_artifact is not None
            else None
        ),
        "runtime_cache_entries": [
            entry.to_dict()
            for entry in inspect_temporal_runtime_cache()
            if entry.platform_key == platform_key
        ],
        "resolver_source": resolver_source,
        "executable_version": executable_version,
    }


def check_cli_smoke(
    state_dir: Path,
    *,
    require_bundled_current_platform: bool = False,
    start_sidecar: bool = False,
) -> dict[str, Any]:
    """Run offline CLI smoke checks against a temporary sidecar state dir."""
    state_dir.mkdir(parents=True, exist_ok=True)
    _run([_script_path("histdatacom"), "--version"])
    _run([_script_path("histdatacom-sidecar-worker"), "--help"])

    sidecar_script = _script_path("histdatacom-sidecar")
    status = _run_json(
        [
            sidecar_script,
            "--state-dir",
            str(state_dir),
            "--json",
            "status",
        ]
    )
    if status.get("state") not in {"running", "stopped"}:
        raise SystemExit(f"unexpected sidecar status payload: {status}")

    doctor = _run_json(
        [
            sidecar_script,
            "--state-dir",
            str(state_dir),
            "--json",
            "doctor",
        ]
    )
    platform = doctor.get("platform", {})
    if not isinstance(platform, dict) or not platform.get("supported"):
        raise SystemExit(f"unexpected sidecar doctor payload: {doctor}")
    if (
        require_bundled_current_platform
        and platform.get("executable_bundled") is not True
    ):
        raise SystemExit(
            "sidecar doctor did not report a bundled current-platform "
            f"executable: {doctor}"
        )

    start_state = ""
    stop_state = ""
    if start_sidecar:
        start = _run_json(
            [
                sidecar_script,
                "--state-dir",
                str(state_dir),
                "--json",
                "start",
                "--startup-timeout",
                "20",
            ]
        )
        start_state = str(start.get("state", ""))
        if start_state != "running":
            raise SystemExit(f"unexpected sidecar start payload: {start}")
        stop = _run_json(
            [
                sidecar_script,
                "--state-dir",
                str(state_dir),
                "--json",
                "stop",
            ]
        )
        stop_state = str(stop.get("state", ""))
    return {
        "status_state": status["state"],
        "doctor_supported": platform["supported"],
        "doctor_executable_bundled": platform.get("executable_bundled"),
        "start_state": start_state,
        "stop_state": stop_state,
    }


def check_live_sidecar_smoke(
    *,
    workspace: Path,
    runtime_home: Path,
    data_directory: Path,
    temporal_executable: Path | None = None,
    startup_timeout: float,
    completion_timeout: float,
    stop_timeout: float,
) -> dict[str, Any]:
    """Run an external HistData.com sidecar smoke."""
    from histdatacom.sidecar.live_smoke import (
        LiveSidecarSmokeError,
        diagnostics_json,
        run_live_sidecar_smoke,
    )

    try:
        return dict(
            run_live_sidecar_smoke(
                workspace=workspace,
                runtime_home=runtime_home,
                data_directory=data_directory,
                temporal_executable=temporal_executable,
                startup_timeout=startup_timeout,
                completion_timeout=completion_timeout,
                stop_timeout=stop_timeout,
            ).to_dict()
        )
    except LiveSidecarSmokeError as err:
        raise SystemExit(
            "live sidecar smoke failed with diagnostics:\n"
            f"{diagnostics_json(err.diagnostics)}"
        ) from err


def check_hermetic_sidecar_smoke(
    *,
    workspace: Path,
    runtime_home: Path,
    data_directory: Path,
    temporal_executable: Path | None = None,
    startup_timeout: float,
    completion_timeout: float,
    stop_timeout: float,
) -> dict[str, Any]:
    """Run a local-only Temporal sidecar smoke for installed wheels."""
    from histdatacom.sidecar.live_smoke import (
        LiveSidecarSmokeError,
        diagnostics_json,
        run_hermetic_sidecar_smoke,
    )

    try:
        return dict(
            run_hermetic_sidecar_smoke(
                workspace=workspace,
                runtime_home=runtime_home,
                data_directory=data_directory,
                temporal_executable=temporal_executable,
                startup_timeout=startup_timeout,
                completion_timeout=completion_timeout,
                stop_timeout=stop_timeout,
            ).to_dict()
        )
    except LiveSidecarSmokeError as err:
        raise SystemExit(
            "hermetic sidecar smoke failed with diagnostics:\n"
            f"{diagnostics_json(err.diagnostics)}"
        ) from err


def check_default_routing_sidecar_smoke(
    *,
    workspace: Path,
    runtime_home: Path,
    data_directory: Path,
    temporal_executable: Path | None = None,
    startup_timeout: float,
    completion_timeout: float,
    stop_timeout: float,
) -> dict[str, Any]:
    """Run local-only smoke through default client routing."""
    from histdatacom.sidecar.live_smoke import (
        LiveSidecarSmokeError,
        diagnostics_json,
        run_default_client_routing_sidecar_smoke,
    )

    try:
        return dict(
            run_default_client_routing_sidecar_smoke(
                workspace=workspace,
                runtime_home=runtime_home,
                data_directory=data_directory,
                temporal_executable=temporal_executable,
                startup_timeout=startup_timeout,
                completion_timeout=completion_timeout,
                stop_timeout=stop_timeout,
            ).to_dict()
        )
    except LiveSidecarSmokeError as err:
        raise SystemExit(
            "default-routing sidecar smoke failed with diagnostics:\n"
            f"{diagnostics_json(err.diagnostics)}"
        ) from err


def check_quality_sidecar_smoke(
    *,
    workspace: Path,
    runtime_home: Path,
    data_directory: Path,
    temporal_executable: Path | None = None,
    startup_timeout: float,
    stop_timeout: float,
) -> dict[str, Any]:
    """Run installed CLI quality checks through the packaged sidecar."""
    smoke_env = _quality_sidecar_env(
        workspace=workspace,
        runtime_home=runtime_home,
    )
    fixtures = _write_quality_smoke_fixtures(data_directory)
    report_dir = data_directory / "quality-smoke-reports"
    clean_report = report_dir / "quality-clean.json"
    dirty_report = report_dir / "quality-dirty.json"
    start_payload: dict[str, Any] | None = None
    stop_payload: dict[str, Any] | None = None
    try:
        start_payload = _start_quality_sidecar(
            workspace=workspace,
            runtime_home=runtime_home,
            temporal_executable=temporal_executable,
            startup_timeout=startup_timeout,
            env=smoke_env,
        )
        clean = _run_quality_cli(
            target=fixtures["clean"],
            report=clean_report,
            data_directory=data_directory,
            env=smoke_env,
            expected_returncodes=(0,),
        )
        dirty = _run_quality_cli(
            target=fixtures["dirty"],
            report=dirty_report,
            data_directory=data_directory,
            env=smoke_env,
            expected_returncodes=(1,),
        )
        clean_payload = _validate_quality_report(
            clean_report,
            expected_status="clean",
            min_errors=0,
            max_errors=0,
        )
        dirty_payload = _validate_quality_report(
            dirty_report,
            expected_status="failed",
            min_errors=1,
            max_errors=None,
        )
        jobs_payload = _quality_jobs_payload(
            workspace=workspace,
            runtime_home=runtime_home,
            env=smoke_env,
        )
        jobs = _validate_quality_sidecar_jobs(
            jobs_payload,
            clean_target=fixtures["clean"],
            clean_report=clean_report,
            dirty_target=fixtures["dirty"],
            dirty_report=dirty_report,
        )
    finally:
        stop_payload = _stop_quality_sidecar(
            workspace=workspace,
            runtime_home=runtime_home,
            stop_timeout=stop_timeout,
            env=smoke_env,
        )
        _validate_quality_sidecar_stop(stop_payload)

    return {
        "start_state": str((start_payload or {}).get("state", "")),
        "stop_state": str((stop_payload or {}).get("state", "")),
        "clean": _quality_smoke_case_result(
            completed=clean,
            report_path=clean_report,
            payload=clean_payload,
        ),
        "dirty": _quality_smoke_case_result(
            completed=dirty,
            report_path=dirty_report,
            payload=dirty_payload,
        ),
        "jobs": jobs,
    }


def _quality_sidecar_env(
    *,
    workspace: Path,
    runtime_home: Path,
) -> dict[str, str]:
    env = dict(os.environ)
    env["HISTDATACOM_SIDECAR_WORKSPACE"] = str(workspace)
    env["HISTDATACOM_SIDECAR_HOME"] = str(runtime_home)
    return env


def _write_quality_smoke_fixtures(data_directory: Path) -> dict[str, Path]:
    fixture_dir = data_directory / "quality-smoke-fixtures"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    clean = fixture_dir / "DAT_ASCII_EURUSD_M1_201202.csv"
    dirty = fixture_dir / "DAT_ASCII_EURUSD_M1_201202_BAD_NUMERIC.csv"
    clean.write_text(
        "\n".join(QUALITY_SMOKE_CLEAN_ROWS) + "\n",
        encoding="ascii",
    )
    dirty.write_text(
        "\n".join(QUALITY_SMOKE_DIRTY_ROWS) + "\n",
        encoding="ascii",
    )
    return {"clean": clean, "dirty": dirty}


def _start_quality_sidecar(
    *,
    workspace: Path,
    runtime_home: Path,
    temporal_executable: Path | None,
    startup_timeout: float,
    env: Mapping[str, str],
) -> dict[str, Any]:
    command = [
        _script_path("histdatacom-sidecar"),
        "--workspace",
        str(workspace),
        "--runtime-home",
        str(runtime_home),
        "--json",
        "start",
        "--startup-timeout",
        str(startup_timeout),
    ]
    if temporal_executable is not None:
        command.extend(["--executable", str(temporal_executable)])
    payload = _run_json(command, env=env)
    if payload.get("state") != "running":
        raise SystemExit(f"quality sidecar did not start: {payload}")
    return payload


def _run_quality_cli(
    *,
    target: Path,
    report: Path,
    data_directory: Path,
    env: Mapping[str, str],
    expected_returncodes: tuple[int, ...],
) -> subprocess.CompletedProcess[str]:
    return _run(
        [
            _script_path("histdatacom"),
            "--no-sidecar-start",
            "--data-directory",
            str(data_directory),
            "--quality",
            "--quality-target",
            str(target),
            "--quality-checks",
            "ingestion",
            "--quality-report",
            str(report),
        ],
        env=env,
        expected_returncodes=expected_returncodes,
    )


def _validate_quality_report(
    path: Path,
    *,
    expected_status: str,
    min_errors: int,
    max_errors: int | None,
) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as err:
        raise SystemExit(f"quality report was not written: {path}") from err
    except json.JSONDecodeError as err:
        raise SystemExit(f"quality report is invalid JSON: {path}") from err
    if not isinstance(payload, dict):
        raise SystemExit(f"quality report is not a JSON object: {path}")
    if payload.get("schema_version") != QUALITY_REPORT_SCHEMA_VERSION:
        raise SystemExit(f"quality report has unexpected schema version: {path}")
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        raise SystemExit(f"quality report missing summary: {path}")
    if summary.get("target_count") != 1:
        raise SystemExit(f"quality report expected one target: {path} {summary}")
    if summary.get("status") != expected_status:
        raise SystemExit(
            "quality report had unexpected status: "
            f"{path} expected={expected_status} summary={summary}"
        )
    error_count = int(summary.get("error_count", 0) or 0)
    if error_count < min_errors:
        raise SystemExit(
            f"quality report expected at least {min_errors} errors: "
            f"{path} {summary}"
        )
    if max_errors is not None and error_count > max_errors:
        raise SystemExit(
            f"quality report expected at most {max_errors} errors: " f"{path} {summary}"
        )
    if not payload.get("target_summaries"):
        raise SystemExit(f"quality report missing target summaries: {path}")
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict) or metadata.get("operation") != ("data-quality"):
        raise SystemExit(f"quality report missing operation metadata: {path}")
    return payload


def _quality_jobs_payload(
    *,
    workspace: Path,
    runtime_home: Path,
    env: Mapping[str, str],
) -> dict[str, Any]:
    return _run_json(
        [
            _script_path("histdatacom-sidecar"),
            "--workspace",
            str(workspace),
            "--runtime-home",
            str(runtime_home),
            "--json",
            "jobs",
            "list",
            "--offline",
        ],
        env=env,
    )


def _validate_quality_sidecar_jobs(
    jobs_payload: Mapping[str, Any],
    *,
    clean_target: Path,
    clean_report: Path,
    dirty_target: Path,
    dirty_report: Path,
) -> dict[str, Any]:
    jobs = jobs_payload.get("jobs")
    if not isinstance(jobs, list):
        raise SystemExit(f"sidecar jobs payload missing jobs: {jobs_payload}")
    clean_job = _find_quality_job(jobs, clean_target, clean_report)
    dirty_job = _find_quality_job(jobs, dirty_target, dirty_report)
    _validate_quality_job(clean_job, expected_status="completed")
    _validate_quality_job(dirty_job, expected_status="failed")
    return {
        "count": len(jobs),
        "clean_workflow_id": str(clean_job.get("workflow_id", "")),
        "dirty_workflow_id": str(dirty_job.get("workflow_id", "")),
        "clean_status": _normalized_quality_job_status(clean_job),
        "dirty_status": _normalized_quality_job_status(dirty_job),
    }


def _find_quality_job(
    jobs: Sequence[Any],
    target: Path,
    report: Path,
) -> Mapping[str, Any]:
    expected_target = str(target)
    expected_report = str(report)
    for job in jobs:
        if not isinstance(job, Mapping):
            continue
        metadata = job.get("metadata")
        if not isinstance(metadata, Mapping):
            continue
        request = metadata.get("run_request")
        if not isinstance(request, Mapping):
            continue
        if request.get("data_quality") is not True:
            continue
        if expected_target not in tuple(request.get("quality_paths", ())):
            continue
        if str(request.get("quality_report_path", "")) != expected_report:
            continue
        return job
    raise SystemExit(
        "sidecar jobs did not include quality request for " f"{expected_target}"
    )


def _validate_quality_job(
    job: Mapping[str, Any],
    *,
    expected_status: str,
) -> None:
    if _normalized_quality_job_status(job) != expected_status:
        raise SystemExit(
            "sidecar quality job had unexpected status: "
            f"expected={expected_status} job={job}"
        )
    artifacts = job.get("artifacts")
    if not isinstance(artifacts, list) or not any(
        isinstance(artifact, Mapping) and artifact.get("kind") == "quality-report"
        for artifact in artifacts
    ):
        raise SystemExit(f"sidecar quality job missing quality-report artifact: {job}")


def _normalized_quality_job_status(job: Mapping[str, Any]) -> str:
    return str(job.get("status", "") or "").strip().lower()


def _stop_quality_sidecar(
    *,
    workspace: Path,
    runtime_home: Path,
    stop_timeout: float,
    env: Mapping[str, str],
) -> dict[str, Any]:
    return _run_json(
        [
            _script_path("histdatacom-sidecar"),
            "--workspace",
            str(workspace),
            "--runtime-home",
            str(runtime_home),
            "--json",
            "stop",
            "--stop-timeout",
            str(stop_timeout),
        ],
        env=env,
    )


def _validate_quality_sidecar_stop(payload: Mapping[str, Any]) -> None:
    if payload.get("state") != "stopped":
        raise SystemExit(f"quality sidecar did not stop cleanly: {payload}")
    pids = payload.get("pids")
    if isinstance(pids, Mapping) and pids:
        raise SystemExit(f"quality sidecar stop left running processes: {payload}")


def _quality_smoke_case_result(
    *,
    completed: subprocess.CompletedProcess[str],
    report_path: Path,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    summary = payload.get("summary")
    return {
        "returncode": completed.returncode,
        "report": str(report_path),
        "status": (
            str(summary.get("status", "")) if isinstance(summary, Mapping) else ""
        ),
    }


def main() -> None:
    """Run install-time sidecar smoke checks."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--wheel-dir",
        type=Path,
        help="install the only histdatacom wheel from this directory first",
    )
    parser.add_argument(
        "--wheel",
        type=Path,
        help="install this exact histdatacom wheel first",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        help="state directory used for offline sidecar CLI checks",
    )
    parser.add_argument(
        "--expect-temporal-extra",
        action="store_true",
        help="require the temporalio optional dependency to be installed",
    )
    parser.add_argument(
        "--skip-cli",
        action="store_true",
        help="skip console command execution and validate import metadata only",
    )
    parser.add_argument(
        "--require-bundled-current-platform",
        action="store_true",
        help="require the installed wheel to bundle this platform executable",
    )
    parser.add_argument(
        "--check-executable-version",
        action="store_true",
        help="run the packaged Temporal executable with --version",
    )
    parser.add_argument(
        "--start-sidecar",
        action="store_true",
        help="start the sidecar without --executable and then stop it",
    )
    parser.add_argument(
        "--live-sidecar-smoke",
        action="store_true",
        help=(
            "external HistData.com operator-gated smoke that starts Temporal "
            "workers, submits a URL-validation job, and validates "
            "status/artifacts"
        ),
    )
    parser.add_argument(
        "--hermetic-sidecar-smoke",
        action="store_true",
        help=(
            "deterministic installed-wheel smoke that starts Temporal workers, "
            "submits a local-only dataset-planning job, and validates "
            "status/artifacts"
        ),
    )
    parser.add_argument(
        "--default-routing-sidecar-smoke",
        action="store_true",
        help=(
            "deterministic installed-wheel smoke that starts Temporal with "
            "non-default worker routing, submits without an explicit worker "
            "config, and validates default client resolver routing"
        ),
    )
    parser.add_argument(
        "--quality-sidecar-smoke",
        action="store_true",
        help=(
            "deterministic installed-wheel smoke that runs clean and dirty "
            "histdatacom --quality commands through the local sidecar"
        ),
    )
    parser.add_argument(
        "--temporal-executable",
        type=Path,
        help=(
            "Temporal executable for live sidecar smokes; defaults to "
            "HISTDATACOM_TEMPORAL_EXECUTABLE or the packaged executable"
        ),
    )
    parser.add_argument(
        "--live-workspace",
        type=Path,
        help="workspace path used for live sidecar smoke runtime scoping",
    )
    parser.add_argument(
        "--live-runtime-home",
        type=Path,
        help="runtime home used for live sidecar smoke state/logs/SQLite",
    )
    parser.add_argument(
        "--live-data-dir",
        type=Path,
        help="HistData data directory used by the live sidecar smoke job",
    )
    parser.add_argument(
        "--live-startup-timeout",
        type=float,
        default=30.0,
        help="seconds to wait for the live Temporal frontend to start",
    )
    parser.add_argument(
        "--live-completion-timeout",
        type=float,
        default=180.0,
        help="seconds to wait for the live smoke job to complete",
    )
    parser.add_argument(
        "--live-stop-timeout",
        type=float,
        default=30.0,
        help="seconds to wait for live sidecar processes to stop",
    )
    args = parser.parse_args()
    if args.wheel is not None and args.wheel_dir is not None:
        parser.error("--wheel and --wheel-dir are mutually exclusive")

    wheel_name = ""
    if args.wheel_dir is not None or args.wheel is not None:
        wheel_name = install_wheel(
            wheel_dir=args.wheel_dir,
            wheel_path=args.wheel,
            install_temporal_extra=args.expect_temporal_extra,
        ).name

    with tempfile.TemporaryDirectory() as temporary_dir:
        state_dir = args.state_dir or Path(temporary_dir) / "sidecar-state"
        report = {
            "wheel": wheel_name,
            "package": check_package_metadata(
                expect_temporal_extra=args.expect_temporal_extra
            ),
            "sidecar": check_sidecar_resources(
                require_bundled_current_platform=(
                    args.require_bundled_current_platform
                ),
                check_executable_version=args.check_executable_version,
            ),
            "cli": None,
            "hermetic_sidecar": None,
            "default_routing_sidecar": None,
            "quality_sidecar": None,
            "live_sidecar": None,
        }
        if not args.skip_cli:
            report["cli"] = check_cli_smoke(
                state_dir,
                require_bundled_current_platform=(
                    args.require_bundled_current_platform
                ),
                start_sidecar=args.start_sidecar,
            )
        if args.hermetic_sidecar_smoke:
            live_workspace = args.live_workspace or Path(temporary_dir) / (
                "live-workspace"
            )
            live_runtime_home = (
                args.live_runtime_home or Path(temporary_dir) / "live-runtime"
            )
            live_data_dir = args.live_data_dir or Path(temporary_dir) / ("live-data")
            report["hermetic_sidecar"] = check_hermetic_sidecar_smoke(
                workspace=live_workspace,
                runtime_home=live_runtime_home,
                data_directory=live_data_dir,
                temporal_executable=args.temporal_executable,
                startup_timeout=args.live_startup_timeout,
                completion_timeout=args.live_completion_timeout,
                stop_timeout=args.live_stop_timeout,
            )
        if args.default_routing_sidecar_smoke:
            live_workspace = args.live_workspace or Path(temporary_dir) / (
                "live-workspace"
            )
            live_runtime_home = (
                args.live_runtime_home or Path(temporary_dir) / "live-runtime"
            )
            live_data_dir = args.live_data_dir or Path(temporary_dir) / ("live-data")
            report["default_routing_sidecar"] = check_default_routing_sidecar_smoke(
                workspace=live_workspace,
                runtime_home=live_runtime_home,
                data_directory=live_data_dir,
                temporal_executable=args.temporal_executable,
                startup_timeout=args.live_startup_timeout,
                completion_timeout=args.live_completion_timeout,
                stop_timeout=args.live_stop_timeout,
            )
        if args.quality_sidecar_smoke:
            live_workspace = args.live_workspace or Path(temporary_dir) / (
                "live-workspace"
            )
            live_runtime_home = (
                args.live_runtime_home or Path(temporary_dir) / "live-runtime"
            )
            live_data_dir = args.live_data_dir or Path(temporary_dir) / ("live-data")
            report["quality_sidecar"] = check_quality_sidecar_smoke(
                workspace=live_workspace,
                runtime_home=live_runtime_home,
                data_directory=live_data_dir,
                temporal_executable=args.temporal_executable,
                startup_timeout=args.live_startup_timeout,
                stop_timeout=args.live_stop_timeout,
            )
        if args.live_sidecar_smoke:
            live_workspace = args.live_workspace or Path(temporary_dir) / (
                "live-workspace"
            )
            live_runtime_home = (
                args.live_runtime_home or Path(temporary_dir) / "live-runtime"
            )
            live_data_dir = args.live_data_dir or Path(temporary_dir) / ("live-data")
            report["live_sidecar"] = check_live_sidecar_smoke(
                workspace=live_workspace,
                runtime_home=live_runtime_home,
                data_directory=live_data_dir,
                temporal_executable=args.temporal_executable,
                startup_timeout=args.live_startup_timeout,
                completion_timeout=args.live_completion_timeout,
                stop_timeout=args.live_stop_timeout,
            )

    print(json.dumps(report, indent=2, sort_keys=True))  # noqa:T201


if __name__ == "__main__":
    main()
