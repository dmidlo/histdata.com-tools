"""Tests for the Docker-backed live InfluxDB smoke helper."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any


def _module():
    script_path = (
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "smoke_influx_docker.py"
    )
    spec = importlib.util.spec_from_file_location(
        "smoke_influx_docker",
        script_path,
    )
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _completed(module: Any, stdout: str = "", stderr: str = "") -> Any:
    return module.subprocess.CompletedProcess(
        args=["docker"],
        returncode=0,
        stdout=stdout,
        stderr=stderr,
    )


def test_parse_docker_port_accepts_bound_ipv4_port() -> None:
    """Docker port output should become a localhost endpoint."""
    module = _module()

    assert module._parse_docker_port("127.0.0.1:49153\n") == (
        "127.0.0.1",
        49153,
    )


def test_start_influx_container_sets_up_disposable_service() -> None:
    """The Docker command should create a one-shot initialized InfluxDB v2."""
    module = _module()
    calls: list[tuple[list[str], bool]] = []
    health_calls: list[tuple[str, float]] = []

    def fake_run(command: list[str], *, check: bool = True) -> Any:
        calls.append((list(command), check))
        if command[:2] == ["docker", "run"]:
            return _completed(module, "container-id\n")
        if command == ["docker", "port", "influx-smoke", "8086/tcp"]:
            return _completed(module, "127.0.0.1:49153\n")
        raise AssertionError(command)

    def fake_health(url: str, timeout: float) -> dict[str, str]:
        health_calls.append((url, timeout))
        return {"status": "pass"}

    container = module.start_influx_container(
        image="influxdb:test",
        container_name="influx-smoke",
        org="org",
        bucket="bucket",
        username="user",
        password="password",
        token="token",
        startup_timeout=12.5,
        run_command=fake_run,
        wait_for_health=fake_health,
    )

    run_command = calls[0][0]
    assert run_command[:7] == [
        "docker",
        "run",
        "--detach",
        "--rm",
        "--name",
        "influx-smoke",
        "--publish",
    ]
    assert "127.0.0.1::8086" in run_command
    assert "DOCKER_INFLUXDB_INIT_MODE=setup" in run_command
    assert "DOCKER_INFLUXDB_INIT_ORG=org" in run_command
    assert "DOCKER_INFLUXDB_INIT_BUCKET=bucket" in run_command
    assert "DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=token" in run_command
    assert run_command[-1] == "influxdb:test"
    assert health_calls == [("http://127.0.0.1:49153", 12.5)]
    assert container.to_dict()["container_id"] == "container-id"
    assert container.url == "http://127.0.0.1:49153"


def test_write_and_verify_influx_uses_real_writer_contract() -> None:
    """Smoke writes should flow through InfluxBatchWriter-compatible APIs."""
    module = _module()
    written_batches: list[list[str]] = []
    query_calls: list[tuple[str, str]] = []

    class FakeWriter:
        def __init__(self, args: dict[str, str]) -> None:
            self.args = args

        def __enter__(self) -> "FakeWriter":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def write_lines(self, lines: list[str]) -> None:
            written_batches.append(lines)

    class FakeRecord:
        def __init__(self, value: int) -> None:
            self.value = value

        def get_value(self) -> int:
            return self.value

    class FakeTable:
        records = [FakeRecord(4), FakeRecord(2)]

    class FakeQueryApi:
        def query(self, query: str, *, org: str) -> list[FakeTable]:
            query_calls.append((query, org))
            return [FakeTable()]

    class FakeClient:
        def __init__(self, **kwargs: str) -> None:
            self.kwargs = kwargs

        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def query_api(self) -> FakeQueryApi:
            return FakeQueryApi()

    report = module.write_and_verify_influx(
        url="http://127.0.0.1:49153",
        org="org",
        bucket="bucket",
        token="token",
        writer_factory=FakeWriter,
        client_factory=FakeClient,
    )

    assert written_batches == [[line] for line in module.SMOKE_LINES]
    assert report["written_lines"] == 2
    assert report["actual_field_count"] == 6
    assert query_calls[0][1] == "org"
    assert 'from(bucket: "bucket")' in query_calls[0][0]
    assert 'r._measurement == "eurusd"' in query_calls[0][0]


def test_run_docker_influx_smoke_cleans_up_container_on_success() -> None:
    """Successful smoke runs should still remove the disposable container."""
    module = _module()
    calls: list[list[str]] = []

    def fake_run(command: list[str], *, check: bool = True) -> Any:
        calls.append(list(command))
        if command[:2] == ["docker", "run"]:
            return _completed(module, "container-id\n")
        if command[:2] == ["docker", "port"]:
            return _completed(module, "127.0.0.1:49153\n")
        if command[:3] == ["docker", "rm", "--force"]:
            return _completed(module, "influx-smoke\n")
        raise AssertionError(command)

    report = module.run_docker_influx_smoke(
        image="influxdb:test",
        container_name="influx-smoke",
        run_command=fake_run,
        wait_for_health=lambda url, timeout: {"status": "pass"},
        writer_factory=lambda args: _NoopWriter(),
        client_factory=lambda **kwargs: _CountClient(6),
    )

    assert report["status"] == "passed"
    assert report["container"]["name"] == "influx-smoke"
    assert ["docker", "rm", "--force", "influx-smoke"] in calls


def test_run_docker_influx_smoke_includes_logs_on_failure() -> None:
    """Failed live smokes should include Docker logs before cleanup."""
    module = _module()
    calls: list[list[str]] = []

    def fake_run(command: list[str], *, check: bool = True) -> Any:
        calls.append(list(command))
        if command[:2] == ["docker", "run"]:
            return _completed(module, "container-id\n")
        if command[:2] == ["docker", "port"]:
            return _completed(module, "127.0.0.1:49153\n")
        if command[:2] == ["docker", "logs"]:
            return _completed(module, "influx log\n")
        if command[:3] == ["docker", "rm", "--force"]:
            return _completed(module, "influx-smoke\n")
        raise AssertionError(command)

    try:
        module.run_docker_influx_smoke(
            image="influxdb:test",
            container_name="influx-smoke",
            run_command=fake_run,
            wait_for_health=lambda url, timeout: {"status": "pass"},
            writer_factory=lambda args: _NoopWriter(),
            client_factory=lambda **kwargs: _CountClient(0),
        )
    except module.DockerInfluxSmokeError as err:
        diagnostics = err.diagnostics
    else:
        raise AssertionError("expected DockerInfluxSmokeError")

    assert diagnostics["logs"] == "influx log\n"
    assert diagnostics["actual_field_count"] == 0
    assert ["docker", "rm", "--force", "influx-smoke"] in calls


class _NoopWriter:
    def __enter__(self) -> "_NoopWriter":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def write_lines(self, lines: list[str]) -> None:
        return None


class _CountClient:
    def __init__(self, count: int) -> None:
        self.count = count

    def __enter__(self) -> "_CountClient":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def query_api(self) -> "_CountClient":
        return self

    def query(self, query: str, *, org: str) -> list[Any]:
        return [_CountTable(self.count)]


class _CountTable:
    def __init__(self, count: int) -> None:
        self.records = [_CountRecord(count)]


class _CountRecord:
    def __init__(self, count: int) -> None:
        self.count = count

    def get_value(self) -> int:
        return self.count
