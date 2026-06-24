"""Run a Docker-backed live InfluxDB smoke for histdatacom imports."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

DEFAULT_INFLUX_IMAGE = "influxdb:2.7-alpine"
DEFAULT_ORG = "histdatacom"
DEFAULT_BUCKET = "histdatacom-smoke"
DEFAULT_USERNAME = "histdatacom"
DEFAULT_PASSWORD = "histdatacom-password"
DEFAULT_TOKEN = "histdatacom-smoke-token"
DEFAULT_STARTUP_TIMEOUT = 60.0
INFLUX_PORT = "8086/tcp"
SMOKE_MEASUREMENT = "eurusd"
SMOKE_LINES = (
    "eurusd,source=histdata.com,format=ascii,timeframe=M1 "
    "openbid=1.30657,highbid=1.30657,lowbid=1.30647,closebid=1.30656 "
    "1328072460000",
    "eurusd,source=histdata.com,format=ascii,timeframe=T "
    "bidquote=1.30658,askquote=1.30675 1328072403973",
)
EXPECTED_FIELD_COUNT = 6

RunCommand = Callable[..., subprocess.CompletedProcess[str]]
HealthWaiter = Callable[[str, float], Mapping[str, Any]]


class DockerInfluxSmokeError(RuntimeError):
    """Raised when the Docker-backed Influx smoke fails."""

    def __init__(self, message: str, diagnostics: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.diagnostics = dict(diagnostics)


@dataclass(frozen=True, slots=True)
class InfluxContainer:
    """Running InfluxDB container details."""

    container_id: str
    name: str
    url: str
    host: str
    port: int

    def to_dict(self) -> dict[str, str | int]:
        """Return JSON-compatible container details."""
        return {
            "container_id": self.container_id,
            "name": self.name,
            "url": self.url,
            "host": self.host,
            "port": self.port,
        }


def _run(
    command: Sequence[str],
    *,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run a command with captured output and optional failure checking."""
    completed = subprocess.run(
        list(command),
        capture_output=True,
        check=False,
        text=True,
    )
    if check and completed.returncode != 0:
        raise DockerInfluxSmokeError(
            f"command failed with exit {completed.returncode}: "
            f"{' '.join(command)}",
            {
                "command": list(command),
                "returncode": completed.returncode,
                "stdout": completed.stdout,
                "stderr": completed.stderr,
            },
        )
    return completed


def _default_container_name() -> str:
    return f"histdatacom-influx-smoke-{os.getpid()}-{secrets.token_hex(4)}"


def _parse_docker_port(port_output: str) -> tuple[str, int]:
    """Parse `docker port` output into a host and integer port."""
    for line in port_output.splitlines():
        endpoint = line.strip()
        if not endpoint:
            continue
        host, separator, raw_port = endpoint.rpartition(":")
        if separator and raw_port.isdigit():
            return host.strip("[]") or "127.0.0.1", int(raw_port)
    raise DockerInfluxSmokeError(
        "docker did not report a published InfluxDB port",
        {"docker_port_output": port_output},
    )


def start_influx_container(
    *,
    image: str = DEFAULT_INFLUX_IMAGE,
    container_name: str | None = None,
    org: str = DEFAULT_ORG,
    bucket: str = DEFAULT_BUCKET,
    username: str = DEFAULT_USERNAME,
    password: str = DEFAULT_PASSWORD,
    token: str = DEFAULT_TOKEN,
    startup_timeout: float = DEFAULT_STARTUP_TIMEOUT,
    run_command: RunCommand = _run,
    wait_for_health: HealthWaiter | None = None,
) -> InfluxContainer:
    """Start a disposable InfluxDB v2 container and wait for readiness."""
    name = container_name or _default_container_name()
    completed = run_command(
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            name,
            "--publish",
            "127.0.0.1::8086",
            "--env",
            "DOCKER_INFLUXDB_INIT_MODE=setup",
            "--env",
            f"DOCKER_INFLUXDB_INIT_USERNAME={username}",
            "--env",
            f"DOCKER_INFLUXDB_INIT_PASSWORD={password}",
            "--env",
            f"DOCKER_INFLUXDB_INIT_ORG={org}",
            "--env",
            f"DOCKER_INFLUXDB_INIT_BUCKET={bucket}",
            "--env",
            f"DOCKER_INFLUXDB_INIT_ADMIN_TOKEN={token}",
            image,
        ]
    )
    container_id = completed.stdout.strip()
    port_output = run_command(["docker", "port", name, INFLUX_PORT]).stdout
    host, port = _parse_docker_port(port_output)
    url = f"http://{host}:{port}"
    health = wait_for_health or wait_for_influx_health
    health(url, startup_timeout)
    return InfluxContainer(
        container_id=container_id,
        name=name,
        url=url,
        host=host,
        port=port,
    )


def wait_for_influx_health(
    url: str,
    timeout: float = DEFAULT_STARTUP_TIMEOUT,
) -> dict[str, Any]:
    """Poll the InfluxDB health endpoint until it reports readiness."""
    deadline = time.monotonic() + timeout
    health_url = f"{url.rstrip('/')}/health"
    last_error = ""
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(health_url, timeout=2.0) as response:
                body = response.read().decode("utf-8", errors="replace")
                payload = json.loads(body) if body else {}
                status = str(payload.get("status", "")).lower()
                if response.status < 500 and status == "pass":
                    return payload
                last_error = body
        except (
            OSError,
            TimeoutError,
            urllib.error.HTTPError,
            urllib.error.URLError,
            json.JSONDecodeError,
        ) as err:
            last_error = repr(err)
        time.sleep(0.5)
    raise DockerInfluxSmokeError(
        "InfluxDB container did not become healthy",
        {"url": health_url, "last_error": last_error},
    )


def stop_influx_container(
    container: InfluxContainer,
    *,
    run_command: RunCommand = _run,
) -> subprocess.CompletedProcess[str]:
    """Force-remove the disposable InfluxDB container."""
    return run_command(["docker", "rm", "--force", container.name])


def docker_logs(
    container: InfluxContainer,
    *,
    run_command: RunCommand = _run,
) -> str:
    """Return current Docker logs for diagnostics."""
    completed = run_command(
        ["docker", "logs", container.name],
        check=False,
    )
    return completed.stdout + completed.stderr


def write_and_verify_influx(
    *,
    url: str,
    org: str = DEFAULT_ORG,
    bucket: str = DEFAULT_BUCKET,
    token: str = DEFAULT_TOKEN,
    lines: Sequence[str] = SMOKE_LINES,
    expected_field_count: int = EXPECTED_FIELD_COUNT,
    writer_factory: Callable[[Mapping[str, Any]], Any] | None = None,
    client_factory: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Write representative line-protocol batches and verify via Flux query."""
    writer_factory = writer_factory or _influx_batch_writer_factory()
    args = {
        "INFLUX_ORG": org,
        "INFLUX_BUCKET": bucket,
        "INFLUX_URL": url,
        "INFLUX_TOKEN": token,
    }
    with writer_factory(args) as writer:
        for line in lines:
            writer.write_lines([line])

    field_count = query_influx_field_count(
        url=url,
        org=org,
        bucket=bucket,
        token=token,
        client_factory=client_factory,
    )
    if field_count < expected_field_count:
        raise DockerInfluxSmokeError(
            "InfluxDB smoke query returned fewer fields than expected",
            {
                "expected_field_count": expected_field_count,
                "actual_field_count": field_count,
                "url": url,
                "bucket": bucket,
            },
        )
    return {
        "written_lines": len(lines),
        "expected_field_count": expected_field_count,
        "actual_field_count": field_count,
    }


def query_influx_field_count(
    *,
    url: str,
    org: str = DEFAULT_ORG,
    bucket: str = DEFAULT_BUCKET,
    token: str = DEFAULT_TOKEN,
    measurement: str = SMOKE_MEASUREMENT,
    client_factory: Callable[..., Any] | None = None,
) -> int:
    """Return the number of field values written for the smoke measurement."""
    client_factory = client_factory or _influx_client_factory()
    query = (
        f'from(bucket: "{bucket}") '
        "|> range(start: 2012-02-01T00:00:00Z, "
        "stop: 2012-02-02T00:00:00Z) "
        f'|> filter(fn: (r) => r._measurement == "{measurement}") '
        '|> count(column: "_value")'
    )
    with client_factory(url=url, token=token, org=org) as client:
        tables = client.query_api().query(query, org=org)
    return _sum_query_record_values(tables)


def _sum_query_record_values(tables: Any) -> int:
    total = 0
    for table in tables:
        for record in getattr(table, "records", ()):
            value = record.get_value()
            total += int(value)
    return total


def _influx_batch_writer_factory() -> Callable[[Mapping[str, Any]], Any]:
    from histdatacom.influx import InfluxBatchWriter

    return InfluxBatchWriter


def _influx_client_factory() -> Callable[..., Any]:
    from influxdb_client import InfluxDBClient

    return InfluxDBClient


def run_docker_influx_smoke(
    *,
    image: str = DEFAULT_INFLUX_IMAGE,
    container_name: str | None = None,
    org: str = DEFAULT_ORG,
    bucket: str = DEFAULT_BUCKET,
    username: str = DEFAULT_USERNAME,
    password: str = DEFAULT_PASSWORD,
    token: str = DEFAULT_TOKEN,
    startup_timeout: float = DEFAULT_STARTUP_TIMEOUT,
    keep_container: bool = False,
    run_command: RunCommand = _run,
    wait_for_health: HealthWaiter | None = None,
    writer_factory: Callable[[Mapping[str, Any]], Any] | None = None,
    client_factory: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Run the full Docker-backed InfluxDB smoke and return a report."""
    container: InfluxContainer | None = None
    diagnostics: dict[str, Any] = {}
    try:
        container = start_influx_container(
            image=image,
            container_name=container_name,
            org=org,
            bucket=bucket,
            username=username,
            password=password,
            token=token,
            startup_timeout=startup_timeout,
            run_command=run_command,
            wait_for_health=wait_for_health,
        )
        write_report = write_and_verify_influx(
            url=container.url,
            org=org,
            bucket=bucket,
            token=token,
            writer_factory=writer_factory,
            client_factory=client_factory,
        )
        return {
            "status": "passed",
            "image": image,
            "container": container.to_dict(),
            "influx": {
                "org": org,
                "bucket": bucket,
                "url": container.url,
            },
            "write": write_report,
        }
    except Exception as err:
        diagnostics["error"] = repr(err)
        if container is not None:
            diagnostics["container"] = container.to_dict()
            diagnostics["logs"] = docker_logs(
                container,
                run_command=run_command,
            )
        if isinstance(err, DockerInfluxSmokeError):
            diagnostics.update(err.diagnostics)
        raise DockerInfluxSmokeError(
            f"Docker InfluxDB smoke failed: {err}",
            diagnostics,
        ) from err
    finally:
        if container is not None and not keep_container:
            stop_influx_container(container, run_command=run_command)


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    parser = argparse.ArgumentParser(
        description="Run a Docker-backed live InfluxDB smoke."
    )
    parser.add_argument("--image", default=DEFAULT_INFLUX_IMAGE)
    parser.add_argument("--container-name", default=None)
    parser.add_argument("--org", default=DEFAULT_ORG)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--username", default=DEFAULT_USERNAME)
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument("--token", default=DEFAULT_TOKEN)
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=DEFAULT_STARTUP_TIMEOUT,
    )
    parser.add_argument(
        "--keep-container",
        action="store_true",
        help="leave the InfluxDB container running after the smoke",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Docker-backed live InfluxDB smoke."""
    args = build_parser().parse_args(argv)
    try:
        report = run_docker_influx_smoke(
            image=args.image,
            container_name=args.container_name,
            org=args.org,
            bucket=args.bucket,
            username=args.username,
            password=args.password,
            token=args.token,
            startup_timeout=args.startup_timeout,
            keep_container=args.keep_container,
        )
    except DockerInfluxSmokeError as err:
        print(
            json.dumps(err.diagnostics, indent=2, sort_keys=True),
            file=sys.stderr,
        )
        return 1
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
