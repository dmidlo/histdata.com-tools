"""Qualified macOS kernel boundaries; no unsupported-platform fallback."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import subprocess
import sys

from .contracts import BrokerSecurityReason as Reason, refuse


@dataclass(frozen=True, slots=True, repr=False)
class BrokerKernelLaunch:
    command_prefix: tuple[str, ...] = field(repr=False)
    environment: dict[str, str] = field(repr=False)
    working_directory: Path = field(repr=False)

    def __repr__(self) -> str:
        return "BrokerKernelLaunch(<host-owned>)"


def require_kernel_backend() -> None:
    if (
        sys.platform != "darwin"
        or os.name != "posix"
        or not Path("/usr/bin/sandbox-exec").is_file()
    ):
        refuse(Reason.UNSUPPORTED)


def _runtime_roots(python: str, source_root: Path) -> tuple[Path, ...]:
    # No site/.pth/plugin code is evaluated by this trusted-interpreter query.
    query = "import json,sys;print(json.dumps([sys.base_prefix,sys.version_info.major,sys.version_info.minor]))"
    result = subprocess.run(
        [python, "-I", "-S", "-B", "-c", query],
        capture_output=True,
        timeout=3,
        env={"LANG": "C"},
    )
    if result.returncode or len(result.stdout) > 4096:
        refuse(Reason.ISOLATION)
    data = json.loads(result.stdout)
    if (
        type(data) is not list
        or len(data) != 3
        or type(data[0]) is not str
        or type(data[1]) is not int
        or type(data[2]) is not int
    ):
        refuse(Reason.ISOLATION)
    base = Path(data[0]).resolve()
    environment = Path(python).absolute().parent.parent.resolve()
    roots = {
        base,
        environment,
        source_root.resolve(),
        Path("/System/Library"),
        Path("/usr/lib"),
    }
    if base.is_relative_to("/opt/local"):
        roots.add(Path("/opt/local/lib"))
        openssl = Path("/opt/local/libexec/openssl3/lib")
        if openssl.is_dir():
            roots.add(openssl.resolve())
    if any(path == Path("/") or not path.is_dir() for path in roots):
        refuse(Reason.ISOLATION)
    return tuple(sorted(roots))


def prepare_kernel_launch(
    worker_python: str,
    source_root: Path,
    working_directory: Path,
    protected_paths: tuple[Path, ...],
    loopback_ports: tuple[int, ...],
) -> BrokerKernelLaunch:
    """Qualify this exact read/write/network profile before secrets or plugins.

    Runtime/code roots are readable and therefore must contain no credentials.
    All new filesystem writes are denied. Already-open transport descriptors
    are intentionally usable. Network is off except exact declared loopback
    ports; their server/proxy is a separate caller-owned trust boundary.
    """
    require_kernel_backend()
    try:
        roots = _runtime_roots(worker_python, source_root)
        if not protected_paths or len(protected_paths) > 128:
            refuse(Reason.ISOLATION)
        for protected in protected_paths:
            path = protected.resolve()
            if any(
                path.is_relative_to(root) or root.is_relative_to(path)
                for root in roots
            ):
                refuse(Reason.ISOLATION)
        expressions = " ".join(
            "(subpath " + json.dumps(str(root), ensure_ascii=False) + ")"
            for root in roots
        )
        profile = "\n".join(
            (
                "(version 1)",
                "(deny default)",
                '(import "system.sb")',
                "(allow process-exec process-fork)",
                "(allow signal (target self))",
                "(allow sysctl-read)",
                "(allow file-read-metadata)",
                "(allow file-read-data (literal "
                + json.dumps(str(working_directory), ensure_ascii=False)
                + "))",
                "(allow file-read-data file-map-executable "
                + expressions
                + ")",
                "(deny file-write*)",
                "(deny network*)",
                *(
                    f'(allow network-outbound (remote ip "localhost:{port}"))'
                    for port in loopback_ports
                ),
            )
        )
        if len(profile.encode("utf-8")) > 32_768:
            refuse(Reason.ISOLATION)
        environment = {
            "HOME": str(working_directory),
            "TMPDIR": str(working_directory),
            "LANG": "C",
        }
        launch = BrokerKernelLaunch(
            ("/usr/bin/sandbox-exec", "-p", profile),
            environment,
            working_directory,
        )
        probe = working_directory / "kernel-probe"
        probe.write_bytes(b"synthetic-probe-only")
        denied_port = next(
            port for port in range(1, 65536) if port not in loopback_ports
        )
        program = """
import os,socket,sys
for operation in (lambda: open(sys.argv[1]).read(), lambda: os.open(sys.argv[1], os.O_WRONLY), lambda: socket.create_connection(('127.0.0.1',int(sys.argv[2])),.1)):
    try:
        operation()
    except PermissionError:
        continue
    raise SystemExit(80)
print('qualified')
"""
        result = subprocess.run(
            [
                *launch.command_prefix,
                worker_python,
                "-I",
                "-B",
                "-c",
                program,
                str(probe),
                str(denied_port),
            ],
            env=environment,
            cwd=working_directory,
            capture_output=True,
            timeout=3,
        )
        if (
            result.returncode
            or result.stdout != b"qualified\n"
            or probe.read_bytes() != b"synthetic-probe-only"
        ):
            refuse(Reason.ISOLATION)
        probe.unlink()
        return launch
    except BaseException:
        refuse(Reason.ISOLATION)
