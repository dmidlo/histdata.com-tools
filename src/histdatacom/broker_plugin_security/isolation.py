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
    bootstrap_source: str = field(repr=False)
    python_search_paths: tuple[str, ...] = field(repr=False)

    def __repr__(self) -> str:
        return "BrokerKernelLaunch(<host-owned>)"


def require_kernel_backend() -> None:
    if sys.platform != "darwin" or os.name != "posix":
        refuse(Reason.UNSUPPORTED)


def _runtime_layout(
    python: str, source_root: Path
) -> tuple[tuple[Path, ...], tuple[str, ...]]:
    # No site/.pth/plugin code is evaluated by this trusted-interpreter query.
    query = "import json,sys,sysconfig;print(json.dumps([sys.base_prefix,sys.version_info.major,sys.version_info.minor,sysconfig.get_path('purelib'),sysconfig.get_path('platlib')]))"
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
        or len(data) != 5
        or type(data[0]) is not str
        or type(data[1]) is not int
        or type(data[2]) is not int
        or type(data[3]) is not str
        or type(data[4]) is not str
    ):
        refuse(Reason.ISOLATION)
    base = Path(data[0]).resolve(strict=True)
    environment = Path(python).absolute().parent.parent.resolve()
    source = source_root.resolve(strict=True)
    is_venv = (environment / "pyvenv.cfg").is_file()
    sites: tuple[Path, ...]
    if is_venv:
        # Python 3.10--3.13 do not restore a venv's prefix with -S. Derive its
        # conventional macOS site directory from the explicitly chosen worker,
        # never by importing site or executing its .pth files. System/user site
        # inheritance and arbitrary .pth search paths are deliberately omitted.
        sites = (
            environment
            / "lib"
            / f"python{data[1]}.{data[2]}"
            / "site-packages",
        )
    else:
        sites = (Path(data[3]), Path(data[4]))
    search = tuple(
        dict.fromkeys(
            str(path.resolve(strict=True))
            for path in (source, *sites)
            if path.is_dir()
        )
    )
    roots = {
        base,
        source,
        Path("/System/Library"),
        Path("/usr/lib"),
        *(Path(path) for path in search),
    }
    if is_venv:
        roots.add(environment)
    if base.is_relative_to("/opt/local"):
        roots.add(Path("/opt/local/lib"))
        openssl = Path("/opt/local/libexec/openssl3/lib")
        if openssl.is_dir():
            roots.add(openssl.resolve())
    if any(path == Path("/") or not path.is_dir() for path in roots):
        refuse(Reason.ISOLATION)
    return tuple(sorted(roots)), search


def _sealed_bootstrap(profile: str, search: tuple[str, ...]) -> str:
    """Trusted -I -S -B prelude, before any host or plugin package import.

    sandbox-exec must permit its initial exec, which also permits a plugin to
    replace itself with that interpreter. Applying the final profile inside the
    already-running trusted interpreter removes that exception. A second
    sandbox_init cannot tighten an existing profile on the qualified backend.
    """
    return (
        "import sys as _broker_sys\n"
        "if not (_broker_sys.flags.isolated and _broker_sys.flags.no_site "
        "and _broker_sys.dont_write_bytecode):\n"
        "    raise SystemExit(78)\n"
        "import ctypes as _broker_ctypes\n"
        "_broker_sandbox = _broker_ctypes.CDLL('/usr/lib/libsandbox.dylib', use_errno=True)\n"
        "_broker_sandbox.sandbox_init.argtypes = [_broker_ctypes.c_char_p, _broker_ctypes.c_uint64, _broker_ctypes.POINTER(_broker_ctypes.c_char_p)]\n"
        "_broker_sandbox.sandbox_init.restype = _broker_ctypes.c_int\n"
        "_broker_error = _broker_ctypes.c_char_p()\n"
        "if _broker_sandbox.sandbox_init("
        + repr(profile.encode("utf-8"))
        + ", 0, _broker_ctypes.byref(_broker_error)) != 0:\n"
        "    raise SystemExit(78)\n"
        # Keep the isolated interpreter's standard-library search precedence;
        # only append host-approved roots after the kernel policy is active.
        "for _broker_path in " + repr(search) + ":\n"
        "    if _broker_path not in _broker_sys.path:\n"
        "        _broker_sys.path.append(_broker_path)\n"
    )


def prepare_kernel_launch(
    worker_python: str,
    source_root: Path,
    working_directory: Path,
    protected_paths: tuple[Path, ...],
    loopback_ports: tuple[int, ...],
) -> BrokerKernelLaunch:
    """Qualify this exact read/write/network profile before secrets or plugins.

    Runtime/code roots are readable and therefore must contain no credentials.
    The caller must prepend bootstrap_source to its trusted worker command and
    launch with -I -S -B. No plugin or site code may run before the prelude.
    All fork/exec and new filesystem writes are denied. Already-open descriptors
    are intentionally usable. Network is off except exact declared loopback
    ports; their server/proxy is a separate caller-owned trust boundary.
    """
    require_kernel_backend()
    try:
        roots, search = _runtime_layout(worker_python, source_root)
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
                "(deny process-exec process-fork)",
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
            (),
            environment,
            working_directory,
            _sealed_bootstrap(profile, search),
            search,
        )
        probe = working_directory / "kernel-probe"
        probe.write_bytes(b"synthetic-probe-only")
        denied_port = next(
            port for port in range(1, 65536) if port not in loopback_ports
        )
        program = """
import os,socket,sys
for operation in (lambda: open(sys.argv[1]).read(), lambda: os.open(sys.argv[1], os.O_WRONLY), lambda: socket.create_connection(('127.0.0.1',int(sys.argv[2])),.1), lambda: os.fork(), lambda: os.posix_spawn(sys.executable,[sys.executable,'-I','-S','-c','pass'],{'LANG':'C'}), lambda: os.execve(sys.executable,[sys.executable,'-I','-S','-c','raise SystemExit(80)'],{'LANG':'C'})):
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
                "-S",
                "-B",
                "-c",
                launch.bootstrap_source + "\n" + program,
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
