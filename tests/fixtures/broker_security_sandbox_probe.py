"""Offline kernel-policy feasibility probe; never reads host credentials."""

from __future__ import annotations

import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="broker-security-probe-") as raw:
        root = Path(raw).resolve()
        protected = root / "protected"
        protected.mkdir()
        secret = protected / "private.txt"
        secret.write_text("synthetic-private-canary")
        environment = {
            "HOME": str(root / "empty-home"),
            "TMPDIR": str(root),
            "LANG": "C",
        }
        roots = (
            Path(sys.prefix).resolve(),
            Path(sys.base_prefix).resolve(),
            Path("/System/Library"),
            Path("/usr/lib"),
            *(
                (Path("/opt/local/lib"),)
                if Path(sys.base_prefix).is_relative_to("/opt/local")
                else ()
            ),
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
                "(allow file-read-data file-map-executable "
                + " ".join(
                    "(subpath " + json.dumps(str(path)) + ")" for path in roots
                )
                + ")",
            )
        )
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = listener.getsockname()[1]
        source = """
import ctypes,json,os,socket,sys
library=ctypes.CDLL('/usr/lib/libsandbox.dylib',use_errno=True)
library.sandbox_init.argtypes=[ctypes.c_char_p,ctypes.c_uint64,ctypes.POINTER(ctypes.c_char_p)]
library.sandbox_init.restype=ctypes.c_int
error=ctypes.c_char_p()
if library.sandbox_init(sys.argv[3].encode(),0,ctypes.byref(error)):
    raise SystemExit(78)
root = sys.argv[1]
checks = {}
def denied(name, operation):
    try:
        operation()
    except PermissionError:
        checks[name] = True
    else:
        checks[name] = False
denied('read', lambda: open(root + '/protected/private.txt').read())
denied('write', lambda: open(root + '/protected/new.txt', 'w'))
denied('truncate', lambda: os.truncate(root + '/protected/private.txt', 0))
denied('rename', lambda: os.rename(root + '/protected/private.txt', root + '/renamed'))
denied('link', lambda: os.link(root + '/protected/private.txt', root + '/linked'))
denied('symlink', lambda: os.symlink(root + '/protected/private.txt', root + '/alias'))
denied('network', lambda: socket.create_connection(('127.0.0.1', int(sys.argv[2])), .2))
denied('descendant', lambda: os.fork())
denied('spawn', lambda: os.posix_spawn(sys.executable,[sys.executable,'-I','-S','-c','pass'],{'LANG':'C'}))
denied('exec', lambda: os.execve(sys.executable,[sys.executable,'-I','-S','-c','raise SystemExit(80)'],{'LANG':'C'}))
print(json.dumps(checks, sort_keys=True))
"""
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    "-S",
                    "-B",
                    "-c",
                    source,
                    str(root),
                    str(port),
                    profile,
                ],
                env=environment,
                cwd=root,
                capture_output=True,
                text=True,
                timeout=5,
            )
        finally:
            listener.close()
        if result.returncode:
            raise RuntimeError(
                f"sandbox probe failed ({result.returncode}): " + result.stderr
            )
        checks = json.loads(result.stdout)
        assert all(checks.values()), checks
        assert secret.read_text() == "synthetic-private-canary"
        assert list(protected.iterdir()) == [secret]
        print(json.dumps(checks, sort_keys=True))


if __name__ == "__main__":
    main()
