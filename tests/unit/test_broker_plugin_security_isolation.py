"""Actual kernel-denial and permitted-operation probes on disposable assets."""

from __future__ import annotations

import json
from pathlib import Path
import socket
import subprocess
import sys

import pytest

from histdatacom.broker_plugin_security import BrokerSecurityError
from histdatacom.broker_plugin_security.isolation import prepare_kernel_launch

ROOT = Path(__file__).resolve().parents[2]
requires_kernel = pytest.mark.skipif(
    sys.platform != "darwin", reason="qualified kernel backend is macOS only"
)


@requires_kernel
def test_kernel_denies_direct_writers_reads_aliases_and_descendants(tmp_path):
    store = tmp_path / "scientific-store"
    store.mkdir()
    original = store / "existing"
    original.write_bytes(b"synthetic-private-account-only")
    alias = tmp_path / "alias"
    alias.symlink_to(store, target_is_directory=True)
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    launch = prepare_kernel_launch(
        sys.executable, ROOT / "src", cwd, (store,), ()
    )
    program = """
import json,mimetypes,os,sys
from pathlib import Path
sys.path.insert(0,sys.argv[2])
# The scientific writer's import chain includes openpyxl. Use only its built-in
# MIME types rather than reading unrelated host /etc Apache configuration.
mimetypes.knownfiles=[]
mimetypes.init()
from histdatacom.forecasting import default_forecast_registry,write_engine_artifact
root=Path(sys.argv[1]); checks={}
def denied(name,action):
    try: action()
    except PermissionError: checks[name]=True
    else: checks[name]=False
denied('read',lambda:(root/'scientific-store/existing').read_bytes())
denied('create',lambda:(root/'scientific-store/new').write_bytes(b'bad'))
denied('truncate',lambda:os.truncate(root/'scientific-store/existing',0))
denied('rename',lambda:os.rename(root/'scientific-store/existing',root/'moved'))
denied('link',lambda:os.link(root/'scientific-store/existing',root/'hardlink'))
denied('symlink',lambda:os.symlink(root/'scientific-store/existing',root/'another-alias'))
denied('alias',lambda:(root/'alias/existing').write_bytes(b'bad'))
denied('unlink',lambda:(root/'scientific-store/existing').unlink())
denied('scientific_writer',lambda:write_engine_artifact(default_forecast_registry(),root/'scientific-store'))
child=os.fork()
if child==0:
    try: (root/'scientific-store/descendant').write_bytes(b'bad')
    except PermissionError: os._exit(0)
    os._exit(1)
checks['descendant']=os.waitpid(child,0)[1]==0
checks['runtime_read']=bool(Path(sys.argv[2]+'/histdatacom/broker_plugin_security/contracts.py').read_bytes())
checks['metadata_read']=bool((root/'scientific-store/existing').stat().st_size)
print(json.dumps(checks,sort_keys=True))
"""
    result = subprocess.run(
        [
            *launch.command_prefix,
            sys.executable,
            "-I",
            "-B",
            "-c",
            program,
            str(tmp_path),
            str(ROOT / "src"),
        ],
        env=launch.environment,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    checks = json.loads(result.stdout)
    assert set(checks) == {
        "read",
        "create",
        "truncate",
        "rename",
        "link",
        "symlink",
        "alias",
        "unlink",
        "scientific_writer",
        "descendant",
        "runtime_read",
        "metadata_read",
    }
    assert all(checks.values()), checks
    assert original.read_bytes() == b"synthetic-private-account-only"
    assert list(store.iterdir()) == [original]


@requires_kernel
def test_exact_loopback_port_positive_other_port_and_external_negative(
    tmp_path,
):
    listener = socket.socket()
    other = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    other.bind(("127.0.0.1", 0))
    other.listen(1)
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    port = listener.getsockname()[1]
    other_port = other.getsockname()[1]
    launch = prepare_kernel_launch(
        sys.executable, ROOT / "src", cwd, (tmp_path / "store",), (port,)
    )
    program = """
import json,socket,sys
checks={}
with socket.create_connection(('127.0.0.1',int(sys.argv[1])),.5): checks['allowed']=True
for name,address in [('other',('127.0.0.1',int(sys.argv[2]))),('external',('198.51.100.1',443))]:
    try: socket.create_connection(address,.2)
    except PermissionError: checks[name]=True
    else: checks[name]=False
print(json.dumps(checks))
"""
    try:
        result = subprocess.run(
            [
                *launch.command_prefix,
                sys.executable,
                "-I",
                "-B",
                "-c",
                program,
                str(port),
                str(other_port),
            ],
            env=launch.environment,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=5,
        )
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == {
            "allowed": True,
            "other": True,
            "external": True,
        }
    finally:
        listener.close()
        other.close()


@requires_kernel
def test_protected_read_root_overlap_and_alias_are_refused(tmp_path):
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    alias = tmp_path / "runtime-alias"
    alias.symlink_to(ROOT / "src", target_is_directory=True)
    for protected in (ROOT / "src", alias, Path(sys.prefix)):
        with pytest.raises(
            BrokerSecurityError, match="security_enforcement_unavailable"
        ):
            prepare_kernel_launch(
                sys.executable, ROOT / "src", cwd, (protected,), ()
            )
    assert list(cwd.iterdir()) == []
