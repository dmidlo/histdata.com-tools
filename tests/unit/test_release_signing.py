"""Tests for local release signing guardrails."""

from __future__ import annotations

import os
from pathlib import Path
import shlex
import subprocess
import textwrap

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PYPI_SCRIPT = PROJECT_ROOT / "pypi.sh"


def _bash_quote(path: Path) -> str:
    """Return a shell-escaped path string."""
    return shlex.quote(str(path))


def _write_gpg_stub(bin_dir: Path, body: str) -> Path:
    """Write an executable gpg stub into a temporary PATH directory."""
    bin_dir.mkdir(parents=True, exist_ok=True)
    gpg = bin_dir / "gpg"
    gpg.write_text(
        "#!/usr/bin/env bash\nset -euo pipefail\n" + body,
        encoding="utf-8",
    )
    gpg.chmod(0o755)
    return gpg


def _run_bash(
    script: str,
    *,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a bash snippet from the project root."""
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    return subprocess.run(  # noqa:S603
        ["bash", "-c", script],
        cwd=PROJECT_ROOT,
        env=merged_env,
        capture_output=True,
        check=False,
        text=True,
    )


def test_release_signing_preflight_blocks_upload_before_build(
    tmp_path: Path,
) -> None:
    """Missing default signing keys should fail before build or upload."""
    bin_dir = tmp_path / "bin"
    trace = tmp_path / "trace.log"
    gpg_log = tmp_path / "gpg.log"
    _write_gpg_stub(
        bin_dir,
        textwrap.dedent(f"""\
            printf '%s\\n' "$*" >> {_bash_quote(gpg_log)}
            if [[ "$*" == *"--detach-sign"* ]]; then
                echo "gpg: no default secret key: No secret key" >&2
                exit 2
            fi
            exit 0
            """),
    )
    script = textwrap.dedent(f"""\
        source {_bash_quote(PYPI_SCRIPT)}
        prepare_release_upload() {{ printf 'prepare\\n' >> {_bash_quote(trace)}; }}
        build() {{ printf 'build\\n' >> {_bash_quote(trace)}; }}
        sign_dist_artifacts() {{ printf 'sign\\n' >> {_bash_quote(trace)}; }}
        upload_dist_artifacts() {{ printf 'upload\\n' >> {_bash_quote(trace)}; }}
        main testpypi
        """)

    result = _run_bash(
        script,
        env={"PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"},
    )

    assert result.returncode == 2
    assert "GPG signing preflight failed with the default signing key" in (
        result.stderr
    )
    assert "HISTDATACOM_GPG_KEY" in result.stderr
    assert trace.read_text(encoding="utf-8") == "prepare\n"
    assert "--detach-sign" in gpg_log.read_text(encoding="utf-8")


def test_release_signing_passes_explicit_key_to_gpg(tmp_path: Path) -> None:
    """HISTDATACOM_GPG_KEY should select the GPG local-user key."""
    bin_dir = tmp_path / "bin"
    gpg_log = tmp_path / "gpg.log"
    _write_gpg_stub(
        bin_dir,
        textwrap.dedent(f"""\
            for arg in "$@"; do
                printf '<%s>\\n' "$arg" >> {_bash_quote(gpg_log)}
            done
            printf '%s\\n' '---' >> {_bash_quote(gpg_log)}
            exit 0
            """),
    )
    script = textwrap.dedent(f"""\
        source {_bash_quote(PYPI_SCRIPT)}
        cd {_bash_quote(tmp_path)}
        mkdir -p dist
        : > dist/histdatacom-1.3.2-py3-none-any.whl
        : > dist/histdatacom-1.3.2.tar.gz
        sign_dist_artifacts
        """)

    result = _run_bash(
        script,
        env={
            "HISTDATACOM_GPG_KEY": "release@example.test",
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        },
    )

    assert result.returncode == 0, result.stderr
    gpg_args = gpg_log.read_text(encoding="utf-8")
    assert "<--batch>" in gpg_args
    assert "<--yes>" in gpg_args
    assert "<--local-user>" in gpg_args
    assert "<release@example.test>" in gpg_args
    assert "<--detach-sign>" in gpg_args
    assert "<--armor>" in gpg_args
    assert "<dist/histdatacom-1.3.2-py3-none-any.whl>" in gpg_args
    assert "<dist/histdatacom-1.3.2.tar.gz>" in gpg_args


def test_release_signing_skip_removes_stale_signatures(
    tmp_path: Path,
) -> None:
    """Unsigned uploads should not accidentally reuse old .asc artifacts."""
    bin_dir = tmp_path / "bin"
    gpg_log = tmp_path / "gpg.log"
    stale_signature = tmp_path / "dist" / "histdatacom-1.3.2.tar.gz.asc"
    _write_gpg_stub(
        bin_dir,
        textwrap.dedent(f"""\
            printf 'unexpected gpg call\\n' >> {_bash_quote(gpg_log)}
            exit 99
            """),
    )
    script = textwrap.dedent(f"""\
        source {_bash_quote(PYPI_SCRIPT)}
        cd {_bash_quote(tmp_path)}
        mkdir -p dist
        : > dist/histdatacom-1.3.2-py3-none-any.whl
        : > dist/histdatacom-1.3.2.tar.gz
        : > {_bash_quote(stale_signature)}
        sign_dist_artifacts
        test ! -e {_bash_quote(stale_signature)}
        """)

    result = _run_bash(
        script,
        env={
            "HISTDATACOM_SKIP_GPG_SIGNING": "1",
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        },
    )

    assert result.returncode == 0, result.stderr
    assert "HISTDATACOM_SKIP_GPG_SIGNING=1" in result.stderr
    assert not stale_signature.exists()
    assert not gpg_log.exists()
