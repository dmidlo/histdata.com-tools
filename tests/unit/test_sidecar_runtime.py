"""Tests for Temporal sidecar runtime path and port policy."""

from __future__ import annotations

from pathlib import Path

import pytest

from histdatacom.sidecar.runtime import (
    DEFAULT_TEMPORAL_UI_PORT_OFFSET,
    PortAllocationError,
    build_sidecar_runtime_policy,
    default_sidecar_runtime_home,
)


def test_default_runtime_home_uses_macos_application_support() -> None:
    """macOS state should live in Application Support, not cwd."""
    home = Path("/Users/alice")

    assert default_sidecar_runtime_home(
        environ={},
        platform_name="Darwin",
        home=home,
    ) == (home / "Library" / "Application Support" / "histdatacom" / "sidecar")


def test_default_runtime_home_uses_linux_xdg_state_home() -> None:
    """Linux should honor XDG_STATE_HOME when present."""
    assert default_sidecar_runtime_home(
        environ={"XDG_STATE_HOME": "/state"},
        platform_name="Linux",
        home="/home/alice",
    ) == Path("/state/histdatacom/sidecar")


def test_default_runtime_home_uses_windows_local_app_data() -> None:
    """Windows should use the user-local application data root."""
    assert default_sidecar_runtime_home(
        environ={"LOCALAPPDATA": "C:/Users/Alice/AppData/Local"},
        platform_name="Windows",
        home="C:/Users/Alice",
    ) == Path("C:/Users/Alice/AppData/Local/histdatacom/sidecar")


def test_runtime_policy_scopes_paths_by_workspace(tmp_path: Path) -> None:
    """Same-named workspaces should not share sidecar runtime state."""
    runtime_home = tmp_path / "runtime"
    left = tmp_path / "left" / "project"
    right = tmp_path / "right" / "project"

    left_policy = build_sidecar_runtime_policy(
        workspace=left,
        runtime_home=runtime_home,
    )
    right_policy = build_sidecar_runtime_policy(
        workspace=right,
        runtime_home=runtime_home,
    )

    assert left_policy.workspace_id != right_policy.workspace_id
    assert left_policy.paths.runtime_dir != right_policy.paths.runtime_dir
    assert (
        left_policy.paths.state_dir == left_policy.paths.runtime_dir / "state"
    )
    assert left_policy.paths.sqlite_db == (
        left_policy.paths.runtime_dir / "sqlite" / "temporal.db"
    )
    assert left_policy.paths.runtime_manifest == (
        left_policy.paths.runtime_dir / "manifests" / "runtime-policy.json"
    )


def test_runtime_policy_stays_outside_download_data_dir(
    tmp_path: Path,
) -> None:
    """Sidecar state should not be placed inside data downloads/caches."""
    workspace = tmp_path / "workspace"
    runtime_home = tmp_path / "runtime"
    data_dir = workspace / "data"

    policy = build_sidecar_runtime_policy(
        workspace=workspace,
        runtime_home=runtime_home,
    )

    assert data_dir not in policy.paths.runtime_dir.parents
    assert policy.paths.runtime_dir.is_relative_to(runtime_home)


def test_port_allocation_skips_collisions_deterministically(
    tmp_path: Path,
) -> None:
    """Derived port collisions should move to the next deterministic pair."""
    initial = build_sidecar_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
    )
    blocked = {
        initial.ports.grpc,
        initial.ports.grpc + DEFAULT_TEMPORAL_UI_PORT_OFFSET,
    }

    policy = build_sidecar_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
        check_ports=True,
        port_available=lambda bind_ip, port: port not in blocked,
    )

    assert policy.ports.grpc not in blocked
    assert policy.ports.ui not in blocked
    assert set(policy.ports.collisions) == blocked


def test_environment_port_collision_fails_clearly(tmp_path: Path) -> None:
    """Explicit port overrides should not silently pick a different port."""
    with pytest.raises(PortAllocationError) as err:
        build_sidecar_runtime_policy(
            workspace=tmp_path / "workspace",
            runtime_home=tmp_path / "runtime",
            environ={
                "HISTDATACOM_SIDECAR_PORT": "19000",
                "HISTDATACOM_SIDECAR_UI_PORT": "20000",
            },
            check_ports=True,
            port_available=lambda bind_ip, port: port != 19000,
        )

    assert "HISTDATACOM_SIDECAR_PORT=19000" in str(err.value)
    assert "blocked=(19000,)" in str(err.value)


def test_temporal_start_args_include_sqlite_and_ports(
    tmp_path: Path,
) -> None:
    """Runtime policy should render Temporal's SQLite and port flags."""
    policy = build_sidecar_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
    )

    assert policy.temporal_start_args() == (
        "--db-filename",
        str(policy.paths.sqlite_db),
        "--ip",
        "127.0.0.1",
        "--port",
        str(policy.ports.grpc),
        "--ui-port",
        str(policy.ports.ui),
    )
