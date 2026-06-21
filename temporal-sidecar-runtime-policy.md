# Temporal Sidecar Runtime Policy

This low-level policy is summarized for users and operators in
`docs/temporal-sidecar-operations.md`.

HistDataCom runs the Temporal developer server as a local sidecar. Runtime
state is intentionally separate from HistData download and cache data.

## Runtime Home

The default per-user runtime home is platform-specific:

- macOS: `~/Library/Application Support/histdatacom/sidecar`
- Linux: `$XDG_STATE_HOME/histdatacom/sidecar`, or
  `~/.local/state/histdatacom/sidecar`
- Windows: `%LOCALAPPDATA%\histdatacom\sidecar`, or
  `~/AppData/Local/histdatacom/sidecar`

`HISTDATACOM_SIDECAR_HOME` or `histdatacom sidecar --runtime-home` can
override this base directory.

## Workspace Scoping

Each workspace receives a deterministic runtime directory under:

`<runtime-home>/workspaces/<workspace-name>-<workspace-hash>/`

The workspace defaults to the launch directory, but callers should pass
`--workspace` or set `HISTDATACOM_SIDECAR_WORKSPACE` when launching from a GUI,
service manager, or any context where the shell working directory is ambiguous.

Each workspace runtime directory contains:

- `state/sidecar.pid.json`
- `state/sidecar.lock`
- `logs/temporal-server.log`
- `logs/temporal-worker.log`
- `sqlite/temporal.db`
- `manifests/runtime-policy.json`

Downloaded ZIP, CSV, and cache files remain under the existing HistData data
directory policy. They are not stored in the sidecar runtime directory.

## Ports

The sidecar binds to `127.0.0.1` by default. Override with
`HISTDATACOM_SIDECAR_IP` when needed.

The default gRPC port is selected deterministically from the workspace hash in
the `17233-19232` range. The UI port is the selected gRPC port plus `1000`.
When a derived port pair is unavailable, the allocator scans forward through a
bounded deterministic window and records any collisions in the runtime policy.

Use `HISTDATACOM_SIDECAR_PORT` and optionally
`HISTDATACOM_SIDECAR_UI_PORT` for explicit ports. Explicit port collisions fail
with a clear error instead of silently selecting a different port.
