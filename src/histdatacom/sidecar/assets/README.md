# HistDataCom Temporal Sidecar Assets

This package-data directory is the stable PyPI payload surface for the
Temporal sidecar migration.

The source tree ships metadata and runtime defaults only. Release builds create
platform wheels by staging an explicit Temporal executable with
`scripts/sidecar_platform_wheel.py`. That staged wheel patches
`manifest.json`, includes the executable under `assets/bin/<platform>/`, and
retags the artifact with the matching platform wheel tag.

Source distributions, unsupported platforms, and universal fallback wheels
must fail sidecar executable lookup with a clear message instead of silently
falling back to an unmanaged system binary.
