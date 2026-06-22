# HistDataCom Temporal Sidecar Assets

This package-data directory is the stable PyPI payload surface for the
Temporal sidecar migration.

The source tree ships metadata, runtime defaults, and third-party notice
material only. Release builds create platform wheels by staging an explicit
Temporal executable with `scripts/sidecar_platform_wheel.py`. That staged wheel
patches `manifest.json`, writes `temporal-cli-provenance.json`, includes the
executable under `assets/bin/<platform>/`, and retags the artifact with the
matching platform wheel tag.

Bundled platform wheel provenance records the Temporal CLI version, upstream
release asset name and URL, expected and verified archive SHA-256 digests, the
packaged executable path, and the packaged executable SHA-256 digest. The
Temporal CLI notice and MIT license are stored under
`third-party/temporal-cli/`.

Source distributions, unsupported platforms, and universal fallback wheels
must fail sidecar executable lookup with a clear message instead of silently
falling back to an unmanaged system binary.
