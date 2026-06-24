# HistDataCom Temporal Runtime Assets

This package-data directory is the stable PyPI payload surface for the
Temporal orchestration runtime.

The normal PyPI/TestPyPI source distribution and universal wheel ship metadata,
runtime defaults, third-party notice material, and the packaged Temporal
artifact index. They do not embed the Temporal executable.

The accepted V1.0 design is metadata-only wheel distribution plus verified
first-run runtime provisioning. The resolver implemented after this design
should read the packaged artifact index, download only pinned Temporal CLI
release artifacts, verify archive SHA-256 checksums before extraction, and cache
the executable outside the installed package.

Bundled executable wheels remain an explicit offline/private artifact path.
Those wheels stage an explicit Temporal executable with
`scripts/sidecar_platform_wheel.py`, patch `manifest.json`, write
`temporal-cli-provenance.json`, include the executable under
`assets/bin/<platform>/`, and retag the artifact with the matching platform
wheel tag.

Metadata-only artifacts and unsupported platforms must fail executable lookup
with a clear message until the runtime resolver can provision a verified cache
entry. They must not silently fall back to an unmanaged system binary.
