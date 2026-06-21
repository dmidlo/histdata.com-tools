# HistDataCom Temporal Sidecar Assets

This package-data directory is the stable PyPI payload surface for the
Temporal sidecar migration.

The current migration slice ships metadata and runtime defaults only. Platform
wheels will later replace the planned manifest entries with bundled Temporal
executables. Source distributions and pure-Python wheels must fail sidecar
executable lookup with a clear message instead of silently falling back to an
unmanaged system binary.
