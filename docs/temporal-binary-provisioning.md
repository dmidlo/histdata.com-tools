# Temporal Binary Provisioning Design

Status: accepted for the V1.0 polish block. Runtime resolver and cache behavior
were implemented in #250, and release verification is tracked by #251.

## Problem

`histdatacom` now uses the local Temporal runtime as the production
orchestration layer for CLI and Python API work. The Python package can ship the
Temporal Python SDK through normal dependencies, but the Temporal CLI/server
binary is large enough that bundled platform wheels are not a good default PyPI
strategy. The macOS platform wheel has already crossed the practical 100 MB
PyPI/TestPyPI upload limit.

The package needs a finished V1.0 install story:

- `pip install histdatacom` should install a lean, normal Python package.
- first use should locate or provision the pinned Temporal binary without
  requiring users to understand packaging internals.
- checksums, version pins, cache behavior, and offline failure modes should be
  explicit and testable.
- future GUI packaging should be able to reuse the same resolver and telemetry.

## Decision Summary

Normal PyPI and TestPyPI artifacts will be metadata-only:

- source distribution
- universal Python wheel
- package metadata, entry points, runtime defaults, third-party notices, and a
  packaged Temporal artifact index

Normal PyPI and TestPyPI artifacts will not embed the Temporal executable.
Bundled executable wheels remain an explicit offline/private distribution path
only. They should not be uploaded to the normal PyPI project unless the project
has a confirmed larger file limit and the release operator deliberately opts in.

The default runtime resolver provisions the Temporal binary from a packaged
artifact index on first use, verifies the archive checksum before use, extracts
the executable into a per-user cache, and reuses only cache entries that match
the pinned version, platform, and checksum.

## Artifact Index

The artifact index is the runtime equivalent of the repository `.repo` file: a
small, versioned, machine-readable catalog that describes the remote artifacts
the package is allowed to fetch.

The index lives as package data under:

```text
src/histdatacom/orchestration/assets/temporal-runtime-index.json
```

That file is intentionally package-owned, not user-owned. It pins the binaries
that a given `histdatacom` release knows how to run. Runtime cache state belongs
outside the installed package.

Minimum schema:

```json
{
  "schema_version": 1,
  "component": "temporal-cli",
  "version": "1.7.2",
  "release_base_url": "https://github.com/temporalio/cli/releases/download/v1.7.2",
  "platforms": {
    "macos-arm64": {
      "system": "macos",
      "machine": "arm64",
      "archive_name": "temporal_cli_1.7.2_darwin_arm64.tar.gz",
      "archive_format": "tar.gz",
      "archive_url": "https://github.com/temporalio/cli/releases/download/v1.7.2/temporal_cli_1.7.2_darwin_arm64.tar.gz",
      "archive_sha256": "561ac68bdb6c16c8e8cbbd49f12578218ff1776007c3f3ae0d0196c8c9a73e79",
      "archive_size_bytes": 0,
      "executable_name": "temporal",
      "license": "MIT",
      "upstream_repository": "https://github.com/temporalio/cli"
    }
  }
}
```

`archive_size_bytes` is required in the production index. A zero value is valid
only in documentation examples. The resolver should validate that production
index entries define positive sizes for supported platforms so release tooling
can report expected download and cache impact.

The current index mirrors the existing checksum table in
`scripts/fetch_temporal_cli.py`. A regression test proves they describe the same
version, platforms, URLs, executable names, checksums, and archive shapes.

## Resolver Order

The resolver should return a structured result with the executable path,
provenance source, version, platform key, checksum information, and whether a
network fetch was required.

Resolution order:

1. explicit executable supplied by CLI/API, kept for development, CI, and
   operator overrides.
2. explicit executable from `HISTDATACOM_TEMPORAL_EXECUTABLE`.
3. bundled executable from an offline/private wheel, when the manifest declares
   it and the bundled provenance matches the pinned index.
4. verified cache entry matching the packaged artifact index.
5. first-run download from the packaged artifact index when network provisioning
   is allowed.
6. actionable failure explaining how to install, cache, or explicitly pass a
   Temporal executable.

Normal CLI/API startup should use this resolver. Direct process controls should
not require end users to know whether the binary came from an explicit path,
private bundle, or first-run cache.

## Cache Policy

The Temporal binary cache is shared per user and per platform, not per
workspace. Workspace-specific SQLite state, logs, PID files, and status
manifests remain under the existing runtime-home policy.

Default cache roots:

- macOS: `~/Library/Caches/histdatacom/temporal-cli`
- Linux: `$XDG_CACHE_HOME/histdatacom/temporal-cli`, or
  `~/.cache/histdatacom/temporal-cli`
- Windows: `%LOCALAPPDATA%\histdatacom\Cache\temporal-cli`

Environment override:

```text
HISTDATACOM_TEMPORAL_CACHE_DIR=/path/to/cache
```

Suggested layout:

```text
<cache-root>/
  temporal-cli/
    v1.7.2/
      macos-arm64/
        561ac68b.../
          archive/temporal_cli_1.7.2_darwin_arm64.tar.gz
          bin/temporal
          provenance.json
          .lock
```

The checksum is part of the path so cache invalidation is natural. Updating the
pinned Temporal version or checksum creates a new cache entry rather than
mutating an old one.

## First-Run Download Behavior

Download flow:

1. load the packaged artifact index.
2. select the entry for `current_platform_key()`.
3. acquire a platform/version/checksum lock.
4. re-check the cache after the lock is acquired.
5. download to a temporary file in the cache filesystem.
6. verify the archive SHA-256 before extraction.
7. extract only the expected executable name.
8. set executable permissions on Unix-like platforms.
9. write `provenance.json`.
10. atomically move the completed cache directory into place.

The resolver must reject corrupt, partial, wrong-platform, or checksum-mismatched
artifacts. It should never execute an unverified binary.

## Offline and Airgapped Behavior

Offline mode should be explicit:

```text
HISTDATACOM_TEMPORAL_OFFLINE=1
```

When offline mode is set, the resolver must not attempt a network request. It
may use an explicit executable, a bundled private/offline wheel executable, or a
verified cache entry. If none exists, it should fail with instructions for one
of these operator actions:

- run the provisioning command once on a connected machine and copy the cache
  directory.
- install a private/offline wheel that includes the verified executable.
- pass an explicit executable path.

CLI cache inspection and pre-seeding commands should expose the resolver
primitives, for example:

```text
histdatacom runtime doctor --json
histdatacom runtime provision --platform macos-arm64 --version 1.7.2
histdatacom runtime cache list --json
histdatacom runtime cache prune --keep-current
```

The command names can be finalized in #246. The resolver and cache primitives
are available through the package runtime surface.

## Security and Integrity

Required controls:

- HTTPS artifact URLs pinned by the packaged index.
- SHA-256 archive verification before extraction.
- archive member validation so extraction cannot write outside the target
  directory.
- single expected executable per archive.
- Unix executable-bit repair after extraction.
- file locking around downloads and extraction.
- atomic cache population so interrupted downloads do not look valid.
- structured provenance written beside the cached executable.
- no silent fallback to arbitrary `temporal` on `PATH`.

The explicit executable override is trusted operator input. It should still be
reported as an override in telemetry so support diagnostics can distinguish
package-provisioned runtimes from manually supplied binaries.

## Release Ownership

Release updates should follow this order:

1. update the pinned Temporal CLI version and checksum metadata.
2. regenerate or validate the packaged artifact index.
3. run unit tests that prove the fetch script and packaged index agree.
4. build the normal metadata-only wheel and sdist.
5. enforce the distribution size gate before upload.
6. smoke install the built wheel into a clean environment.
7. exercise resolver behavior using an isolated cache:
   - cache miss download
   - cache hit reuse
   - checksum failure fixture
   - offline failure without cache
   - explicit executable override

Bundled executable wheels remain a separate operator target. They should be
named and documented as offline/private artifacts, not the default PyPI path.

## Impact on Follow-Up Issues

#250 implemented:

- artifact index loader and validation
- cache directory policy
- resolver result model
- file locking and atomic cache writes
- verified download/extract flow
- explicit executable and offline behavior
- tests for cache hit, cache miss, checksum failure, offline failure, and
  explicit executable override

#251 should implement:

- release preflight for metadata-only normal wheels
- wheel size gates that fail before upload
- installed-wheel smoke that provisions or locates the runtime through the
  resolver
- TestPyPI parity checks for the non-bundled install path
- documentation for keyring, cache, and network prerequisites

This design kept #249 intentionally scoped to architecture and metadata. Runtime
behavior landed in #250, and release hardening belongs in #251.
