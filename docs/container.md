# Container image

The application image packages `histdatacom` as a one-shot command-line tool.
It is not an InfluxDB image, a separately managed Temporal service, or a
long-lived API server. Each `docker run` invocation performs one CLI operation
and then exits.

## Pull or build

Version tags publish matching Linux AMD64 and ARM64 images to GitHub Container
Registry:

```sh
docker pull ghcr.io/dmidlo/histdata.com-tools:2.1.0
```

Build the current checkout for the native platform with the strict repository
build context:

```sh
docker build --tag histdatacom:local .
```

The `.dockerignore` file is an allow-list. Only `pyproject.toml`, package source,
the reviewed container dependency constraints, the license, and the package
README enter the build context. Repository data, Git history, virtual
environments, generated reports, caches, and local credentials are excluded by
construction. The base image uses a multi-platform digest and
`container/constraints.txt` fixes the default image's Python build and runtime
dependency graph for reviewed release-tag rebuilds.

## Persistent workspace

Use one named volume at `/workspace` for downloaded data, cache files, runtime
state, logs, manifests, SQLite state, and the checksum-verified Temporal binary
cache:

```sh
docker volume create histdatacom-workspace

docker run --rm \
  --mount type=volume,source=histdatacom-workspace,target=/workspace \
  ghcr.io/dmidlo/histdata.com-tools:2.1.0 \
  --version

docker run --rm \
  --mount type=volume,source=histdatacom-workspace,target=/workspace \
  ghcr.io/dmidlo/histdata.com-tools:2.1.0 \
  -C -p eurusd -f ascii -t tick-data-quotes -s 2024-01
```

The image intentionally has no Dockerfile `VOLUME` declaration, so Docker does
not silently create anonymous storage. A named volume is recommended because a
new volume copies the image workspace ownership and initial directories. For a
host bind mount, make the host directory writable by container UID and GID
`10001:10001` before running the image. Do not solve a permission mismatch by
running the market-data process as root.

The default relative data directory is `/workspace/data`. Container environment
defaults also keep orchestration state in `/workspace/runtime` and the Temporal
cache in `/workspace/cache/temporal-cli`. Explicit CLI options and supported
environment overrides remain available when a different layout is required.

## Temporal lifecycle and networking

The normal image does not preload a platform binary. The first operation that
starts orchestration downloads the pinned Temporal CLI artifact over HTTPS,
verifies its archive checksum, records provenance, and saves it in the mounted
cache. Later containers using the same workspace volume reuse that verified
entry. Air-gapped use therefore requires a previously provisioned cache or the
project's private/offline bundled-wheel path.

Temporal server and worker processes are children of the one-shot operation.
They are started and stopped as part of a waited CLI job. `--keep-runtime` is
not useful across `docker run` boundaries because the container process exits;
durable job/runtime state remains in the workspace, but processes do not. The
image uses `tini` for signal forwarding and zombie reaping so `docker stop`
delivers a clean termination path.

No runtime ports are published by default. A normal waited job talks to its
container-local runtime. Interactive runtime diagnostics can be executed in a
single container, but treating that container as a production service is
outside the image contract.

The default image installs the base Python package dependency set. Optional
Python extras such as `models`, `influx`, `query`, and `jupyter` are not silently
added to this lean image. A purpose-specific image must extend the Dockerfile
and lock the selected extra dependencies explicitly.

## Verification

The local smoke builds the native image, inspects its runtime configuration and
OCI labels, verifies UID/GID and workspace writes, exercises CLI version/help,
and removes its temporary image:

```sh
python scripts/smoke_container.py
```

The connected smoke additionally provisions the real pinned Temporal binary,
runs `runtime start`, `runtime doctor`, and `runtime stop` within one container,
and proves the verified cache survives into a second container through a named
volume. It removes the temporary image and volume by default:

```sh
python scripts/smoke_container.py --deep-runtime
```

Use `--keep-image` or `--keep-workspace-volume` only when debugging retained
state. The smoke report names retained resources.

## Publication and cleanup

Container validation is isolated from ordinary `dev` pushes. The Container
workflow runs for pull requests that change the container surface, for manual
dispatch, and for `v*` tags. Pull-request and manual runs build without
publishing. A version tag publishes AMD64 and ARM64 manifests to GHCR with
SemVer and commit tags, OCI metadata, provenance, and an SBOM. The workflow does
not run coverage; coverage remains a `dev`-to-`main` promotion gate.

Remove local state explicitly when it is no longer required:

```sh
docker volume rm histdatacom-workspace
docker image rm ghcr.io/dmidlo/histdata.com-tools:2.1.0
```
