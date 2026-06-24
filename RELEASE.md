# Release Process

`histdatacom` is published on PyPI. Keep release changes conservative and
validate both source distributions and wheels before publishing.

## Release Decision Tree

Local publishing is the authoritative release path today. GitHub Actions
publishing is future architecture and remains guarded until the repository is
ready to move deployment out of the local maintainer workflow.

1. Build and validate locally from any reviewed branch with
   `bash pypi.sh build`.
2. Local TestPyPI-style preflight: run `bash pypi.sh testpypi_preflight`.
   This builds the artifacts, serves them through a local simple index, and
   verifies the installed package exactly like the TestPyPI install harness
   without requiring upload credentials.
3. TestPyPI dry run: check out `dev`, confirm the tree is clean, then run
   `bash pypi.sh testpypi`. Verify install behavior from TestPyPI before any
   production release.
4. PyPI production release: merge or fast-forward the reviewed release state to
   `main`, confirm the same version passed TestPyPI from `dev`, then run
   `bash pypi.sh pypi` from `main`.
5. Emergency branch overrides are explicit: set
   `HISTDATACOM_ALLOW_RELEASE_BRANCH_MISMATCH=1` only after reviewing why the
   normal `dev` -> TestPyPI and `main` -> PyPI branch policy cannot be used.

Tag pushes are intentionally build-only in the release workflow. The dormant
GitHub Actions PyPI publishing path is reachable only from manual
`workflow_dispatch`, the `pypi` environment approval gate, the `main` branch,
and the explicit TestPyPI dry-run confirmation input.

## Trusted Publishing Configuration

Trusted Publishing is the target GitHub Actions deployment model, not the
current publishing path. Keep the configuration notes below current so the
future migration is straightforward, but publish with `pypi.sh` until the
Actions release path is deliberately enabled.

The GitHub repository has protected `testpypi` and `pypi` environments with
required reviewer approval. Configure the matching Trusted Publishers in
TestPyPI and PyPI before using the publish jobs:

- PyPI/TestPyPI project: `histdatacom`
- GitHub owner/repository: `dmidlo/histdata.com-tools`
- Workflow filename: `release.yml`
- TestPyPI environment name: `testpypi`
- PyPI environment name: `pypi`

Trusted Publishing removes long-lived PyPI credentials from GitHub Actions.
The publish jobs request only job-scoped OIDC credentials with `id-token:
write`, and the build job remains separate from publish jobs.

## Provenance And Signatures

The release workflow generates GitHub artifact attestations for the built wheel
and source distribution before uploading them as workflow artifacts. The
Trusted Publishing upload action also publishes PyPI digital attestations by
default for PyPI and TestPyPI uploads.

Local GPG detached signatures remain part of the local fallback path for now.
`pypi.sh` signs both the wheel and source distribution before local upload. The
GitHub release path does not use local GPG keys; it relies on GitHub artifact
attestations and PyPI Trusted Publishing attestations.

## Local Publishing

The existing local workflow is the current publishing workflow:

```sh
bash pypi.sh build
bash pypi.sh testpypi_preflight
bash pypi.sh testpypi
bash pypi.sh pypi
```

`bash pypi.sh testpypi_preflight` is the local release preflight that does not
contact TestPyPI or require keyring credentials. It builds `dist/`, creates a
temporary local simple index from those artifacts, installs through
`scripts/verify_testpypi_install.py`, and runs the same bundled-sidecar,
CLI-parity, quality-smoke, default-routing, sidecar lifecycle, and live
download/extract checks used after a real TestPyPI upload. The preflight writes
`dist/local-simple-index-report.json` and `dist/testpypi-preflight-report.json`
for auditability.

`bash pypi.sh testpypi` is guarded to run from `dev` by default. Override the
branch name only for a deliberate repository policy change:

```sh
HISTDATACOM_TESTPYPI_BRANCH=release-candidate bash pypi.sh testpypi
```

`bash pypi.sh pypi` is guarded to run from `main` by default. Both upload
commands refuse to run with uncommitted tracked changes before building,
signing, or uploading artifacts.

Local uploads sign distributions by default. On a TestPyPI verification
machine without the release GPG secret key, set
`HISTDATACOM_SKIP_GPG_SIGNING=1` explicitly to upload unsigned TestPyPI
artifacts:

```sh
HISTDATACOM_SKIP_GPG_SIGNING=1 bash pypi.sh testpypi
```

Local uploads also preflight distribution file sizes before calling Twine.
PyPI and TestPyPI commonly default to a 100 MB per-file upload limit, and the
bundled Temporal sidecar wheels can exceed that default. If the project has a
confirmed raised limit, set `HISTDATACOM_ALLOW_OVERSIZE_UPLOAD=1`; if the
confirmed project-specific limit is different, set
`HISTDATACOM_MAX_UPLOAD_FILE_BYTES` to that byte count:

```sh
HISTDATACOM_ALLOW_OVERSIZE_UPLOAD=1 bash pypi.sh testpypi
HISTDATACOM_MAX_UPLOAD_FILE_BYTES=200000000 bash pypi.sh testpypi
```

`pypi.sh build` now uses the PEP 517 build frontend:

```sh
python -m build
python -m twine check dist/*.whl dist/*.tar.gz
```

It also inspects the built wheel metadata directly and installs the wheel into
a fresh virtual environment before any upload command can run. The smoke check
uses `scripts/smoke_sidecar_install.py`, which validates package metadata,
console entry points, packaged sidecar resources, current-platform manifest
support, and offline `histdatacom-sidecar status`/`doctor` behavior. Legacy
`setup.py` commands are intentionally unsupported; this project is built from
`pyproject.toml`.

Source distributions and universal fallback wheels are metadata-only sidecar
artifacts. They must declare every supported platform and fail explicitly when
a Temporal executable is requested from an artifact that does not bundle one.
Platform wheels are built from explicit Temporal executable artifacts and must
set the current platform manifest entry to `bundled: true`.

To build a platform wheel locally, provide the executable artifact and the
fetch report generated for that exact artifact:

```sh
venv/bin/python scripts/fetch_temporal_cli.py \
  --platform-key macos-arm64 \
  --version 1.7.2 \
  --download-dir .temporal-cli/macos-arm64/downloads \
  --output-dir .temporal-cli/macos-arm64/bin \
  --report .temporal-cli/macos-arm64/temporal-cli-report.json

HISTDATACOM_SIDECAR_EXECUTABLE=.temporal-cli/macos-arm64/bin/temporal \
HISTDATACOM_FETCH_REPORT=.temporal-cli/macos-arm64/temporal-cli-report.json \
bash pypi.sh build
```

Set `HISTDATACOM_SIDECAR_PLATFORM` when cross-building from a prepared
platform artifact, for example `linux-x86_64` or `windows-x86_64`. The helper
uses `scripts/sidecar_platform_wheel.py` to stage a temporary source tree,
patch `manifest.json`, include `assets/bin/<platform>/temporal`, and retag the
wheel to the manifest platform tag. The source tree remains metadata-only and
`src/histdatacom/sidecar/assets/bin/` is ignored by Git to prevent committing
oversized executable artifacts.

Bundled platform wheel smoke should install the built wheel directly:

```sh
python scripts/inspect_wheel.py \
  --wheel dist/histdatacom-*-py3-none-<platform-tag>.whl \
  --require-bundled-platform <platform-key>
python scripts/smoke_sidecar_install.py \
  --wheel dist/histdatacom-*-py3-none-<platform-tag>.whl \
  --require-bundled-current-platform \
  --check-executable-version \
  --expect-temporal-extra \
  --start-sidecar \
  --quality-sidecar-smoke
```

Sidecar runtime documentation must be reviewed for every release that changes
sidecar CLI flags, runtime paths, worker queues, package resources, or bundled
executable behavior:

- `README.md` for the user-facing sidecar compatibility summary.
- `docs/temporal-sidecar-operations.md` for lifecycle commands, state layout,
  logs, SQLite persistence, troubleshooting, cancellation/resume behavior, and
  GUI-oriented job control.
- `docs/temporal-workflow-topology.md` for workflow, activity, task queue, and
  testing boundaries.
- `docs/temporal-sidecar-performance.md` for lane sizing and benchmark policy.

Release notes should clearly state whether the published wheel is still
metadata-only or includes a bundled Temporal executable for each supported
platform. They should also call out the `histdatacom[temporal]` extra whenever
sidecar client or worker dependency behavior changes.

Package metadata is declared in `pyproject.toml`. Local release environments
must use `setuptools>=77` so the built artifacts include current SPDX license
metadata.

The upload commands use local `.pypirc` credentials and GPG detached
signatures, matching the historical release process. Move to the GitHub
Trusted Publishing path only after the branch, sidecar, runner, and approval
model is reviewed as a separate release-engineering change.

After a TestPyPI dry run, install from TestPyPI in a disposable environment:

```sh
bash pypi.sh testpypi_install
```

This runs `scripts/verify_testpypi_install.py`, which downloads the exact
`histdatacom` wheel from TestPyPI without dependencies, installs that artifact
into a fresh virtual environment with dependencies resolved from PyPI, and
checks parity against the current package surface: version metadata, console
entry points, sidecar assets, current CLI flags, sidecar lifecycle probes,
deterministic sidecar workflow smokes, quality-mode sidecar smokes, and a small
live download/extract smoke. The default local verification requires a bundled
wheel for the current platform so stale metadata-only TestPyPI artifacts are
rejected before production publishing.

After a production publish, verify the PyPI install path:

```sh
bash pypi.sh pypi_install
```

## GitHub Actions

The CI workflow builds and tests the package on pull requests, pushes to
`main`, and manual dispatches.

CI inspects the built wheel with `scripts/inspect_wheel.py`, writes
`dist/sidecar-wheel-report.json`, uploads that report with the distribution
artifacts, and then installs the built wheel on Ubuntu, macOS, and Windows.
Those platform smoke checks verify the sidecar resource manifest and the
offline sidecar CLI probes against the artifact that would be published. They
also run the installed `histdatacom --quality` command through the packaged
Temporal sidecar against clean and dirty local fixtures.

CI also runs `actionlint` against every workflow. The same workflow lint is
available locally through pre-commit, so workflow syntax and common GitHub
Actions mistakes are checked before push.

Workflow actions are pinned to current supported releases:

- `actions/checkout@v7.0.0`
- `actions/attest@v4.1.0`
- `actions/setup-python@v6.2.0`
- `actions/upload-artifact@v7.0.1`
- `actions/download-artifact@v8.0.1`
- `actions/dependency-review-action@v5.0.0`
- `github/codeql-action/init@v4.36.2`
- `github/codeql-action/analyze@v4.36.2`
- `pypa/gh-action-pypi-publish@v1.14.0`
- `rhysd/actionlint@v1.7.12`

The GitHub-maintained Node 24 actions require GitHub Actions Runner
`v2.327.1` or newer on self-hosted runners. The hosted Ubuntu, macOS, and
Windows runners used by this project satisfy that requirement. The checkout
v7 fork-safety defaults only affect `pull_request_target` and `workflow_run`
triggers; these workflows use `push`, `pull_request`, `workflow_dispatch`, and
tag push triggers. The artifact v4+ service is intended for GitHub.com hosted
workflows, which is the supported CI environment for this project.

Pull request CI uses workflow concurrency to cancel stale runs when a branch is
updated. Release workflow concurrency does not cancel in-progress runs, so a
manual publish cannot be interrupted by a later tag or dispatch event.

The release workflow builds artifacts on `v*` tags and manual dispatches. It
does not publish automatically on tag push. The dormant publish jobs are branch
guarded to match local policy: TestPyPI is only dispatchable from `dev`, and
PyPI is only dispatchable from `main`. Publishing through GitHub Actions also
requires a matching protected environment approval and OIDC Trusted Publishing
configured on the target index.

Release artifact provenance covers everything under `dist/`, including the
sidecar wheel inspection report. The release build runs the same sidecar wheel
smoke used by local `pypi.sh build` before attestations and artifact upload.

## Rollback And Yank Guidance

Published package files are immutable. If a release is bad, publish a fixed
patch release rather than trying to replace files for the same version.

Yank a release on PyPI when it is broken, uninstallable, violates compatibility
expectations, or contains a security vulnerability. Use the PyPI release
management page and provide a yank reason so downstream users can understand
the mitigation. Delete a release only for exceptional cases such as leaked
secrets or malicious content where leaving the files available is worse than
breaking historical installs.

## Dependency And Security Triage

Dependabot monitors GitHub Actions and Python packaging metadata weekly. Its
pull requests should run the full CI and security workflows before merge.

The security workflow runs on pull requests, pushes to `main`, manual dispatch,
and a weekly schedule. It has no publishing permissions. Dependency Review is
enabled for pull requests on this public repository and fails when dependency
changes introduce moderate, high, or critical vulnerabilities in runtime or
development scopes. `pip-audit` installs the package with all optional runtime
and integration extras, audits the resulting Python environment, and uploads a
JSON report. Before `pip-audit` runs, the workflow verifies that the `all`
extra installed the Temporal runtime dependency and that sidecar package
resources import correctly, so the new sidecar dependency surface is included
in the audit environment. CodeQL analyzes the Python source with no build step;
it only has `security-events: write` so GitHub can receive code-scanning
results.

The package metadata intentionally separates dependency surfaces:

- Runtime dependencies support ordinary `pip install histdatacom` users and use
  lower bounds instead of lock-file pins so downstream applications can resolve
  compatible environments.
- Optional integrations live behind extras such as `histdatacom[pandas]`,
  `histdatacom[arrow]`, `histdatacom[influx]`, `histdatacom[jupyter]`, and
  `histdatacom[temporal]`.
- `histdatacom[test]`, `histdatacom[lint]`, and `histdatacom[release]` split
  contributor tooling by purpose. `histdatacom[dev]` aggregates those direct
  tool pins plus optional integration dependencies for local development.
- This project does not keep a committed lock file for runtime dependencies
  because it is a published library package. For local reproducibility, create a
  fresh virtual environment, install `.[dev]`, and use the pinned pre-commit
  hook revisions and direct developer-tool pins.

For a published PyPI package, triage vulnerability findings by affected install
surface:

- Runtime dependencies affect ordinary `pip install histdatacom` users and
  should be patched with a minimum-version floor or compatible dependency range
  before publishing a patch release.
- Optional dependency findings affect users who install extras such as
  `histdatacom[pandas]`, `histdatacom[arrow]`, `histdatacom[influx]`, or
  `histdatacom[jupyter]`; patch those ranges and mention the affected extra in
  release notes. Treat `histdatacom[temporal]` the same way once sidecar
  runtime dependencies are involved in a finding.
- Development and build findings block contributor or release hygiene but do
  not automatically require a PyPI release unless the vulnerable package is
  included in built distributions or runtime metadata.
- Transitive findings should be fixed by raising the direct dependency lower
  bound when possible. Avoid pinning runtime dependencies more tightly than
  needed for a library package.
- If no fixed version exists, keep the finding open, document the exposure and
  mitigation in the tracking issue, and avoid publishing a release that expands
  the affected install surface.
