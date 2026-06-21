# Release Process

`histdatacom` is published on PyPI. Keep release changes conservative and
validate both source distributions and wheels before publishing.

## Release Decision Tree

Use GitHub Actions for release artifact builds and provenance. Use the local
publisher only as a fallback while Trusted Publishing is being verified or if
GitHub Actions is unavailable.

1. Build and validate only: run the `Release` workflow manually with
   `release_target=build-only`, or push a `v*` tag. This builds, validates,
   attests, and uploads artifacts. It does not publish.
2. TestPyPI dry run: configure the TestPyPI Trusted Publisher, then run the
   `Release` workflow manually with `release_target=testpypi`. Approve the
   `testpypi` environment deployment, then verify install behavior from
   TestPyPI.
3. PyPI production release: configure the PyPI Trusted Publisher, confirm the
   same version passed TestPyPI, then run the `Release` workflow manually with
   `release_target=pypi` and `testpypi_dry_run_confirmed=true`. Approve the
   `pypi` environment deployment.
4. Local fallback: run `bash pypi.sh build`, then `bash pypi.sh testpypi` or
   `bash pypi.sh pypi` only when the GitHub Trusted Publishing path is not
   available.

Tag pushes are intentionally build-only. Production publishing is reachable
only from manual `workflow_dispatch`, the `pypi` environment approval gate, and
the explicit TestPyPI dry-run confirmation input.

## Trusted Publishing Configuration

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

The existing local workflow remains available:

```sh
bash pypi.sh build
bash pypi.sh testpypi
bash pypi.sh pypi
```

`pypi.sh build` now uses the PEP 517 build frontend:

```sh
python -m build
python -m twine check dist/*
```

It also inspects the built wheel metadata directly and installs the wheel into
a fresh virtual environment before any upload command can run. Legacy
`setup.py` commands are intentionally unsupported; this project is built from
`pyproject.toml`.

Package metadata is declared in `pyproject.toml`. Local release environments
must use `setuptools>=77` so the built artifacts include current SPDX license
metadata.

The upload commands still use local `.pypirc` credentials and GPG detached
signatures, matching the historical release process. Prefer the GitHub Trusted
Publishing path once the PyPI and TestPyPI publishers are verified.

After a TestPyPI dry run, install from TestPyPI in a disposable environment:

```sh
bash pypi.sh testpypi_install
```

After a production publish, verify the PyPI install path:

```sh
bash pypi.sh pypi_install
```

## GitHub Actions

The CI workflow builds and tests the package on pull requests, pushes to
`main`, and manual dispatches.

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
does not publish automatically on tag push. Publishing to TestPyPI or PyPI
requires a manual workflow dispatch, a matching protected environment approval,
and OIDC Trusted Publishing configured on the target index.

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
JSON report. CodeQL analyzes the Python source with no build step; it only has
`security-events: write` so GitHub can receive code-scanning results.

The package metadata intentionally separates dependency surfaces:

- Runtime dependencies support ordinary `pip install histdatacom` users and use
  lower bounds instead of lock-file pins so downstream applications can resolve
  compatible environments.
- Optional integrations live behind extras such as `histdatacom[pandas]`,
  `histdatacom[arrow]`, `histdatacom[influx]`, and `histdatacom[jupyter]`.
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
  release notes.
- Development and build findings block contributor or release hygiene but do
  not automatically require a PyPI release unless the vulnerable package is
  included in built distributions or runtime metadata.
- Transitive findings should be fixed by raising the direct dependency lower
  bound when possible. Avoid pinning runtime dependencies more tightly than
  needed for a library package.
- If no fixed version exists, keep the finding open, document the exposure and
  mitigation in the tracking issue, and avoid publishing a release that expands
  the affected install surface.
