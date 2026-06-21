# Release Process

`histdatacom` is published on PyPI. Keep release changes conservative and
validate both source distributions and wheels before publishing.

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

Package metadata is declared in `pyproject.toml`. Local release environments
must use `setuptools>=77` so the built artifacts include current SPDX license
metadata.

The upload commands still use local `.pypirc` credentials and GPG detached
signatures, matching the historical release process.

## GitHub Actions

The CI workflow builds and tests the package on pull requests, pushes to
`main`, and manual dispatches.

CI also runs `actionlint` against every workflow. The same workflow lint is
available locally through pre-commit, so workflow syntax and common GitHub
Actions mistakes are checked before push.

Workflow actions are pinned to current supported releases:

- `actions/checkout@v7.0.0`
- `actions/setup-python@v6.2.0`
- `actions/upload-artifact@v7.0.1`
- `actions/download-artifact@v8.0.1`
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
does not publish automatically on tag push. Publishing to PyPI through GitHub
Actions requires a manual workflow dispatch with `publish_to_pypi` enabled.

Before enabling GitHub Actions publishing, configure PyPI Trusted Publishing for
this project:

- PyPI project: `histdatacom`
- GitHub owner/repository: `dmidlo/histdata.com-tools`
- Workflow filename: `release.yml`
- Environment name: `pypi`

The `pypi` environment should require manual approval in GitHub before publish
jobs run.
