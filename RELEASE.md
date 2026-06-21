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

The upload commands still use local `.pypirc` credentials and GPG detached
signatures, matching the historical release process.

## GitHub Actions

The CI workflow builds and tests the package on pull requests, pushes to
`main`, and manual dispatches.

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
