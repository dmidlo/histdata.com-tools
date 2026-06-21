#!/usr/bin/env bash

set -euo pipefail

bold=$(tput bold 2>/dev/null || true)
normal=$(tput sgr0 2>/dev/null || true)
project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

cd "${project_root}"

install_release_frontend()
{
    python -m pip install -e ".[release]"
}

dev()
{
    echo "${bold}pypi.sh: Setting Up Dev${normal}"
    python -m pip uninstall -y histdatacom
    python -m pip install -e ".[dev]"
    pre-commit install
    echo "${bold}pypi.sh: Dev Ready.${normal}"
}

inspect_wheel_metadata()
{
    python scripts/inspect_wheel.py
}

smoke_wheel_install()
{
    local smoke_dir
    smoke_dir=$(mktemp -d)

    (
        trap 'rm -rf "${smoke_dir}"' EXIT

        python -m venv "${smoke_dir}/venv"
        # shellcheck source=/dev/null
        source "${smoke_dir}/venv/bin/activate"
        python -m pip install --upgrade pip
        python -m pip install dist/*.whl
        python - <<'PY'
from importlib import metadata

import histdatacom
from histdatacom.sidecar import (
    SidecarExecutableUnavailable,
    load_sidecar_manifest,
    sidecar_asset,
    sidecar_executable_path,
)

if metadata.version("histdatacom") != histdatacom.__version__:
    raise SystemExit(
        "installed package metadata version does not match imported package"
    )
if not sidecar_asset("manifest.json").is_file():
    raise SystemExit("sidecar manifest is not installed as package data")
if load_sidecar_manifest().sidecar != "temporal":
    raise SystemExit("sidecar manifest does not describe Temporal")
try:
    with sidecar_executable_path("linux-x86_64"):
        raise SystemExit("metadata-only wheel exposed executable")
except SidecarExecutableUnavailable as err:
    if "not bundled in this distribution" not in str(err):
        raise
PY
        histdatacom --version
    )
}

sign_dist_artifacts()
{
    local artifacts=(dist/*.whl dist/*.tar.gz)

    gpg --detach-sign --armor "${artifacts[@]}"
}

build()
{
    rm -rf ./dist
    install_release_frontend
    python -m build
    python -m twine check dist/*
    inspect_wheel_metadata
    smoke_wheel_install
}

buildenv()
{
    echo "${bold}setting up test pip environment${normal}"
    rm -rf "${project_root}/../myproject"
    mkdir "${project_root}/../myproject"
    cd "${project_root}/../myproject"
    pwd
    python -m venv venv
    echo "${bold}activating test pip environment${normal}"
    # shellcheck source=/dev/null
    source venv/bin/activate
    python -m pip install polars
    echo "${bold}test pip environment set up complete.${normal}"
}

destroyenv()
{
    cd "${project_root}"
    rm -rf "${project_root}/../myproject"
    echo "${bold}leaving test pip environment${normal}"
    # shellcheck source=/dev/null
    source "${project_root}/venv/bin/activate"
}

histdatacom_test()
{
    echo "${bold}testing histdatacom -h test pip environment${normal}"
    histdatacom -h
    echo "${bold}testing histdatacom -D test pip environment${normal}"
    histdatacom -p eurusd -f ascii -t tick-data-quotes -s now
    echo "${bold}testing histdatacom --version test pip environment${normal}"
    histdatacom --version
}

case "${1:-}" in
    dev)
        dev
        ;;
    build)
        build
        ;;
    pypi)
        build
        sign_dist_artifacts
        python -m twine upload -r pypi --config-file .pypirc dist/*.whl dist/*.tar.gz dist/*.asc
        ;;
    testpypi)
        build
        sign_dist_artifacts
        python -m twine upload -r testpypi --config-file .pypirc dist/*.whl dist/*.tar.gz dist/*.asc
        ;;
    testpypi_install)
        buildenv
        echo "${bold}installing histdatacom from testpypi: https://test.pypi.org/simple/${normal}"
        python -m pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ histdatacom
        histdatacom_test
        destroyenv
        ;;
    pypi_install)
        buildenv
        echo "${bold}installing histdatacom from pypi: https://pypi.org/${normal}"
        python -m pip install histdatacom
        histdatacom_test
        destroyenv
        ;;
    *)
        echo "Usage: $0 {dev|build|pypi|testpypi|testpypi_install|pypi_install}" >&2
        exit 2
        ;;
esac
