#!/usr/bin/env bash

set -euo pipefail

bold=$(tput bold 2>/dev/null || true)
normal=$(tput sgr0 2>/dev/null || true)
project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
build_dependencies=(
    build==1.5.0
    setuptools==80.10.2
    twine==6.2.0
    wheel==0.47.0
)

cd "${project_root}"

install_build_frontend()
{
    python -m pip install "${build_dependencies[@]}"
}

dev()
{
    echo "${bold}pypi.sh: Setting Up Dev${normal}"
    python -m pip uninstall -y histdatacom
    install_build_frontend
    python -m pip install -e ".[dev]"
    pre-commit install
    echo "${bold}pypi.sh: Dev Ready.${normal}"
}

inspect_wheel_metadata()
{
    python - <<'PY'
from email.parser import Parser
from pathlib import Path
from zipfile import ZipFile

wheels = sorted(Path("dist").glob("histdatacom-*.whl"))
if len(wheels) != 1:
    raise SystemExit(f"expected exactly one wheel, found {wheels}")

with ZipFile(wheels[0]) as wheel:
    metadata_paths = [
        name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")
    ]
    entry_point_paths = [
        name for name in wheel.namelist() if name.endswith(".dist-info/entry_points.txt")
    ]
    if len(metadata_paths) != 1:
        raise SystemExit(f"expected one METADATA file, found {metadata_paths}")
    if len(entry_point_paths) != 1:
        raise SystemExit(f"expected one entry_points.txt file, found {entry_point_paths}")

    wheel_metadata = Parser().parsestr(wheel.read(metadata_paths[0]).decode("utf-8"))
    entry_points = wheel.read(entry_point_paths[0]).decode("utf-8")

if wheel_metadata["Name"] != "histdatacom":
    raise SystemExit(f"unexpected wheel name: {wheel_metadata['Name']}")
if wheel_metadata["Requires-Python"] != ">=3.10.0":
    raise SystemExit(
        f"unexpected Python requirement: {wheel_metadata['Requires-Python']}"
    )
if "histdatacom = histdatacom.histdata_com:main" not in entry_points:
    raise SystemExit("histdatacom console script missing from wheel metadata")
PY
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

if metadata.version("histdatacom") != histdatacom.__version__:
    raise SystemExit(
        "installed package metadata version does not match imported package"
    )
PY
        histdatacom --version
    )
}

build()
{
    rm -rf ./dist
    install_build_frontend
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
        gpg --detach-sign -a dist/*.tar.gz
        python -m twine upload -r pypi --config-file .pypirc dist/*.whl dist/*.tar.gz dist/*.asc
        ;;
    testpypi)
        build
        gpg --detach-sign -a dist/*.tar.gz
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
