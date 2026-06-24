#!/usr/bin/env bash

set -euo pipefail

bold=$(tput bold 2>/dev/null || true)
normal=$(tput sgr0 2>/dev/null || true)
project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
testpypi_branch="${HISTDATACOM_TESTPYPI_BRANCH:-dev}"
pypi_branch="${HISTDATACOM_PYPI_BRANCH:-main}"

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
    local wheel
    local wheels=(dist/histdatacom-*.whl)

    if ((${#wheels[@]} == 0)); then
        echo "pypi.sh: no wheels found in dist" >&2
        exit 1
    fi

    for wheel in "${wheels[@]}"; do
        python scripts/inspect_wheel.py \
            --wheel "${wheel}" \
            --report "dist/$(basename "${wheel%.whl}")-sidecar-wheel-report.json"
    done
}

smoke_wheel_install()
{
    local wheel
    local wheels=(dist/histdatacom-*.whl)

    if ((${#wheels[@]} == 0)); then
        echo "pypi.sh: no wheels found in dist" >&2
        exit 1
    fi

    for wheel in "${wheels[@]}"; do
        local smoke_dir
        smoke_dir=$(mktemp -d)

        (
            trap 'rm -rf "${smoke_dir}"' EXIT

            python -m venv "${smoke_dir}/venv"
            # shellcheck source=/dev/null
            source "${smoke_dir}/venv/bin/activate"
            python -m pip install --upgrade pip
            python "${project_root}/scripts/smoke_sidecar_install.py" \
                --wheel "${project_root}/${wheel}" \
                --state-dir "${smoke_dir}/sidecar-state"
        )
    done
}

current_sidecar_platform()
{
    python - <<'PY'
from histdatacom.sidecar.resources import current_platform_key

print(current_platform_key())
PY
}

sidecar_platform_wheel()
{
    local executable="${HISTDATACOM_SIDECAR_EXECUTABLE:-}"
    local platform_key="${HISTDATACOM_SIDECAR_PLATFORM:-}"
    local report="dist/sidecar-platform-wheel-build-report.json"
    local wheel

    if [[ -z "${executable}" ]]; then
        echo "Set HISTDATACOM_SIDECAR_EXECUTABLE to build a bundled sidecar wheel." >&2
        exit 2
    fi

    if [[ -z "${platform_key}" ]]; then
        platform_key=$(current_sidecar_platform)
    fi

    python scripts/sidecar_platform_wheel.py \
        --platform-key "${platform_key}" \
        --executable "${executable}" \
        --dist-dir dist \
        --report "${report}"

    wheel=$(python - "${report}" <<'PY'
import json
import sys
from pathlib import Path

print(Path(json.loads(Path(sys.argv[1]).read_text())["wheel"]))
PY
)
    python scripts/inspect_wheel.py \
        --wheel "${wheel}" \
        --require-bundled-platform "${platform_key}" \
        --report "dist/$(basename "${wheel%.whl}")-sidecar-wheel-report.json"
}

sign_dist_artifacts()
{
    local artifacts=(dist/*.whl dist/*.tar.gz)

    gpg --detach-sign --armor "${artifacts[@]}"
}

current_git_branch()
{
    git rev-parse --abbrev-ref HEAD
}

require_clean_release_tree()
{
    if ! git diff --quiet || ! git diff --cached --quiet; then
        echo "pypi.sh: refusing release upload with uncommitted tracked changes." >&2
        echo "Commit or stash changes before publishing." >&2
        exit 2
    fi
}

require_release_branch()
{
    local target="$1"
    local expected_branch="$2"
    local branch

    branch=$(current_git_branch)

    if [[ "${branch}" == "HEAD" ]]; then
        echo "pypi.sh: refusing ${target} upload from detached HEAD." >&2
        echo "Check out ${expected_branch} before publishing." >&2
        exit 2
    fi

    if [[ "${branch}" != "${expected_branch}" ]]; then
        if [[ "${HISTDATACOM_ALLOW_RELEASE_BRANCH_MISMATCH:-}" == "1" ]]; then
            echo "pypi.sh: overriding ${target} branch guard from ${branch}; expected ${expected_branch}." >&2
            return
        fi

        echo "pypi.sh: refusing ${target} upload from ${branch}; expected ${expected_branch}." >&2
        echo "Set HISTDATACOM_ALLOW_RELEASE_BRANCH_MISMATCH=1 only for an explicitly reviewed emergency." >&2
        exit 2
    fi
}

prepare_release_upload()
{
    local target="$1"
    local expected_branch="$2"

    require_release_branch "${target}" "${expected_branch}"
    require_clean_release_tree
}

build()
{
    rm -rf ./dist
    install_release_frontend
    python -m build
    if [[ -n "${HISTDATACOM_SIDECAR_EXECUTABLE:-}" ]]; then
        sidecar_platform_wheel
    fi
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
    (
        local sidecar_state

        sidecar_state=$(mktemp -d)
        trap 'rm -rf "${sidecar_state}"' EXIT

        echo "${bold}testing histdatacom -h test pip environment${normal}"
        histdatacom -h
        echo "${bold}testing histdatacom -D test pip environment${normal}"
        histdatacom -p eurusd -f ascii -t tick-data-quotes -s now
        echo "${bold}testing histdatacom --version test pip environment${normal}"
        histdatacom --version
        echo "${bold}testing histdatacom-sidecar status test pip environment${normal}"
        histdatacom-sidecar --state-dir "${sidecar_state}" --json status >/dev/null
        echo "${bold}testing histdatacom-sidecar doctor test pip environment${normal}"
        histdatacom-sidecar --state-dir "${sidecar_state}" --json doctor >/dev/null
        echo "${bold}testing histdatacom-sidecar-worker help test pip environment${normal}"
        histdatacom-sidecar-worker --help >/dev/null
    )
}

case "${1:-}" in
    dev)
        dev
        ;;
    build)
        build
        ;;
    sidecar_wheel)
        install_release_frontend
        sidecar_platform_wheel
        ;;
    pypi)
        prepare_release_upload "PyPI" "${pypi_branch}"
        build
        sign_dist_artifacts
        python -m twine upload -r pypi --config-file .pypirc dist/*.whl dist/*.tar.gz dist/*.asc
        ;;
    testpypi)
        prepare_release_upload "TestPyPI" "${testpypi_branch}"
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
        echo "Usage: $0 {dev|build|sidecar_wheel|pypi|testpypi|testpypi_install|pypi_install}" >&2
        exit 2
        ;;
esac
