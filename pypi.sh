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
    local fetch_report="${HISTDATACOM_FETCH_REPORT:-}"
    local platform_key="${HISTDATACOM_SIDECAR_PLATFORM:-}"
    local report="dist/sidecar-platform-wheel-build-report.json"
    local wheel

    if [[ -z "${executable}" ]]; then
        echo "Set HISTDATACOM_SIDECAR_EXECUTABLE to build a bundled sidecar wheel." >&2
        exit 2
    fi

    if [[ -z "${fetch_report}" ]]; then
        echo "Set HISTDATACOM_FETCH_REPORT to the fetch_temporal_cli.py JSON report for the bundled executable." >&2
        exit 2
    fi

    if [[ -z "${platform_key}" ]]; then
        platform_key=$(current_sidecar_platform)
    fi

    python scripts/sidecar_platform_wheel.py \
        --platform-key "${platform_key}" \
        --executable "${executable}" \
        --fetch-report "${fetch_report}" \
        --check-version \
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

    if [[ "${HISTDATACOM_SKIP_GPG_SIGNING:-}" == "1" ]]; then
        echo "pypi.sh: skipping GPG signing because HISTDATACOM_SKIP_GPG_SIGNING=1." >&2
        return
    fi

    gpg --detach-sign --armor "${artifacts[@]}"
}

upload_dist_artifacts()
{
    local repository="$1"
    local artifacts=(dist/*.whl dist/*.tar.gz)
    local signatures=(dist/*.asc)

    validate_dist_artifact_sizes

    if [[ -e "${signatures[0]}" ]]; then
        artifacts+=("${signatures[@]}")
    fi

    python -m twine upload -r "${repository}" --config-file .pypirc "${artifacts[@]}"
}

validate_dist_artifact_sizes()
{
    local max_bytes="${HISTDATACOM_MAX_UPLOAD_FILE_BYTES:-100000000}"
    local allow_oversize="${HISTDATACOM_ALLOW_OVERSIZE_UPLOAD:-}"
    local oversized=()
    local artifact
    local size

    if ! [[ "${max_bytes}" =~ ^[0-9]+$ ]]; then
        echo "HISTDATACOM_MAX_UPLOAD_FILE_BYTES must be an integer byte count: ${max_bytes}" >&2
        exit 2
    fi

    for artifact in dist/*.whl dist/*.tar.gz; do
        [[ -e "${artifact}" ]] || continue
        size=$(python - "${artifact}" <<'PY'
import sys
from pathlib import Path

print(Path(sys.argv[1]).stat().st_size)
PY
)
        if (( size > max_bytes )); then
            oversized+=("${artifact}:${size}")
        fi
    done

    if ((${#oversized[@]} == 0)); then
        return
    fi

    {
        echo "pypi.sh: one or more distribution files exceed ${max_bytes} bytes."
        printf '  %s\n' "${oversized[@]}"
    } >&2

    if [[ "${allow_oversize}" == "1" ]]; then
        echo "pypi.sh: continuing because HISTDATACOM_ALLOW_OVERSIZE_UPLOAD=1." >&2
        return
    fi

    echo "Set HISTDATACOM_ALLOW_OVERSIZE_UPLOAD=1 only after confirming the PyPI/TestPyPI project upload limit has been raised." >&2
    echo "Alternatively set HISTDATACOM_MAX_UPLOAD_FILE_BYTES to the confirmed project-specific limit." >&2
    exit 2
}

current_git_branch()
{
    git rev-parse --abbrev-ref HEAD
}

current_package_version()
{
    python - <<'PY'
import histdatacom

print(histdatacom.__version__)
PY
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
    python -m twine check dist/*.whl dist/*.tar.gz
    validate_dist_artifact_sizes
    inspect_wheel_metadata
    smoke_wheel_install
}

verify_release_install()
{
    local index_url="$1"
    local report="${2:-}"
    local report_args=()

    if [[ -n "${report}" ]]; then
        report_args=(--report "${report}")
    fi

    python scripts/verify_testpypi_install.py \
        --version "$(current_package_version)" \
        --index-url "${index_url}" \
        "${report_args[@]}" \
        --require-external-runtime-provisioning \
        --check-executable-version \
        --start-sidecar \
        --hermetic-sidecar-smoke \
        --default-routing-sidecar-smoke \
        --quality-sidecar-smoke \
        --live-stop-timeout 90 \
        --download-smoke
}

testpypi_preflight()
{
    local local_index
    local local_index_url

    build
    local_index=$(mktemp -d)

    (
        trap 'rm -rf "${local_index}"' EXIT

        python scripts/build_local_simple_index.py \
            --dist-dir dist \
            --output-root "${local_index}" \
            --report "dist/local-simple-index-report.json"
        local_index_url="file://${local_index}/simple/"

        echo "${bold}verifying histdatacom from local TestPyPI-style index: ${local_index_url}${normal}"
        verify_release_install \
            "${local_index_url}" \
            "dist/testpypi-preflight-report.json"
    )
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
        upload_dist_artifacts pypi
        ;;
    testpypi)
        prepare_release_upload "TestPyPI" "${testpypi_branch}"
        build
        sign_dist_artifacts
        upload_dist_artifacts testpypi
        ;;
    testpypi_preflight)
        testpypi_preflight
        ;;
    testpypi_install)
        echo "${bold}verifying histdatacom from testpypi: https://test.pypi.org/simple/${normal}"
        verify_release_install "https://test.pypi.org/simple/"
        ;;
    pypi_install)
        buildenv
        echo "${bold}installing histdatacom from pypi: https://pypi.org/${normal}"
        python -m pip install histdatacom
        histdatacom_test
        destroyenv
        ;;
    *)
        echo "Usage: $0 {dev|build|sidecar_wheel|pypi|testpypi|testpypi_preflight|testpypi_install|pypi_install}" >&2
        exit 2
        ;;
esac
