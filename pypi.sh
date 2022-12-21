bold=$(tput bold)
normal=$(tput sgr0)

dev()
{
    echo "${bold}pypi.sh: Setting Up Dev${normal}"
    pip uninstall -y histdatacom
    pip install twine wheel
    pip install git+https://github.com/h2oai/datatable
    pip install -e .[dev]
    pre-commit install
    pre-commit install --hook-type commit-msg --hook-type pre-push
    pre-commit autoupdate
    echo "${bold}pypi.sh: Dev Ready.${normal}"
}

build()
{
    rm -rf ./dist
    pip install twine wheel
    python setup.py check
    python setup.py sdist
    python setup.py bdist_wheel --universal
}

buildenv()
{
    echo "${bold}setting up test pip environment${normal}"
    rm -rf ../myproject
    mkdir ../myproject
    cd ../myproject
    pwd
    python -m venv venv
    echo "${bold}activating test pip environment${normal}"
    source venv/bin/activate
    pip install git+https://github.com/h2oai/datatable
    echo "${bold}test pip environment set up complete.${normal}"
}

destroyenv()
{
    cd ../histdata.com-tools
    rm -rf ../myproject
    echo "${bold}leaving test pip environment${normal}"
    source venv/bin/activate
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

if [[ $1 == "dev" ]]
then
    dev
    exit 0
elif [[ $1 == "build" ]]
then
    build
    exit 0
elif [[ $1 == "pypi" ]]
then
    build
    gpg --detach-sign -a dist/*.tar.gz
    twine upload -r pypi --config-file .pypirc dist/*.whl dist/*.tar.gz dist/*.asc
    exit 0
elif [[ $1 == "testpypi" ]]
then
    build
    gpg --detach-sign -a dist/*.tar.gz
    twine upload -r testpypi --config-file .pypirc dist/*.whl dist/*.tar.gz dist/*.asc
elif [[ $1 == "testpypi_install" ]]
then
    buildenv
    echo "${bold}installing histdatacom from testpypi: https://test.pypi.org/simple/${normal}"
    python3 -m pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ histdatacom
    histdatacom_test
    destroyenv
elif [[ $1 == "pypi_install" ]]
then
    buildenv
    echo "${bold}installing histdatacom from pypi: https://pypi.org/${normal}"
    pip install histdatacom
    histdatacom_test
    destroyenv
fi
