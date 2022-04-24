bold=$(tput bold)
normal=$(tput sgr0)

dev()
{
    echo "${bold}pypi.sh: Setting Up Dev${normal}"
    pip uninstall -y histdatacom
    python setup.py build
    python setup.py install
    python setup.py develop
    echo "${bold}pypi.sh: Dev Ready.${normal}"
}

build()
{
    rm -rf ./dist
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
    echo "${bold}test pip environment set up complete.${normal}"
}

destroyenv()
{
    echo "${bold}testing histdatacom -h test pip environment${normal}"
    histdatacom -h
    echo "${bold}testing histdatacom -D test pip environment${normal}"
    histdatacom -p eurusd -f ascii -t tick-data-quotes -s now
    cd ../histdata.com-tools
    rm -rf ../myproject
    echo "${bold}leaving test pip environment${normal}"
    source venv/bin/activate
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
    destroyenv
elif [[ $1 == "pypi_install" ]]
then
    buildenv
    echo "${bold}installing histdatacom from pypi: https://pypi.org/${normal}"
    pip install histdatacom
    destroyenv
fi
