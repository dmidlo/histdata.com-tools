dev()
{
    python setup.py build
    python setup.py install
    python setup.py develop
}

build()
{
    rm -rf ./dist
    python setup.py check
    python setup.py sdist
    python setup.py bdist_wheel --universal
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
    echo "pypi"
    twine upload dist/* dist/*.asc
elif [[ $1 == "testpypi" ]]
then
    build
    echo "testpypi"
    gpg --detach-sign -a dist/*.tar.gz
    twine upload -r testpypi --config-file .pypirc dist/*.whl dist/*.tar.gz dist/*.asc
fi
