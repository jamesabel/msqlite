pushd .
cd ..
set PYTHONPATH=%CD%
mkdir cov
venv\Scripts\pytest.exe --cov-report=html --cov-report=xml:cov\coverage.xml --cov src\msqlite
set PYTHONPATH=
popd
