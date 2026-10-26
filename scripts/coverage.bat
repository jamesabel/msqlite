pushd .
cd ..
mkdir cov
uv run pytest --cov-report=html --cov-report=xml:cov\coverage.xml --cov src\msqlite
popd
