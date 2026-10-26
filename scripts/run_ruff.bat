pushd .
cd ..
uv run ruff format src test_msqlite scripts
uv run ruff check --fix src test_msqlite scripts
popd
