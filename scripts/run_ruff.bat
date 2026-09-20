pushd .
cd ..
call venv\Scriptsctivate.bat
ruff format src test_msqlite scripts
ruff check --fix src test_msqlite scripts
call deactivate
popd
