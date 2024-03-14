pushd .
cd ..
call venv\Scripts\activate.bat
cd src
mypy -m msqlite
cd ..
mypy -m test_msqlite
call deactivate
popd
