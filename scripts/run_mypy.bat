pushd .
cd ..
call venv\Scriptsctivate.bat
mypy
call deactivate
popd
