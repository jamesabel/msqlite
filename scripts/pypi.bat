pushd .
cd ..
rmdir /s /q dist
venv\Scripts\python.exe -m build
call venv\Scripts\activate.bat
twine upload dist/*
call deactivate
popd
