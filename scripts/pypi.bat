pushd .
cd ..
rmdir /s /q dist
venv\Scripts\python.exe -m build
venv\Scripts\activate.bat
twine upload dist/*
deactivate
popd
