pushd .
cd ..
call venv\Scripts\activate.bat
python -m black src/msqlite test_msqlite
call deactivate
popd
