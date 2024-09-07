@echo off

mkdir ".\database\postgresql"
call start_container.bat
python -m pip install virtualenv
python -m virtualenv ./api/.venv
.\api\.venv\Scripts\python.exe -m pip install -r ./api/requirements.txt
.\api\.venv\Scripts\python.exe ./api/app.py