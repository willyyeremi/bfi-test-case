@echo off

docker-compose up -d
python -m pip install virtualenv
python -m virtualenv ./api/.venv
Set-ExecutionPolicy -ExecutionPolicy Unrestricted -Scope Process
.\api\.venv\Scripts\activate
python -m pip install -r ./api/requirements.txt
python ./api/app.py