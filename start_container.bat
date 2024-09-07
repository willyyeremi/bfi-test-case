@echo off
REM Start Docker Compose
docker-compose up -d

REM Wait for the Airflow container to be up and running
TIMEOUT /T 120

REM Start the Airflow scheduler in a separate terminal
start cmd /c "docker exec -d airflow_container airflow webserver"