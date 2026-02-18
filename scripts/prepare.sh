#!/bin/bash

echo "Начинаем подготовку"

pg_isready -h localhost -p 5432
if [ $? -ne 0 ]; then
    echo "Postgres не запущен"
    exit 1
fi

psql -U postgres -c "CREATE USER validator WITH PASSWORD 'val1dat0r';"
psql -U postgres -c "CREATE DATABASE \"project-sem-1\" OWNER validator;"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE \"project-sem-1\" TO validator;"

echo "Готово"
