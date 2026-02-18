#!/bin/bash

echo "Запускаем сервер"

cd "$(dirname "$0")/.."

go mod download

go run main.go
