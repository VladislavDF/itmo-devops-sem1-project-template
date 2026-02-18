#!/bin/bash

echo "Начинаем тесты"

curl -s http://localhost:8080/api/v0/prices -o /dev/null
if [ $? -ne 0 ]; then
    echo "Сервер не отвечает на порту 8080"
    exit 1
fi
echo "Сервер работает"

echo "Отправляем POST..."
curl -X POST -F "file=@correct.zip" http://localhost:8080/api/v0/prices
echo ""

echo "Скачиваем данные..."
curl -X GET http://localhost:8080/api/v0/prices --output test.zip

if [ -f test.zip ]; then
    echo "Файл test.zip создан"
    ls -la test.zip
else
    echo "Ошибка: файл не скачался"
    exit 1
fi

echo "Тесты завершены"