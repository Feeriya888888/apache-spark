# Spark Orders Analytics — PySpark-проект на конкретной задаче

Apache Spark (PySpark)  считает базовую аналитику по заказам интернет-магазина:

1. **Выручка по дням** — сколько магазин заработал за каждую дату.
2. **Топ-5 товаров** по выручке.
3. **Средний чек** по каждому покупателю.
4. **Выручка по категориям** товаров.

Вход — CSV с заказами, выход — папки с результатами (Parquet/CSV).

## Структура проекта

```
spark-orders-analytics/
├── README.md                 # этот файл
├── requirements.txt          # зависимости
├── Dockerfile                # образ для запуска в Docker
├── docker-compose.yml        # поднимает джоб в контейнере
├── data/
│   └── orders.csv            # демо-данные (можно заменить своими)
└── src/
    └── main.py               # сам Spark-джоб
```

## Формат входных данных

CSV с заголовком и колонками:

| колонка     | тип     | пример        |
|-------------|---------|---------------|
| order_id    | string  | 1001          |
| dt          | date    | 2026-01-15    |
| customer_id | string  | c-42          |
| product     | string  | Espresso Cup  |
| category    | string  | kitchen       |
| qty         | int     | 2             |
| price       | double  | 490.0         |


