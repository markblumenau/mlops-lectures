# Airflow

Скопируйте настройки: `cp .env.example .env`.
На Linux задайте в .env AIRFLOW_UID равным `id -u`, не стирая другие настройки.
Каталоги logs и include должны быть доступны UID контейнера на запись.

## Как поднять?
```bash
make up
make health
make cli C="dags list-import-errors"
make test
```

`make up` собирает образ.

## Прогон 

```bash
make cli C="dags test 00_hello"
make cli C="dags test 01_orders_taskflow"
make cli C="dags test 02_orders_scheduled 2026-09-05"
docker compose exec -T scheduler airflow dags test 03_orders_warehouse \
  --conf '{"order_date":"2026-09-01"}'
make cli C="dags test 04_daily_sales_marts"
make cli C="dags test 05_wait_for_source"
make cli C="dags test 06_flaky_retries"
docker compose exec -T warehouse psql -X -U etl -d warehouse < include/fingerprint.sql
docker compose exec -T warehouse psql -X -U etl -d warehouse < include/mart-check.sql
```

## Завершение

`make down` останавливает контейнеры, сохраняя named volumes.
`make clean` - осознанный полный сброс обеих учебных баз и артефактов.
