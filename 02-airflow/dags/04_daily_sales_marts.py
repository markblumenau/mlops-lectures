from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, AssetOrTimeSchedule, CronTriggerTimetable, dag, task

TZ = "Europe/Moscow"
RAW_ORDERS = Asset("warehouse.raw.orders")
DAILY_SALES = Asset("warehouse.marts.daily_sales")

STALE_DATES_SQL = """
WITH expected AS (
    SELECT order_date, city, count(*) AS orders_cnt,
           sum(amount) AS revenue, round(avg(amount), 2) AS avg_check
    FROM raw.orders
    WHERE amount IS NOT NULL
    GROUP BY order_date, city
)
SELECT DISTINCT coalesce(r.order_date, m.order_date) AS order_date
FROM expected r
FULL OUTER JOIN marts.daily_sales m USING (order_date, city)
WHERE (r.orders_cnt, r.revenue, r.avg_check)
      IS DISTINCT FROM (m.orders_cnt, m.revenue, m.avg_check)
ORDER BY 1
"""
DELETE_SQL = "DELETE FROM marts.daily_sales WHERE order_date = %s"
REBUILD_SQL = """
INSERT INTO marts.daily_sales (order_date, city, orders_cnt, revenue, avg_check)
SELECT order_date, city, count(*), sum(amount), round(avg(amount), 2)
FROM raw.orders
WHERE order_date = %s AND amount IS NOT NULL
GROUP BY order_date, city
"""


@dag(
    dag_id="04_daily_sales_marts",
    start_date=pendulum.datetime(2026, 9, 1, tz=TZ),
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 4 * * *", timezone=TZ),
        assets=[RAW_ORDERS],
    ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform", "retries": 2,
        "retry_delay": pendulum.duration(seconds=10),
        "execution_timeout": pendulum.duration(minutes=2),
    },
    tags=["seminar", "production"],
)
def daily_sales_marts():
    @task
    def dates_to_rebuild(**context: Any) -> list[str]:
        dates: set[str] = set()
        for _asset, events in (context.get("triggering_asset_events") or {}).items():
            for event in events:
                order_date = (event.extra or {}).get("order_date")
                if order_date:
                    dates.add(order_date)
        hook = PostgresHook(postgres_conn_id="warehouse")
        stale = {str(row[0]) for row in hook.get_records(STALE_DATES_SQL)}
        result = sorted(dates | stale)
        print(f"Из событий: {sorted(dates)}; расхождения: {sorted(stale)}; пересчёт: {result}")
        return result

    @task(outlets=[DAILY_SALES])
    def rebuild(dates: list[str]) -> None:
        if not dates:
            print("Расхождений нет")
            return
        hook = PostgresHook(postgres_conn_id="warehouse")
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            for order_date in dates:
                cur.execute(DELETE_SQL, (order_date,))
                cur.execute(REBUILD_SQL, (order_date,))
                print(f"{order_date}: записано групп {cur.rowcount}")
            conn.commit()

    rebuild(dates_to_rebuild())


daily_sales_marts()
