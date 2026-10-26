from __future__ import annotations

from typing import Any

import pendulum
import requests
from airflow.sdk import CronDataIntervalTimetable, dag, task

SOURCE_URL = "http://source-api:8000/orders"
TZ = "Europe/Moscow"


@dag(
    dag_id="02_orders_scheduled",
    start_date=pendulum.datetime(2026, 9, 1, tz=TZ),
    end_date=pendulum.datetime(2026, 9, 7, tz=TZ),
    schedule=CronDataIntervalTimetable("@daily", timezone=TZ),
    catchup=True,
    max_active_runs=2,
    tags=["seminar", "step-3"],
)
def orders_scheduled():
    @task
    def extract(**context: Any) -> dict:
        interval_start = context["data_interval_start"]
        order_date = interval_start.in_timezone(TZ).to_date_string()
        print(
            f"интервал {interval_start.in_timezone(TZ)} .. "
            f"{context['data_interval_end'].in_timezone(TZ)}"
        )
        response = requests.get(SOURCE_URL, params={"date": order_date}, timeout=30)
        response.raise_for_status()
        orders = response.json()["orders"]
        print(f"дата выгрузки {order_date}: {len(orders)} заказов")
        return {"order_date": order_date, "orders": orders}

    @task
    def report(payload: dict) -> None:
        orders = [o for o in payload["orders"] if o["amount"] is not None]
        revenue = round(sum(o["amount"] for o in orders), 2)
        print(f"{payload['order_date']}: {len(orders)} заказов на {revenue}")

    report(extract())


orders_scheduled()
