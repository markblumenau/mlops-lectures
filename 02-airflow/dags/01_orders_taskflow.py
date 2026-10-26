from __future__ import annotations

from typing import Any

import pendulum
import requests
from airflow.sdk import Param, dag, task

SOURCE_URL = "http://source-api:8000/orders"


@dag(
    dag_id="01_orders_taskflow",
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Moscow"),
    schedule=None,
    catchup=False,
    params={"order_date": Param("2026-09-01", type="string", format="date")},
    tags=["seminar", "step-2"],
)
def orders_taskflow():
    @task
    def extract(**context: Any) -> list[dict]:
        order_date = context["params"]["order_date"]
        response = requests.get(SOURCE_URL, params={"date": order_date}, timeout=30)
        response.raise_for_status()
        orders = response.json()["orders"]
        print(f"{order_date}: получено {len(orders)} заказов")
        return orders

    @task
    def transform(orders: list[dict]) -> list[dict]:
        clean = [o for o in orders if o["amount"] is not None]
        print(f"отброшено записей без суммы: {len(orders) - len(clean)}")
        for order in clean:
            order["price_per_item"] = round(order["amount"] / order["items"], 2)
        return clean

    @task
    def summarize(orders: list[dict]) -> dict:
        revenue = sum(o["amount"] for o in orders)
        by_city: dict[str, int] = {}
        for order in orders:
            by_city[order["city"]] = by_city.get(order["city"], 0) + 1
        result = {
            "orders": len(orders),
            "revenue": round(revenue, 2),
            "avg_check": round(revenue / len(orders), 2) if orders else 0,
            "by_city": by_city,
        }
        print(result)
        return result

    summarize(transform(extract()))


orders_taskflow()
