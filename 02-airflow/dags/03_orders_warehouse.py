from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any

import pendulum
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, BaseHook, CronDataIntervalTimetable, Param, dag, task

TZ = "Europe/Moscow"
RAW_ORDERS = Asset("warehouse.raw.orders")
ARTIFACTS = Path("/opt/airflow/include/artifacts")

UPSERT_SQL = """
INSERT INTO raw.orders (order_id, order_date, created_at, city, channel, items, amount)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (order_id) DO UPDATE SET
    order_date = EXCLUDED.order_date,
    created_at = EXCLUDED.created_at,
    city       = EXCLUDED.city,
    channel    = EXCLUDED.channel,
    items      = EXCLUDED.items,
    amount     = EXCLUDED.amount,
    loaded_at  = now()
"""


def save_payload(payload: dict) -> dict:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    digest = hashlib.sha256(content).hexdigest()
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    target = ARTIFACTS / f"{digest}.json"
    with tempfile.NamedTemporaryFile(dir=ARTIFACTS, delete=False) as tmp:
        tmp.write(content)
    Path(tmp.name).replace(target)
    return {"path": str(target), "sha256": digest, "order_date": payload["order_date"]}


def read_payload(ref: dict) -> dict:
    content = Path(ref["path"]).read_bytes()
    if hashlib.sha256(content).hexdigest() != ref["sha256"]:
        raise ValueError("Хеш артефакта не совпал")
    return json.loads(content)


def accepted_orders(payload: dict) -> dict:
    orders = payload["orders"]
    if not orders:
        raise ValueError("Пустой ответ: сначала выясните, это ноль продаж или сбой источника")
    if len({o["order_id"] for o in orders}) != len(orders):
        raise ValueError("Повторяющиеся order_id в одном ответе источника")
    for order in orders:
        if order["items"] <= 0 or (order["amount"] is not None and order["amount"] < 0):
            raise ValueError(f"Недопустимые items/amount: {order['order_id']}")
        if order["created_at"][:10] != payload["order_date"]:
            raise ValueError(f"Заказ из другого дня: {order['order_id']}")
    broken = [o for o in orders if o["amount"] is None]
    if len(broken) > len(orders) * 0.1:
        raise ValueError(f"Без суммы {len(broken)} из {len(orders)} заказов")
    print(f"Отбраковано {len(broken)} из {len(orders)}; исходный артефакт сохранён")
    return {**payload, "orders": [o for o in orders if o["amount"] is not None]}


@dag(
    dag_id="03_orders_warehouse",
    start_date=pendulum.datetime(2026, 9, 1, tz=TZ),
    schedule=CronDataIntervalTimetable("@daily", timezone=TZ),
    catchup=False,
    max_active_runs=2,
    params={"order_date": Param(None, type=["null", "string"], format="date")},
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "retry_delay": pendulum.duration(seconds=10),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(seconds=60),
        "execution_timeout": pendulum.duration(minutes=2),
    },
    tags=["seminar", "production"],
)
def orders_warehouse():
    @task
    def extract(**context: Any) -> dict:
        explicit = context["params"]["order_date"]
        interval_start = context.get("data_interval_start")
        if explicit:
            order_date = explicit
        elif interval_start is not None:
            order_date = interval_start.in_timezone(TZ).to_date_string()
        else:
            raise ValueError("Для запуска без интервала передайте order_date в Params")
        conn = BaseHook.get_connection("source_api")
        response = requests.get(f"{conn.host}/orders", params={"date": order_date}, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if payload.get("date") != order_date:
            raise ValueError("Источник вернул другой день")
        orders = payload["orders"]
        print(f"{order_date}: получено {len(orders)} заказов")
        return save_payload({"order_date": order_date, "orders": orders})

    @task(retries=0)
    def check_quality(ref: dict) -> dict:
        return save_payload(accepted_orders(read_payload(ref)))

    @task(outlets=[RAW_ORDERS])
    def load(ref: dict, **context: Any) -> int:
        payload = read_payload(ref)
        rows = [
            (o["order_id"], payload["order_date"], o["created_at"], o["city"],
             o["channel"], o["items"], o["amount"])
            for o in payload["orders"]
        ]
        hook = PostgresHook(postgres_conn_id="warehouse")
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.executemany(UPSERT_SQL, rows)
            conn.commit()
        context["outlet_events"][RAW_ORDERS].extra = {
            "order_date": payload["order_date"], "rows": len(rows)
        }
        print(f"Записано {len(rows)} строк за {payload['order_date']}")
        return len(rows)

    load(check_quality(extract()))


orders_warehouse()
