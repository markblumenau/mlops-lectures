from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="00_hello",
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Moscow"),
    schedule=None,
    catchup=False,
    tags=["seminar", "step-1"],
)
def hello():
    @task
    def pick_city() -> str:
        return "Казань"

    @task
    def greet(city: str) -> str:
        print(f"Привет из {city}")
        return f"ok:{city}"

    greet(pick_city())


hello()
