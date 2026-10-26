from __future__ import annotations

import pendulum
import requests
from airflow.sdk import PokeReturnValue, dag, task
from airflow.providers.http.sensors.http import HttpSensor

TZ = "Europe/Moscow"


@dag(
    dag_id="05_wait_for_source",
    start_date=pendulum.datetime(2026, 9, 1, tz=TZ),
    schedule=None,
    catchup=False,
    tags=["seminar", "step-6"],
)
def wait_for_source():
    @task.sensor(poke_interval=10, timeout=120, mode="reschedule")
    def wait_for_api() -> PokeReturnValue:
        try:
            ok = requests.get("http://source-api:8000/health", timeout=5).ok
        except requests.RequestException:
            ok = False
        print(f"источник доступен: {ok}")
        return PokeReturnValue(is_done=ok, xcom_value={"checked_at": str(pendulum.now(TZ))})

    @task
    def go(sensor_result: dict) -> None:
        print(f"источник был готов в {sensor_result['checked_at']}, работаем")

    go(wait_for_api())

    HttpSensor(
        task_id="wait_deferred",
        http_conn_id="source_api",
        endpoint="health",
        deferrable=True,
        poke_interval=5,
        timeout=120,
        extra_options={"timeout": 5},
    )


wait_for_source()
