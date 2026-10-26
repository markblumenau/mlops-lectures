from __future__ import annotations

import pendulum
import requests
from airflow.sdk import dag, task


@dag(
    dag_id="06_flaky_retries",
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Moscow"),
    schedule=None,
    catchup=False,
    tags=["seminar", "step-6"],
)
def flaky_retries():
    @task(
        retries=5,
        pool="source_api_pool",
        execution_timeout=pendulum.duration(seconds=30),
        retry_delay=pendulum.duration(seconds=5),
        retry_exponential_backoff=True,
        max_retry_delay=pendulum.duration(seconds=30),
    )
    def fetch(**context) -> int:
        attempt = context["ti"].try_number
        response = requests.get(
            "http://source-api:8000/orders", params={"date": "2026-09-05"}, timeout=10
        )
        print(f"попытка {attempt}: код {response.status_code}")
        response.raise_for_status()
        return len(response.json()["orders"])

    fetch()


flaky_retries()
