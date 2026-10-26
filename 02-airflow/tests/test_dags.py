from __future__ import annotations

import pytest
from airflow.dag_processing.dagbag import DagBag

DAGS_FOLDER = "/opt/airflow/dags"


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder=DAGS_FOLDER)


def test_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}, f"файлы не разобрались: {dagbag.import_errors}"


def test_dags_found(dagbag: DagBag) -> None:
    assert len(dagbag.dags) >= 5


def test_every_dag_has_tags(dagbag: DagBag) -> None:
    without = [dag_id for dag_id, dag in dagbag.dags.items() if not dag.tags]
    assert without == [], f"DAG'и без тегов: {without}"


@pytest.mark.parametrize("dag_id", ["03_orders_warehouse", "04_daily_sales_marts"])
def test_production_dags_have_retries(dagbag: DagBag, dag_id: str) -> None:
    dag = dagbag.dags[dag_id]
    for task in dag.tasks:
        if task.task_id == "check_quality":
            assert task.retries == 0
            continue
        assert task.retries >= 1, f"{dag_id}.{task.task_id} без retries"


def test_warehouse_dag_structure(dagbag: DagBag) -> None:
    dag = dagbag.dags["03_orders_warehouse"]
    assert {t.task_id for t in dag.tasks} == {"extract", "check_quality", "load"}
    assert dag.get_task("load").upstream_task_ids == {"check_quality"}


def test_production_timeouts(dagbag: DagBag) -> None:
    for name in ("03_orders_warehouse", "04_daily_sales_marts"):
        assert all(t.execution_timeout is not None for t in dagbag.dags[name].tasks)
