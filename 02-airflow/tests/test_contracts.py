import copy
import runpy

import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook


@pytest.fixture(scope="module")
def loader():
    return runpy.run_path("/opt/airflow/dags/03_orders_warehouse.py")


@pytest.fixture
def payload():
    return {
        "order_date": "2026-09-01",
        "orders": [
            {"order_id": str(i), "created_at": "2026-09-01T12:00:00",
             "items": 1, "amount": None if i < 2 else 10}
            for i in range(30)
        ],
    }


def test_quality_accepts_small_defect_and_keeps_input(loader, payload):
    before = copy.deepcopy(payload)
    clean = loader["accepted_orders"](payload)
    assert len(clean["orders"]) == 28
    assert payload == before


@pytest.mark.parametrize("case", ["empty", "duplicates", "items", "wrong_day", "too_many_nulls"])
def test_quality_rejects_invalid_input(loader, payload, case):
    if case == "empty":
        payload["orders"] = []
    elif case == "duplicates":
        payload["orders"][1]["order_id"] = "0"
    elif case == "items":
        payload["orders"][0]["items"] = 0
    elif case == "wrong_day":
        payload["orders"][0]["created_at"] = "2026-09-02T00:00:00"
    else:
        for order in payload["orders"][:4]:
            order["amount"] = None
    with pytest.raises(ValueError):
        loader["accepted_orders"](payload)


def test_artifact_reference_is_small_and_detects_corruption(loader, payload, tmp_path, monkeypatch):
    save = loader["save_payload"]
    monkeypatch.setitem(save.__globals__, "ARTIFACTS", tmp_path)
    ref = save(payload)
    assert set(ref) == {"path", "sha256", "order_date"}
    assert "orders" not in ref
    assert ref == save(payload)
    assert loader["read_payload"](ref) == payload
    from pathlib import Path
    Path(ref["path"]).write_text("{}")
    with pytest.raises(ValueError, match="Хеш"):
        loader["read_payload"](ref)


def test_mart_reconciles_values_and_deleted_groups():
    module = runpy.run_path("/opt/airflow/dags/04_daily_sales_marts.py")
    def isolated(sql):
        return sql.replace("raw.orders", "test_raw_orders").replace("marts.daily_sales", "test_daily_sales")
    with PostgresHook(postgres_conn_id="warehouse").get_conn() as conn, conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE test_raw_orders (LIKE raw.orders INCLUDING ALL) ON COMMIT DROP")
        cur.execute("CREATE TEMP TABLE test_daily_sales (LIKE marts.daily_sales INCLUDING ALL) ON COMMIT DROP")
        cur.execute("""INSERT INTO test_raw_orders
            (order_id,order_date,created_at,city,channel,items,amount) VALUES
            ('a','2026-09-01','2026-09-01 10:00','Москва','web',1,10),
            ('b','2026-09-01','2026-09-01 11:00','Казань','web',1,20)""")
        def rebuild():
            cur.execute(isolated(module["DELETE_SQL"]), ("2026-09-01",))
            cur.execute(isolated(module["REBUILD_SQL"]), ("2026-09-01",))
        def stale():
            cur.execute(isolated(module["STALE_DATES_SQL"]))
            return [str(row[0]) for row in cur.fetchall()]
        rebuild()
        assert stale() == []
        cur.execute("UPDATE test_raw_orders SET amount=17 WHERE order_id='a'")
        assert stale() == ["2026-09-01"]
        rebuild()
        assert stale() == []
        cur.execute("DELETE FROM test_raw_orders WHERE city='Казань'")
        rebuild()
        assert stale() == []
        cur.execute("SELECT count(*) FROM test_daily_sales")
        assert cur.fetchone()[0] == 1
        cur.execute("DELETE FROM test_raw_orders")
        assert stale() == ["2026-09-01"]
        rebuild()
        assert stale() == []
        cur.execute("SELECT count(*) FROM test_daily_sales")
        assert cur.fetchone()[0] == 0
        conn.rollback()
