.DEFAULT_GOAL := help
COMPOSE := docker compose

help:  ## показать список команд
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*## /\t/' | expand -t20

up:  ## собрать образ и поднять всё
	$(COMPOSE) up -d --build --wait --wait-timeout 240
	$(MAKE) pool
	@echo "Airflow:    http://localhost:8080"
	@echo "Источник:   http://localhost:8000/orders?date=2026-09-01"

down:  ## остановить, оставив данные
	$(COMPOSE) down

clean:  ## остановить и стереть обе базы и логи
	$(COMPOSE) down -v
	rm -rf logs/* include/artifacts

ps:  ## что запущено
	$(COMPOSE) ps

health:  ## проверить, что все компоненты живы
	$(COMPOSE) exec -T scheduler python /opt/airflow/include/health.py

logs:  ## логи планировщика
	$(COMPOSE) logs -f scheduler

cli:  ## произвольная команда airflow: make cli C="dags list"
	$(COMPOSE) exec -T scheduler airflow $(C)

test:  ## прогнать тесты DAG'ов
	$(COMPOSE) exec -T scheduler python -m pytest /opt/airflow/tests -q

psql:  ## psql в хранилище
	$(COMPOSE) exec warehouse psql -U etl -d warehouse

flaky:  ## перезапустить источник так, чтобы он падал в 60% случаев
	FLAKY_RATE=0.6 $(COMPOSE) up -d --force-recreate source-api

stable:  ## вернуть источнику нормальное поведение
	FLAKY_RATE=0 FAIL_FIRST=0 SOURCE_READY=1 $(COMPOSE) up -d --force-recreate source-api

.PHONY: help up down clean ps health logs cli test psql flaky stable

fail-first:  ## первые два запроса за каждую дату вернут 503
	FLAKY_RATE=0 FAIL_FIRST=2 $(COMPOSE) up -d --force-recreate source-api

not-ready:  ## health вернёт 404 для опыта с сенсорами
	SOURCE_READY=0 FAIL_FIRST=0 $(COMPOSE) up -d --force-recreate source-api

pool:  ## создать пул с одним слотом для опыта
	$(COMPOSE) exec -T scheduler airflow pools set source_api_pool 1 "One concurrent source request"

.PHONY: fail-first not-ready pool
