SHELL := /bin/bash
.DEFAULT_GOAL := help

PYTHON ?= python3
VENV ?= .venv
PY := $(VENV)/bin/python
COMPOSE ?= docker compose

START ?= 2026-01-01
END ?= 2026-01-07
DATE ?= 2026-01-03
SCENARIO ?= default

ifneq (,$(wildcard .env))
include .env
export
endif

export SENTINEL_DB_HOST ?= localhost
export SENTINEL_DB_PORT ?= $(POSTGRES_HOST_PORT)

.PHONY: help install up down clean logs ps generate run rerun backfill backfill-airflow \
        unpause demo-failure demo-dirty report dbt-build dbt-docs test test-integration lint format

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

.env:
	cp .env.example .env

$(PY):
	$(PYTHON) -m venv $(VENV)

install: $(PY) ## Create .venv with the package, dbt and dev tools
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -e ".[dev,dbt,report]"

up: .env ## Build and start PostgreSQL and Airflow (UI on http://localhost:8080)
	mkdir -p data reports
	$(COMPOSE) up -d --build --wait postgres airflow-webserver airflow-scheduler

down: ## Stop containers, keep volumes
	$(COMPOSE) down

clean: ## Stop containers and delete volumes, landing data and reports
	$(COMPOSE) down --volumes
	rm -rf data reports

logs: ## Follow Airflow scheduler logs
	$(COMPOSE) logs -f airflow-scheduler

ps: ## Container status
	$(COMPOSE) ps -a

generate: ## Write synthetic landing files for START..END
	$(PY) -m dataops_sentinel.synthetic.generator --start $(START) --end $(END) --scenario $(SCENARIO)

run: ## Run the pipeline from the host for START..END (generates data, no Airflow)
	$(PY) -m dataops_sentinel.orchestration.cli --init-db --generate --start $(START) --end $(END) --scenario $(SCENARIO)

rerun: ## Re-run one DATE from the host with existing landing files
	$(PY) -m dataops_sentinel.orchestration.cli --start $(DATE) --try-number 2

backfill: ## Reprocess START..END from the host with existing landing files
	$(PY) -m dataops_sentinel.orchestration.cli --start $(START) --end $(END) --try-number 2

unpause: ## Unpause the DAG so Airflow schedules new daily runs
	$(COMPOSE) exec airflow-scheduler airflow dags unpause dataops_sentinel_daily

backfill-airflow: ## Airflow backfill for START..END with synthetic SCENARIO
	$(COMPOSE) exec airflow-scheduler airflow dags backfill dataops_sentinel_daily -s $(START) -e $(END) \
		--reset-dagruns -y -c '{"scenario": "$(SCENARIO)"}'

demo-failure: ## Airflow: transient failure on ingest_orders for DATE, recovered by the retry
	$(COMPOSE) exec -e SENTINEL_FAULT_INJECTION=ingest_orders@$(DATE):transient_once airflow-scheduler \
		airflow dags backfill dataops_sentinel_daily -s $(DATE) -e $(DATE) --reset-dagruns -y

demo-dirty: ## Host: dirty source data for DATE is blocked by the quality gate, then fixed
	-$(PY) -m dataops_sentinel.orchestration.cli --generate --start $(DATE) --scenario dirty
	$(PY) -m dataops_sentinel.orchestration.cli --generate --start $(DATE) --scenario default --try-number 2

report: ## Write the Markdown run report for START..END
	$(PY) -m dataops_sentinel.reporting.report --start $(START) --end $(END)

dbt-build: ## dbt build from the host
	$(VENV)/bin/dbt build --project-dir dbt --profiles-dir dbt

dbt-docs: ## Generate dbt documentation
	$(VENV)/bin/dbt docs generate --project-dir dbt --profiles-dir dbt

test: ## Unit tests
	$(PY) -m pytest tests/unit -q

test-integration: ## Integration tests against PostgreSQL (needs make up)
	SENTINEL_RUN_INTEGRATION=1 SENTINEL_DB_USER=$(POSTGRES_ADMIN_USER) \
		SENTINEL_DB_PASSWORD=$(POSTGRES_ADMIN_PASSWORD) $(PY) -m pytest tests/integration -v

lint: ## Ruff and SQLFluff
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .
	$(VENV)/bin/sqlfluff lint sql dbt/models dbt/tests

format: ## Apply Ruff formatting
	$(PY) -m ruff check --fix .
	$(PY) -m ruff format .
