SHELL := /bin/bash

DBT_PROJECT_DIR := ./dbt/suumo
WITH_ENV := set -a; source .env; set +a;

.PHONY: dbt-debug dbt-run dbt-build

dbt-debug:
	@$(WITH_ENV) dbt debug --project-dir $(DBT_PROJECT_DIR)

dbt-run:
	@$(WITH_ENV) dbt run --project-dir $(DBT_PROJECT_DIR)

dbt-build:
	@$(WITH_ENV) dbt build --project-dir $(DBT_PROJECT_DIR)

docker-up:
	docker compose up -d

dokcer-down:
	docker compose down

docker-restart:
	docker compose down && docker compose up -d