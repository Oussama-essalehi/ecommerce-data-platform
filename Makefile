up: ## Build and start everything: sources, Kafka, the bronze stream, the warehouse, Airflow
	docker compose up -d --build

down: ## Stop everything, keep the data
	docker compose down

reset: ## Stop everything and delete Kafka data, checkpoints, the lake, the warehouse and generated files
	docker compose down -v
	rm -rf data/landing data/lake data/quality data/alerts

csv: ## Write the marketplace CSV exports up to yesterday
	docker compose run --rm simulator export-csv

summary: ## Print orders and revenue for the last seven days
	docker compose run --rm simulator summary

bronze: ## Load the API and every CSV export into the bronze tables
	docker compose run --rm spark bronze-api
	docker compose run --rm spark bronze-marketplace --all

silver: ## Rebuild the silver tables (cleaned data) from bronze
	docker compose run --rm spark silver

gold: ## Rebuild the gold tables (business view) from silver
	docker compose run --rm spark gold

lake: bronze silver gold ## Run bronze, silver and gold in order

quality: ## Run the data-quality checks on the lake (stops here if a check is in error)
	docker compose run --rm spark quality

warehouse: ## Load gold into PostgreSQL, then build and test the star schema with dbt
	docker compose run --rm spark warehouse-load
	docker compose run --rm dbt build
	docker compose run --rm dbt source freshness

all: lake quality warehouse ## Run the whole pipeline, from the sources to the marts

docs: ## Generate and serve the warehouse documentation and lineage on http://localhost:8081 (Ctrl+C to stop)
	docker compose run --rm --service-ports dbt-docs

figures: ## Print the figures the Power BI report must show, computed on the warehouse
	docker compose cp dashboards/check_figures.sql warehouse:/tmp/check_figures.sql
	docker compose exec warehouse psql -U warehouse -d warehouse -f /tmp/check_figures.sql

trigger: ## Start a run of the daily DAG in Airflow now
	docker compose exec airflow airflow dags unpause ecommerce_daily
	docker compose exec airflow airflow dags trigger ecommerce_daily

psql: ## Open a SQL prompt on the warehouse
	docker compose exec warehouse psql -U warehouse -d warehouse

status: ## Row counts and key figures of every layer
	docker compose run --rm spark status

logs: ## Follow the event producer
	docker compose logs -f order-producer

test: ## Run the Spark job tests inside the pipelines image
	docker compose run --rm --no-deps --entrypoint python spark -m pytest -q

help:
	@grep -E "^[a-z]+:.*##" Makefile | sed "s/:.*## /\t/"

.PHONY: up down reset csv summary bronze silver gold lake quality warehouse all docs figures trigger psql status logs test help
.DEFAULT_GOAL := help
