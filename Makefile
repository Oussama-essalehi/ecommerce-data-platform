up: ## Build and start Kafka, the API, the event producer, the bronze stream and the warehouse
	docker compose up -d --build

down: ## Stop everything, keep the data
	docker compose down

reset: ## Stop everything and delete Kafka data, checkpoints, the lake, the warehouse and generated files
	docker compose down -v
	rm -rf data/landing data/lake data/quality

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

.PHONY: up down reset csv summary bronze silver gold lake quality warehouse all psql status logs test help
.DEFAULT_GOAL := help
