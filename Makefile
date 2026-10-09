up: ## Build and start Kafka, the API, the event producer and the bronze stream
	docker compose up -d --build

down: ## Stop everything, keep the data
	docker compose down

reset: ## Stop everything and delete Kafka data, checkpoints, the lake and generated files
	docker compose down -v
	rm -rf data/landing data/lake

csv: ## Write the marketplace CSV exports up to yesterday
	docker compose run --rm simulator export-csv

summary: ## Print orders and revenue for the last seven days
	docker compose run --rm simulator summary

bronze: ## Load the API and every CSV export into the bronze tables
	docker compose run --rm spark bronze-api
	docker compose run --rm spark bronze-marketplace --all

status: ## Row counts and freshness of the bronze tables
	docker compose run --rm spark status

logs: ## Follow the event producer
	docker compose logs -f order-producer

test: ## Run the Spark job tests inside the pipelines image
	docker compose run --rm --no-deps --entrypoint python spark -m pytest -q

help:
	@grep -E "^[a-z]+:.*##" Makefile | sed "s/:.*## /\t/"

.PHONY: up down reset csv summary bronze status logs test help
.DEFAULT_GOAL := help
