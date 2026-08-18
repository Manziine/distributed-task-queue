.PHONY: up down test lint build logs

up:
	docker compose up --build -d

down:
	docker compose down

test:
	pytest tests/ -v --cov=app

lint:
	ruff check app/ tests/

build:
	docker compose build

logs:
	docker compose logs -f

worker-scale:
	docker compose up -d --scale worker=

queue-stats:
	curl -s http://localhost:8000/api/dashboard | python -m json.tool

clean:
	docker compose down -v --remove-orphans
