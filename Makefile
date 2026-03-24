.PHONY: up down logs test lint producer consumer clean

# Start the full pipeline (infrastructure + app)
up:
	docker compose up --build -d

# Stop everything
down:
	docker compose down -v

# View logs (all services)
logs:
	docker compose logs -f

# Run only infrastructure (Kafka, Schema Registry, MongoDB)
infra:
	docker compose up -d zookeeper kafka schema-registry mongodb

# Run producer locally (requires infra running)
producer:
	python producer_main.py

# Run consumer locally (requires infra running)
consumer:
	python consumer_main.py

# Run tests
test:
	python -m pytest tests/ -v

# Run linter
lint:
	ruff check .

# Clean up
clean:
	docker compose down -v
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf logs/*.log
