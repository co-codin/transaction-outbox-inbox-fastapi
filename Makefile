.PHONY: help up down down-v build logs logs-api logs-consumer logs-inbox logs-grpc test test-e2e test-broker-outage test-inbox-integration lint check migrate revision grpc-generate clean dev dev-api dev-worker dev-inbox dev-grpc

# Default target
help: ## Show this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# Docker targets
up: ## Start all services with Docker Compose (builds if needed)
	API_KEY=$${API_KEY:-dev-api-key} docker compose up --build

down: ## Stop and remove containers
	docker compose down

down-v: ## Stop containers and remove volumes (full clean)
	docker compose down -v

build: ## Build Docker images without starting
	docker compose build

logs: ## Tail logs for all services
	docker compose logs -f

logs-api: ## Tail logs for the API service
	docker compose logs -f api

logs-consumer: ## Tail logs for the consumer service
	docker compose logs -f consumer

logs-inbox: ## Tail logs for the Kafka inbox worker
	docker compose logs -f inbox

logs-grpc: ## Tail logs for the gRPC service
	docker compose logs -f grpc

# Local development targets
test: ## Run the test suite (requires activated venv + dev dependencies)
	pytest -q

test-e2e: ## Run end-to-end tests against the running Docker stack (make up first)
	E2E=1 pytest -q tests/test_e2e.py

test-broker-outage: ## Run RabbitMQ outage recovery test against the running Docker stack
	E2E_BROKER_OUTAGE=1 pytest -q tests/test_broker_outage.py

test-inbox-integration: ## Run Kafka inbox integration tests (requires Docker)
	pytest -q tests/test_inbox_integration.py

lint: ## Run Ruff
	ruff check .

check: ## Run local lint, compile, tests, and Compose validation
	ruff check .
	python -m compileall -q app alembic tests
	pytest -q
	docker compose config >/dev/null

migrate: ## Run database migrations (alembic upgrade head)
	alembic upgrade head

revision: ## Create a new Alembic revision (usage: make revision MSG="your message")
	alembic revision --autogenerate -m "$(MSG)"

grpc-generate: ## Regenerate gRPC Python stubs from app/grpc/payment.proto
	python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. app/grpc/payment.proto

dev: ## Print local development setup instructions
	@echo "Local development setup:"
	@echo ""
	@echo "  python -m venv .venv"
	@echo "  . .venv/bin/activate"
	@echo "  pip install -r requirements-dev.txt"
	@echo "  cp .env.example .env     # edit if needed"
	@echo "  make migrate"
	@echo ""
	@echo "Then in separate terminals:"
	@echo "  make dev-api"
	@echo "  make dev-worker"
	@echo "  make dev-inbox"
	@echo "  make dev-grpc"
	@echo ""
	@echo "Run tests with: make test"

dev-api: ## Run the FastAPI development server locally
	uvicorn app.main:app --reload

dev-worker: ## Run the FastStream consumer locally
	faststream run app.worker:app --reload

dev-inbox: ## Run the Kafka transactional inbox worker locally
	python -m app.inbox_worker

dev-grpc: ## Run the gRPC payment server locally
	python -m app.grpc.server

# Utility
clean: ## Remove Python cache files and __pycache__ directories
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleaned Python cache files."
