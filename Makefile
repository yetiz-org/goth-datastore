# Makefile for goth-datastore Docker testing environment
# Provides convenient commands for local development and testing

.PHONY: help docker-up docker-down docker-logs docker-clean test-docker test-local build docker-status wait-for-services test-database-postgres

# Default target
help: ## Show this help message
	@echo "🚀 Goth-Datastore Docker Testing Environment"
	@echo ""
	@echo "Available commands:"
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# Docker environment management
docker-up: ## Start all Docker services (Redis, MySQL, Cassandra)
	@echo "🚀 Starting Docker services..."
	docker compose up -d
	@echo "⏳ Waiting for services to be healthy..."
	@./docker/wait-for-services.sh

docker-down: ## Stop and remove Docker containers
	@echo "🛑 Stopping Docker services..."
	docker compose down

docker-clean: ## Stop containers and remove volumes (clean slate)
	@echo "🧹 Cleaning up Docker environment..."
	docker compose down -v --remove-orphans
	docker system prune -f

docker-logs: ## Show logs from all Docker services
	@echo "📋 Showing Docker service logs..."
	docker compose logs -f

docker-status: ## Show status of Docker services
	@echo "📊 Docker services status:"
	docker compose ps

# Testing commands
test-docker: docker-up ## Run tests in Docker environment with automatic cleanup
	@echo "🧪 Running tests in Docker environment..."
	@echo "Setting up test configuration for Docker services..."
	@cp -r docker/config/* example/
	@echo "Running Go tests..."
	@set -e; \
	go test -v ./... -timeout 30m; \
	TEST_RESULT=$$?; \
	echo "Restoring original test configuration..."; \
	git checkout -- example/ || true; \
	echo "🛑 Stopping Docker services..."; \
	docker compose down; \
	if [ $$TEST_RESULT -eq 0 ]; then \
		echo "✅ Tests passed and Docker services stopped"; \
	else \
		echo "❌ Tests failed but Docker services stopped"; \
		exit $$TEST_RESULT; \
	fi

test-local: ## Run tests with local services (assumes services are already running)
	@echo "🧪 Running tests with local configuration..."
	go test -v ./... -timeout 30m

test-redis: docker-up ## Run only Redis tests in Docker environment with automatic cleanup
	@echo "🧪 Running Redis tests in Docker environment..."
	@cp docker/config/redis-test/secret.json example/redis-test/
	@set -e; \
	go test -v -run TestRedis ./... -timeout 10m; \
	TEST_RESULT=$$?; \
	git checkout -- example/redis-test/secret.json || true; \
	echo "🛑 Stopping Docker services..."; \
	docker compose down; \
	if [ $$TEST_RESULT -eq 0 ]; then \
		echo "✅ Redis tests passed and Docker services stopped"; \
	else \
		echo "❌ Redis tests failed but Docker services stopped"; \
		exit $$TEST_RESULT; \
	fi

test-database: docker-up ## Run only Database tests in Docker environment with automatic cleanup
	@echo "🧪 Running Database tests in Docker environment..."
	@cp docker/config/database-test/secret.json example/database-test/
	@set -e; \
	go test -v -run TestDatabaseMySQLCRUD ./... -timeout 10m; \
	TEST_RESULT=$$?; \
	git checkout -- example/database-test/secret.json || true; \
	echo "🛑 Stopping Docker services..."; \
	docker compose down; \
	if [ $$TEST_RESULT -eq 0 ]; then \
		echo "✅ Database tests passed and Docker services stopped"; \
	else \
		echo "❌ Database tests failed but Docker services stopped"; \
		exit $$TEST_RESULT; \
	fi

test-database-postgres: docker-up ## Run only PostgreSQL Database tests in Docker environment with automatic cleanup
	@echo "🧪 Running PostgreSQL Database tests in Docker environment..."
	@cp docker/config/database-postgres-test/secret.json example/database-postgres-test/
	@set -e; \
	go test -v -run TestDatabasePostgresCRUD ./... -timeout 10m; \
	TEST_RESULT=$$?; \
	git checkout -- example/database-postgres-test/secret.json 2>/dev/null || true; \
	echo "🛑 Stopping Docker services..."; \
	docker compose down; \
	if [ $$TEST_RESULT -eq 0 ]; then \
		echo "✅ PostgreSQL Database tests passed and Docker services stopped"; \
	else \
		echo "❌ PostgreSQL Database tests failed but Docker services stopped"; \
		exit $$TEST_RESULT; \
	fi

test-cassandra: docker-up ## Run only Cassandra tests in Docker environment with automatic cleanup
	@echo "🧪 Running Cassandra tests in Docker environment..."
	@cp docker/config/cassandra-test/secret.json example/cassandra-test/
	@set -e; \
	go test -v -run TestCassandra ./... -timeout 10m; \
	TEST_RESULT=$$?; \
	git checkout -- example/cassandra-test/secret.json || true; \
	echo "🛑 Stopping Docker services..."; \
	docker compose down; \
	if [ $$TEST_RESULT -eq 0 ]; then \
		echo "✅ Cassandra tests passed and Docker services stopped"; \
	else \
		echo "❌ Cassandra tests failed but Docker services stopped"; \
		exit $$TEST_RESULT; \
	fi

# Build and development commands
build: ## Build the Go application
	@echo "🔨 Building Go application..."
	go build -v ./...

fmt: ## Format Go code
	@echo "🎨 Formatting Go code..."
	go fmt ./...

lint: ## Run golangci-lint (requires golangci-lint to be installed)
	@echo "🔍 Running linter..."
	golangci-lint run

# Utility commands
wait-for-services: ## Wait for all services to be healthy
	@./docker/wait-for-services.sh

deps: ## Download Go dependencies
	@echo "📦 Downloading Go dependencies..."
	go mod download
	go mod tidy

# CI/CD simulation
ci: docker-clean build test-docker ## Simulate CI/CD pipeline locally
	@echo "✅ Local CI/CD pipeline completed successfully!"

# Quick development cycle (test-docker already includes cleanup)
dev: docker-up wait-for-services test-docker ## Quick development cycle: up + wait + test + cleanup
	@echo "🎉 Development cycle completed with full cleanup!"

# Simple one-command test start with complete lifecycle management
quick-test: docker-up ## Complete test lifecycle: Setup → Test → Cleanup automatically
	@echo "🚀 Quick Test: Starting Docker environment and running all tests..."
	@echo "Setting up test configuration..."
	@cp -r docker/config/* example/
	@echo "Running comprehensive test suite..."
	@bash -c ' \
	cleanup() { \
		echo "Restoring original test configuration..."; \
		git checkout -- example/ 2>/dev/null || true; \
		echo "🛑 Stopping Docker services and cleaning up resources..."; \
		docker compose down; \
	}; \
	trap cleanup EXIT; \
	set -e; \
	go test -v ./... -timeout 30m; \
	echo "✅ Quick test completed successfully! All resources will be cleaned up."; \
	'
