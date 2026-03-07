# Makefile for goth-datastore Docker testing environment
# Provides convenient commands for local development and testing

.PHONY: help docker-up docker-down docker-logs docker-clean docker-status wait-for-services redis-cluster-init test test-local test-redis test-database test-database-postgres test-cassandra test-redis-cluster build fmt lint deps ci

COMPOSE_NETWORK := $(notdir $(CURDIR))_goth-network

# Default target
help: ## Show this help message
	@echo "🚀 Goth-Datastore Docker Testing Environment"
	@echo ""
	@echo "Available commands:"
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# Docker environment management
docker-up: ## Start all Docker services and wait until ready
	@echo "🚀 Starting Docker services..."
	@docker rm -f goth-redis-cluster-17000 goth-redis-cluster-17001 goth-redis-cluster-17002 2>/dev/null || true
	docker compose up -d
	@echo "⏳ Waiting for services to be healthy..."
	@./docker/wait-for-services.sh
	@$(MAKE) redis-cluster-init

docker-down: ## Stop and remove Docker containers
	@echo "🛑 Stopping Docker services..."
	docker compose down
	@docker rm -f goth-redis-cluster-17000 goth-redis-cluster-17001 goth-redis-cluster-17002 2>/dev/null || true

docker-clean: ## Stop containers and remove volumes (clean slate)
	@echo "🧹 Cleaning up Docker environment..."
	docker compose down -v --remove-orphans
	@docker rm -f goth-redis-cluster-17000 goth-redis-cluster-17001 goth-redis-cluster-17002 2>/dev/null || true
	docker system prune -f

docker-logs: ## Show logs from all Docker services
	@echo "📋 Showing Docker service logs..."
	docker compose logs -f

docker-status: ## Show status of Docker services
	@echo "📊 Docker services status:"
	docker compose ps

redis-cluster-init: ## Create Redis cluster if it is not initialized yet
	@docker compose exec -T redis-cluster-17000 redis-cli -p 17000 cluster info 2>/dev/null | grep -q 'cluster_state:ok' || \
		docker compose exec -T redis-cluster-17000 redis-cli --cluster create redis-cluster-17000:17000 redis-cluster-17001:17001 redis-cluster-17002:17002 --cluster-replicas 0 --cluster-yes

test: docker-up ## Run full Go test suite and Redis cluster integration
	@echo "🧪 Running full Go test suite..."
	go test -v ./... -timeout 30m
	@$(MAKE) test-redis-cluster

test-local: ## Run tests with local services (assumes services are already running)
	@echo "🧪 Running tests with local configuration..."
	go test -v ./... -timeout 30m

test-redis: docker-up ## Run Redis tests
	@echo "🧪 Running Redis tests..."
	go test -v -run TestRedis ./... -timeout 10m

test-database: docker-up ## Run MySQL database tests
	@echo "🧪 Running MySQL database tests..."
	go test -v -run TestDatabaseMySQLCRUD ./... -timeout 10m

test-database-postgres: docker-up ## Run PostgreSQL database tests
	@echo "🧪 Running PostgreSQL database tests..."
	go test -v -run TestDatabasePostgresCRUD ./... -timeout 10m

test-cassandra: docker-up ## Run Cassandra tests
	@echo "🧪 Running Cassandra tests..."
	go test -v -run TestCassandra ./... -timeout 10m

test-redis-cluster: ## Run Redis cluster integration test in Docker network
	@echo "🧪 Running Redis cluster integration test..."
	docker run --rm --network $(COMPOSE_NETWORK) -v $(CURDIR):/workspace -w /workspace -e TEST_REDIS_CLUSTER_ADDRS=redis-cluster-17000:17000,redis-cluster-17001:17001,redis-cluster-17002:17002 golang:1.24.4 sh -lc "/usr/local/go/bin/go test ./... -run TestRedisClusterIntegration -count=1"

build: ## Build the Go application
	@echo "🔨 Building Go application..."
	go build -v ./...

fmt: ## Format Go code
	@echo "🎨 Formatting Go code..."
	go fmt ./...

lint: ## Run golangci-lint (requires golangci-lint to be installed)
	@echo "🔍 Running linter..."
	golangci-lint run

wait-for-services: ## Wait for all services to be healthy
	@./docker/wait-for-services.sh

deps: ## Download Go dependencies
	@echo "📦 Downloading Go dependencies..."
	go mod download
	go mod tidy

ci: docker-clean build test ## Simulate CI/CD pipeline locally
	@echo "✅ Local CI/CD pipeline completed successfully!"
