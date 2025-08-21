#!/bin/bash
# wait-for-services.sh
# Script to wait for all services to be healthy before running tests

set -e

echo "🚀 Waiting for all services to be healthy..."

# Function to wait for a service to be healthy
wait_for_service() {
    local service_name=$1
    local max_attempts=${2:-30}
    local attempt=0
    
    echo "⏳ Waiting for $service_name to be healthy..."
    
    while [ $attempt -lt $max_attempts ]; do
        if docker compose ps $service_name | grep -q "healthy"; then
            echo "✅ $service_name is healthy"
            return 0
        fi
        
        attempt=$((attempt + 1))
        echo "   Attempt $attempt/$max_attempts - $service_name not ready yet..."
        sleep 2
    done
    
    echo "❌ $service_name failed to become healthy within $max_attempts attempts"
    docker compose logs $service_name
    return 1
}

# Function to test service connectivity
test_redis_connection() {
    echo "🔍 Testing Redis connection..."
    if docker compose exec -T redis redis-cli ping | grep -q "PONG"; then
        echo "✅ Redis connection successful"
    else
        echo "❌ Redis connection failed"
        return 1
    fi
}

test_mysql_connection() {
    echo "🔍 Testing MySQL connection..."
    if docker compose exec -T mysql mysqladmin ping -h localhost -u test -ptest --silent; then
        echo "✅ MySQL connection successful"
    else
        echo "❌ MySQL connection failed"
        return 1
    fi
}

test_cassandra_connection() {
    echo "🔍 Testing Cassandra connection..."
    if docker compose exec -T cassandra cqlsh -e "SELECT now() FROM system.local;" > /dev/null 2>&1; then
        echo "✅ Cassandra connection successful"
    else
        echo "❌ Cassandra connection failed"
        return 1
    fi
}

# Main execution
main() {
    echo "🏁 Starting service health checks..."
    
    # Wait for services to be healthy
    wait_for_service "redis" 20
    wait_for_service "mysql" 40
    wait_for_service "cassandra" 60  # Cassandra takes longer to start
    
    echo ""
    echo "🧪 Testing service connectivity..."
    
    # Test actual connectivity
    test_redis_connection
    test_mysql_connection
    test_cassandra_connection
    
    echo ""
    echo "🎉 All services are healthy and ready for testing!"
}

# Allow script to be sourced or executed
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
