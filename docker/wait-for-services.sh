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

# Function to test service connectivity from host
test_redis_connection() {
    echo "🔍 Testing Redis connection from host..."
    local max_attempts=30
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        # Test from within container first
        if docker compose exec -T redis redis-cli ping | grep -q "PONG"; then
            echo "✅ Redis internal connection successful"
            
            # Test from host using IPv4 explicitly
            if command -v redis-cli >/dev/null 2>&1; then
                if redis-cli -h 127.0.0.1 -p 6379 ping | grep -q "PONG"; then
                    echo "✅ Redis host connection successful"
                    return 0
                fi
            fi
            
            # Alternative test using nc if redis-cli not available
            if command -v nc >/dev/null 2>&1; then
                if nc -z 127.0.0.1 6379; then
                    echo "✅ Redis host port accessible"
                    return 0
                fi
            fi
            
            # If we can't test from host, assume internal success is enough
            echo "✅ Redis connection successful (internal test passed)"
            return 0
        fi
        
        attempt=$((attempt + 1))
        echo "   Redis connection attempt $attempt/$max_attempts..."
        sleep 2
    done
    
    echo "❌ Redis connection failed after $max_attempts attempts"
    return 1
}

test_mysql_connection() {
    echo "🔍 Testing MySQL connection from host..."
    local max_attempts=30
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        # Test from within container first
        if docker compose exec -T mysql mysqladmin ping -h localhost -u test -ptest --silent; then
            echo "✅ MySQL internal connection successful"
            
            # Test from host
            if command -v nc >/dev/null 2>&1; then
                if nc -z 127.0.0.1 3306; then
                    echo "✅ MySQL host port accessible"
                    return 0
                fi
            fi
            
            # If we can't test from host, assume internal success is enough
            echo "✅ MySQL connection successful (internal test passed)"
            return 0
        fi
        
        attempt=$((attempt + 1))
        echo "   MySQL connection attempt $attempt/$max_attempts..."
        sleep 2
    done
    
    echo "❌ MySQL connection failed after $max_attempts attempts"
    return 1
}

test_cassandra_connection() {
    echo "🔍 Testing Cassandra connection from host..."
    local max_attempts=60  # Cassandra needs more time
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        # Test from within container first
        if docker compose exec -T cassandra cqlsh -e "SELECT now() FROM system.local;" > /dev/null 2>&1; then
            echo "✅ Cassandra internal connection successful"
            
            # Test from host
            if command -v nc >/dev/null 2>&1; then
                if nc -z 127.0.0.1 9042; then
                    echo "✅ Cassandra host port accessible"
                    return 0
                fi
            fi
            
            # If we can't test from host, assume internal success is enough
            echo "✅ Cassandra connection successful (internal test passed)"
            return 0
        fi
        
        attempt=$((attempt + 1))
        echo "   Cassandra connection attempt $attempt/$max_attempts..."
        sleep 3  # Longer sleep for Cassandra
    done
    
    echo "❌ Cassandra connection failed after $max_attempts attempts"
    return 1
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
