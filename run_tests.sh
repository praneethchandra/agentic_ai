#!/bin/bash

# Data Source Interface MCP Server and Client - Test Runner
# This script sets up the Docker environment and runs comprehensive integration tests

set -e  # Exit on any error

echo "🚀 Data Source Interface MCP Server and Client - Integration Test Suite"
echo "======================================================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    print_error "docker-compose is not installed. Please install docker-compose and try again."
    exit 1
fi

print_status "Starting Docker services..."

# Stop any existing containers
docker-compose down --volumes --remove-orphans 2>/dev/null || true

# Build and start services
docker-compose up -d --build

print_status "Waiting for services to be ready..."

# Wait for services to be healthy
max_attempts=60
attempt=0

while [ $attempt -lt $max_attempts ]; do
    attempt=$((attempt + 1))
    
    # Check MongoDB
    if docker-compose exec -T mongodb mongosh --quiet --eval "db.runCommand('ping').ok" school_management > /dev/null 2>&1; then
        mongodb_ready=true
    else
        mongodb_ready=false
    fi
    
    # Check Elasticsearch
    if curl -s http://localhost:9200/_cluster/health > /dev/null 2>&1; then
        elasticsearch_ready=true
    else
        elasticsearch_ready=false
    fi
    
    # Check PostgreSQL
    if docker-compose exec -T postgresql pg_isready -U admin -d school_management > /dev/null 2>&1; then
        postgresql_ready=true
    else
        postgresql_ready=false
    fi
    
    if [ "$mongodb_ready" = true ] && [ "$elasticsearch_ready" = true ] && [ "$postgresql_ready" = true ]; then
        print_success "All services are ready!"
        break
    fi
    
    if [ $attempt -eq $max_attempts ]; then
        print_error "Services failed to start within expected time"
        print_status "Service status:"
        echo "  MongoDB: $mongodb_ready"
        echo "  Elasticsearch: $elasticsearch_ready"
        echo "  PostgreSQL: $postgresql_ready"
        exit 1
    fi
    
    print_status "Waiting for services... (attempt $attempt/$max_attempts)"
    sleep 2
done

print_status "Running integration tests..."

# Run tests with different configurations
test_exit_code=0

echo ""
echo "📋 Test Categories:"
echo "  1. CRUD Operations (all databases)"
echo "  2. Bulk Operations (all databases)"
echo "  3. Relationship Management (all databases)"
echo "  4. Aggregate Operations (all databases)"
echo "  5. MCP Client Functionality"
echo ""

# Run tests in the app container
if docker-compose exec -T app python -m pytest tests/ -v --tb=short; then
    print_success "All integration tests passed!"
else
    test_exit_code=1
    print_error "Some tests failed. Check the output above for details."
fi

# Generate test report
print_status "Generating test report..."
docker-compose exec -T app python -m pytest tests/ --tb=short --quiet > test_results.txt 2>&1 || true

# Show service logs if tests failed
if [ $test_exit_code -ne 0 ]; then
    print_warning "Showing service logs for debugging..."
    echo ""
    echo "=== MongoDB Logs ==="
    docker-compose logs --tail=20 mongodb
    echo ""
    echo "=== Elasticsearch Logs ==="
    docker-compose logs --tail=20 elasticsearch
    echo ""
    echo "=== PostgreSQL Logs ==="
    docker-compose logs --tail=20 postgresql
fi

# Cleanup option
echo ""
read -p "Do you want to stop and remove the Docker containers? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_status "Stopping and removing containers..."
    docker-compose down --volumes --remove-orphans
    print_success "Cleanup completed!"
else
    print_status "Containers are still running. You can access them at:"
    echo "  MongoDB: mongodb://admin:password@localhost:27017"
    echo "  Elasticsearch: http://localhost:9200"
    echo "  PostgreSQL: postgresql://admin:password@localhost:5432/school_management"
    echo ""
    echo "To stop containers later, run: docker-compose down --volumes"
fi

echo ""
if [ $test_exit_code -eq 0 ]; then
    print_success "🎉 All tests completed successfully!"
else
    print_error "❌ Some tests failed. Please check the logs above."
fi

exit $test_exit_code
