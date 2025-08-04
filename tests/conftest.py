"""
Pytest configuration and fixtures for integration tests.
"""
import asyncio
import os
import pytest
from typing import AsyncGenerator, Dict, Any
import time

from data_source_interface.database_interface import MongoDBInterface
from data_source_interface.elasticsearch_interface import ElasticsearchInterface
from data_source_interface.postgresql_interface import PostgreSQLInterface
from data_source_interface.mcp_client import DataSourceMCPClient


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def wait_for_services():
    """Wait for all database services to be ready."""
    max_retries = 30
    retry_delay = 2
    
    services = {
        'mongodb': ('mongodb://admin:password@localhost:27017/school_management?authSource=admin', MongoDBInterface),
        'elasticsearch': ('http://localhost:9200', ElasticsearchInterface),
        'postgresql': ('postgresql://admin:password@localhost:5432/school_management', PostgreSQLInterface)
    }
    
    ready_services = {}
    
    for service_name, (connection_string, interface_class) in services.items():
        for attempt in range(max_retries):
            try:
                if interface_class == PostgreSQLInterface:
                    interface = interface_class(connection_string)
                else:
                    interface = interface_class(connection_string, 'school_management')
                await interface.initialize()
                await interface.cleanup()
                ready_services[service_name] = connection_string
                print(f"✓ {service_name} is ready")
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"✗ {service_name} failed to start after {max_retries} attempts: {e}")
                    raise
                print(f"⏳ Waiting for {service_name} (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(retry_delay)
    
    return ready_services


@pytest.fixture(scope="session")
async def mongodb_interface(wait_for_services) -> AsyncGenerator[MongoDBInterface, None]:
    """Create and initialize MongoDB interface."""
    connection_string = wait_for_services['mongodb']
    interface = MongoDBInterface(connection_string, 'school_management_test')
    await interface.initialize()
    yield interface
    await interface.cleanup()


@pytest.fixture(scope="session")
async def elasticsearch_interface(wait_for_services) -> AsyncGenerator[ElasticsearchInterface, None]:
    """Create and initialize Elasticsearch interface."""
    connection_string = wait_for_services['elasticsearch']
    interface = ElasticsearchInterface(connection_string, 'school_management_test')
    await interface.initialize()
    yield interface
    await interface.cleanup()


@pytest.fixture(scope="session")
async def postgresql_interface(wait_for_services) -> AsyncGenerator[PostgreSQLInterface, None]:
    """Create and initialize PostgreSQL interface."""
    connection_string = wait_for_services['postgresql']
    interface = PostgreSQLInterface(connection_string)
    await interface.initialize()
    yield interface
    await interface.cleanup()


@pytest.fixture(params=['mongodb', 'elasticsearch', 'postgresql'])
async def database_interface(request, mongodb_interface, elasticsearch_interface, postgresql_interface):
    """Parametrized fixture that provides all database interfaces."""
    interfaces = {
        'mongodb': mongodb_interface,
        'elasticsearch': elasticsearch_interface,
        'postgresql': postgresql_interface
    }
    
    interface = interfaces[request.param]
    
    # Clean up before each test
    try:
        await interface.cleanup_test_data()
    except:
        pass  # Ignore cleanup errors
    
    return interface, request.param


@pytest.fixture
async def mcp_client() -> AsyncGenerator[DataSourceMCPClient, None]:
    """Create and initialize MCP client."""
    client = DataSourceMCPClient()
    await client.initialize()
    yield client
    await client.cleanup()


@pytest.fixture
def sample_person_data() -> Dict[str, Any]:
    """Sample person data for testing."""
    return {
        "first_name": "John",
        "last_name": "Doe",
        "email": "john.doe@test.com",
        "phone": "+1-555-0001",
        "address": "123 Test Street"
    }


@pytest.fixture
def sample_student_data() -> Dict[str, Any]:
    """Sample student data for testing."""
    return {
        "first_name": "Alice",
        "last_name": "Johnson",
        "email": "alice.johnson@school.test",
        "student_id": "STU001",
        "grade_level": 10,
        "guardian_contact": "parent@test.com"
    }


@pytest.fixture
def sample_teacher_data() -> Dict[str, Any]:
    """Sample teacher data for testing."""
    return {
        "first_name": "Dr. Sarah",
        "last_name": "Brown",
        "email": "sarah.brown@school.test",
        "employee_id": "EMP001",
        "subjects": ["mathematics", "statistics"],
        "department": "Mathematics",
        "qualification": "PhD in Mathematics"
    }


@pytest.fixture
def sample_class_data() -> Dict[str, Any]:
    """Sample class data for testing."""
    return {
        "name": "Advanced Mathematics",
        "academic_year": "2024-2025",
        "grade_level": 10,
        "capacity": 25,
        "location": "Room 201",
        "class_code": "MATH-10A",
        "semester": "Fall"
    }


@pytest.fixture
def bulk_students_data() -> list[Dict[str, Any]]:
    """Bulk student data for testing."""
    return [
        {
            "first_name": f"Student{i}",
            "last_name": "Test",
            "email": f"student{i}@test.com",
            "student_id": f"STU{i:03d}",
            "grade_level": 9 + (i % 4)
        }
        for i in range(1, 11)
    ]


@pytest.fixture
def sample_scores_data() -> list[Dict[str, Any]]:
    """Sample scores data for testing."""
    return [
        {
            "subject": "mathematics",
            "score": 85.5,
            "max_score": 100.0,
            "assessment_type": "midterm"
        },
        {
            "subject": "physics",
            "score": 92.0,
            "max_score": 100.0,
            "assessment_type": "quiz"
        }
    ]
