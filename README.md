# Data Source Interface MCP Server and Client Architecture

A comprehensive **Model Context Protocol (MCP)** server and client implementation for managing educational data across multiple database backends (MongoDB, Elasticsearch, PostgreSQL). This project provides a unified interface for CRUD operations, bulk operations, relationship management, and aggregate queries in a school management system.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   MCP Client    │    │   MCP Server    │    │  Database Layer │
│                 │    │                 │    │                 │
│ • Python Client │◄──►│ • Tool Handler  │◄──►│ • MongoDB       │
│ • REST API      │    │ • Resource Mgmt │    │ • Elasticsearch │
│ • CLI Interface │    │ • Validation    │    │ • PostgreSQL    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Key Components

- **MCP Server**: Provides tools and resources for data operations
- **MCP Client**: Consumes MCP server capabilities via standardized protocol
- **Database Interfaces**: Unified abstraction layer for multiple database backends
- **Pydantic Models**: Type-safe data validation and serialization
- **Docker Environment**: Containerized setup for all database services

## 🚀 Features

### Core Functionality
- ✅ **CRUD Operations**: Create, Read, Update, Delete for all entities
- ✅ **Bulk Operations**: Batch processing with configurable batch sizes
- ✅ **Relationship Management**: Student-class enrollments, teacher assignments
- ✅ **Aggregate Queries**: Statistics and analytics across entities
- ✅ **Multi-Database Support**: MongoDB, Elasticsearch, PostgreSQL
- ✅ **Type Safety**: Full Pydantic model validation
- ✅ **Async/Await**: Non-blocking operations throughout

### Entity Types
- **Person**: Base entity with common attributes
- **Student**: Extends Person with academic information
- **Teacher**: Extends Person with employment details
- **Class**: Academic class/course management
- **Scores**: Student assessment tracking

### Supported Operations

#### CRUD Operations
```python
# Create entities
person_id = await client.create_person(first_name="John", last_name="Doe", email="john@example.com")
student_id = await client.create_student(first_name="Alice", last_name="Smith", student_id="STU001")
teacher_id = await client.create_teacher(first_name="Dr. Brown", employee_id="EMP001", subjects=["math"])
class_id = await client.create_class(name="Advanced Math", class_code="MATH-101")

# Read entities
person = await client.get_person(person_id)
student = await client.get_student(student_id)

# Update entities
await client.update_person(person_id, phone="+1-555-0123")
await client.update_student(student_id, grade_level=11)

# Delete entities
await client.delete_person(person_id)
```

#### Bulk Operations
```python
# Bulk create students
students_data = [
    {"first_name": "Student1", "last_name": "Test", "email": "s1@test.com", "student_id": "STU001"},
    {"first_name": "Student2", "last_name": "Test", "email": "s2@test.com", "student_id": "STU002"},
    # ... more students
]

result = await client.bulk_operation(
    operation_type="create",
    entity_type="student", 
    data=students_data,
    batch_size=10
)
```

#### Relationship Management
```python
# Add students to class
await client.add_students_to_class(class_id, [student_id1, student_id2])

# Assign teacher to class for specific subject
await client.add_teacher_to_class(class_id, teacher_id, "mathematics")

# Add scores to students
scores_data = [{
    "student_id": student_id,
    "class_id": class_id,
    "subject": "mathematics",
    "score": 85.5,
    "max_score": 100.0,
    "assessment_type": "midterm",
    "teacher_id": teacher_id
}]
await client.add_scores_to_students(scores_data)
```

#### Aggregate Queries
```python
# Get students per class
students_stats = await client.get_students_per_class(class_id)

# Get average scores per class
avg_scores = await client.get_avg_score_per_class(class_id)

# Get teachers per class
teachers_stats = await client.get_teachers_per_class(class_id)

# Get subjects per class
subjects_stats = await client.get_subjects_per_class(class_id)

# Custom aggregate query
custom_query = {
    "entity": "student",
    "group_by": "grade_level",
    "filters": {"grade_level": {"$gte": 10}},
    "aggregations": {
        "count": {"$count": {}},
        "avg_grade": {"$avg": "grade_level"}
    }
}
result = await client.aggregate_query(custom_query)
```

## 📁 Project Structure

```
data_source_interface/
├── __init__.py                 # Package initialization
├── models.py                   # Pydantic data models
├── database_interface.py       # MongoDB implementation
├── elasticsearch_interface.py  # Elasticsearch implementation
├── postgresql_interface.py     # PostgreSQL implementation
├── mcp_server.py              # MCP server implementation
└── mcp_client.py              # MCP client implementation

tests/
├── __init__.py                # Test package initialization
├── conftest.py                # Pytest configuration and fixtures
├── test_crud_operations.py    # CRUD operation tests
├── test_bulk_operations.py    # Bulk operation tests
├── test_relationships.py      # Relationship management tests
├── test_aggregates.py         # Aggregate query tests
└── test_mcp_client.py         # MCP client tests

docker/
├── mongodb/
│   └── init.js                # MongoDB initialization script
└── postgresql/
    └── init.sql               # PostgreSQL initialization script

├── docker-compose.yml         # Docker services configuration
├── Dockerfile                 # Application container
├── requirements.txt           # Python dependencies
├── demo.py                    # Usage demonstration
├── run_tests.sh              # Test runner script
└── README.md                 # This file
```

## 🛠️ Setup and Installation

### Prerequisites
- Docker and Docker Compose
- Python 3.11+ (for local development)
- Git

### Quick Start

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd data_source_interface
   ```

2. **Start the services**
   ```bash
   # Make the test script executable
   chmod +x run_tests.sh
   
   # Start services and run tests
   ./run_tests.sh
   ```

3. **Or start services manually**
   ```bash
   # Start all database services
   docker-compose up -d
   
   # Wait for services to be ready (about 30-60 seconds)
   
   # Run the demo
   python demo.py
   ```

### Manual Setup (Development)

1. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Start database services**
   ```bash
   docker-compose up -d mongodb elasticsearch postgresql
   ```

3. **Run tests**
   ```bash
   pytest tests/ -v
   ```

## 🧪 Testing

The project includes comprehensive integration tests covering all functionality across all database backends.

### Test Categories

1. **CRUD Operations** (`test_crud_operations.py`)
   - Create, read, update, delete for all entity types
   - Unique constraint validation
   - Error handling for non-existent entities

2. **Bulk Operations** (`test_bulk_operations.py`)
   - Bulk create, update, delete operations
   - Batch size configuration
   - Error handling and partial success scenarios

3. **Relationship Management** (`test_relationships.py`)
   - Student-class enrollments
   - Teacher-class-subject assignments
   - Score management
   - Complex multi-entity scenarios

4. **Aggregate Operations** (`test_aggregates.py`)
   - Students per class statistics
   - Average scores per class and subject
   - Teachers per class analytics
   - Custom aggregate queries

5. **MCP Client** (`test_mcp_client.py`)
   - End-to-end MCP protocol testing
   - Client-server communication
   - Error handling and edge cases

### Running Tests

```bash
# Run all tests
./run_tests.sh

# Run specific test categories
pytest tests/test_crud_operations.py -v
pytest tests/test_bulk_operations.py -v
pytest tests/test_relationships.py -v
pytest tests/test_aggregates.py -v
pytest tests/test_mcp_client.py -v

# Run tests for specific database backend
pytest tests/ -v -k "mongodb"
pytest tests/ -v -k "elasticsearch" 
pytest tests/ -v -k "postgresql"
```

## 🗄️ Database Schemas

### MongoDB Collections
- `persons` - Base person entities
- `students` - Student-specific data
- `teachers` - Teacher-specific data  
- `classes` - Class/course information
- `class_enrollments` - Student-class relationships
- `teacher_assignments` - Teacher-class-subject relationships
- `scores` - Student assessment scores

### Elasticsearch Indices
- `persons` - Person documents with full-text search
- `students` - Student documents with academic metadata
- `teachers` - Teacher documents with subject expertise
- `classes` - Class documents with scheduling info
- `class_enrollments` - Enrollment relationship documents
- `teacher_assignments` - Assignment relationship documents
- `scores` - Score documents with analytics metadata

### PostgreSQL Tables
- `persons` - Person records with constraints
- `students` - Student records with foreign keys
- `teachers` - Teacher records with array fields
- `classes` - Class records with JSON metadata
- `class_enrollments` - Many-to-many enrollment table
- `teacher_assignments` - Teacher-class-subject assignments
- `scores` - Score records with referential integrity

## 🔧 Configuration

### Environment Variables
```bash
# Database connection strings
MONGODB_URL=mongodb://admin:password@localhost:27017/school_management?authSource=admin
ELASTICSEARCH_URL=http://localhost:9200
POSTGRESQL_URL=postgresql://admin:password@localhost:5432/school_management

# MCP Server configuration
MCP_SERVER_HOST=localhost
MCP_SERVER_PORT=8000

# Logging
LOG_LEVEL=INFO
```

### Docker Services
- **MongoDB**: Port 27017, with authentication
- **Elasticsearch**: Port 9200, single-node cluster
- **PostgreSQL**: Port 5432, with extensions
- **Application**: Python 3.11 with all dependencies

## 📊 Performance Considerations

### Database-Specific Optimizations

**MongoDB**
- Compound indexes on frequently queried fields
- Aggregation pipeline optimization
- Connection pooling with motor

**Elasticsearch**
- Bulk indexing for large datasets
- Query optimization with filters
- Index templates for consistent mapping

**PostgreSQL**
- B-tree indexes on foreign keys
- Partial indexes for conditional queries
- Connection pooling with asyncpg

### Bulk Operations
- Configurable batch sizes (default: 100)
- Parallel processing where possible
- Error isolation and partial success handling

## 🔒 Security Features

- Input validation with Pydantic models
- SQL injection prevention with parameterized queries
- Connection string validation
- Error message sanitization
- Database user privilege separation

## 🚀 Usage Examples

### Basic MCP Client Usage

```python
import asyncio
from data_source_interface.mcp_client import DataSourceMCPClient

async def main():
    client = DataSourceMCPClient()
    await client.initialize()
    
    try:
        # Create a student
        student_id = await client.create_student(
            first_name="Alice",
            last_name="Johnson", 
            email="alice@school.edu",
            student_id="STU001",
            grade_level=10
        )
        
        # Create a class
        class_id = await client.create_class(
            name="Advanced Mathematics",
            academic_year="2024-2025",
            class_code="MATH-101"
        )
        
        # Enroll student in class
        await client.add_students_to_class(class_id, [student_id])
        
        # Get class statistics
        stats = await client.get_students_per_class(class_id)
        print(f"Class has {stats['total_students']} students")
        
    finally:
        await client.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
```

### Direct Database Interface Usage

```python
import asyncio
from data_source_interface.database_interface import MongoDBInterface

async def main():
    # Initialize MongoDB interface
    interface = MongoDBInterface(
        "mongodb://admin:password@localhost:27017/school_management?authSource=admin",
        "school_management"
    )
    await interface.initialize()
    
    try:
        # Create and manage entities
        person_id = await interface.create_person({
            "first_name": "John",
            "last_name": "Doe", 
            "email": "john@example.com"
        })
        
        # Bulk operations
        students_data = [
            {"first_name": f"Student{i}", "last_name": "Test", 
             "email": f"student{i}@test.com", "student_id": f"STU{i:03d}"}
            for i in range(1, 11)
        ]
        
        result = await interface.bulk_operation(
            operation_type="create",
            entity_type="student",
            data=students_data,
            batch_size=5
        )
        
        print(f"Created {result['processed']} students")
        
    finally:
        await interface.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Run the test suite (`./run_tests.sh`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- **Model Context Protocol (MCP)** for the standardized protocol
- **Pydantic** for data validation and serialization
- **FastAPI** for async web framework patterns
- **Docker** for containerization
- **pytest** for comprehensive testing framework

## 📞 Support

For questions, issues, or contributions:

1. Check the [Issues](../../issues) page
2. Review the test files for usage examples
3. Run `./run_tests.sh` to verify your setup
4. Check Docker logs if services fail to start

---

**Built with ❤️ for educational data management**
