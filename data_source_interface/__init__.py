"""
Data Source Interface package for MCP Server and Client architecture.
Provides CRUD operations, bulk operations, and aggregations for NoSQL databases.
"""

from .models import (
    Person, Student, Teacher, Class, ClassEnrollment, TeacherAssignment, Score,
    BulkOperation, AggregateQuery, SubjectEnum, GatheringTypeEnum,
    PersonResponse, ClassResponse, BulkOperationResponse, AggregateResponse
)

from .database_interface import DatabaseInterface
from .database_interface import MongoDBInterface
from .elasticsearch_interface import ElasticsearchInterface
from .postgresql_interface import PostgreSQLInterface
from .mcp_server import DataSourceMCPServer
from .mcp_client import DataSourceMCPClient

__version__ = "1.0.0"
__author__ = "Data Source Interface Team"
__description__ = "MCP Server and Client architecture for Data Source Interface application"

__all__ = [
    # Models
    "Person", "Student", "Teacher", "Class", "ClassEnrollment", "TeacherAssignment", "Score",
    "BulkOperation", "AggregateQuery", "SubjectEnum", "GatheringTypeEnum",
    "PersonResponse", "ClassResponse", "BulkOperationResponse", "AggregateResponse",
    
    # Database Interfaces
    "DatabaseInterface", "MongoDBInterface", "ElasticsearchInterface", "PostgreSQLInterface",
    
    # MCP Components
    "DataSourceMCPServer", "DataSourceMCPClient"
]
