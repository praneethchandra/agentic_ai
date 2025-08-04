"""
MCP Server implementation for the Data Source Interface application.
Provides tools and resources for CRUD operations, bulk operations, and aggregations.
"""

import asyncio
import json
import os
import sys
from typing import Any, Dict, List, Optional

from .models import (
    Person, Student, Teacher, Class, Score, BulkOperation, AggregateQuery,
    SubjectEnum
)
from .database_interface import DatabaseInterface, MongoDBInterface
from .elasticsearch_interface import ElasticsearchInterface
from .postgresql_interface import PostgreSQLInterface


class DataSourceMCPServer:
    """MCP Server for Data Source Interface operations."""
    
    def __init__(self):
        self.db_interface: Optional[DatabaseInterface] = None
        self.db_type = os.getenv('DATABASE_TYPE', 'mongodb')
        self.connection_string = os.getenv('DATABASE_CONNECTION_STRING', 'mongodb://localhost:27017')
        self.database_name = os.getenv('DATABASE_NAME', 'school_management')
    
    async def initialize(self):
        """Initialize the database connection."""
        try:
            if self.db_type.lower() == 'mongodb':
                self.db_interface = MongoDBInterface(self.connection_string, self.database_name)
            elif self.db_type.lower() == 'elasticsearch':
                hosts = [self.connection_string]
                self.db_interface = ElasticsearchInterface(hosts, self.database_name)
            elif self.db_type.lower() == 'postgresql':
                self.db_interface = PostgreSQLInterface(self.connection_string)
            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")
            
            connected = await self.db_interface.connect()
            if not connected:
                raise Exception("Failed to connect to database")
            
            print(f"Connected to {self.db_type} database successfully")
        except Exception as e:
            print(f"Database initialization error: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup database connections."""
        if self.db_interface:
            await self.db_interface.disconnect()
    
    # Person operations
    async def create_person(self, person_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new person."""
        try:
            person = Person(**person_data)
            response = await self.db_interface.create_person(person)
            return {
                "success": response.success,
                "message": response.message,
                "data": response.data.dict() if response.data else None,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to create person: {str(e)}",
                "errors": [str(e)]
            }
    
    async def get_person(self, person_id: str) -> Dict[str, Any]:
        """Get a person by ID."""
        try:
            response = await self.db_interface.get_person(person_id)
            return {
                "success": response.success,
                "message": response.message,
                "data": response.data.dict() if response.data else None,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to get person: {str(e)}",
                "errors": [str(e)]
            }
    
    async def update_person(self, person_id: str, person_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update a person."""
        try:
            person = Person(**person_data)
            response = await self.db_interface.update_person(person_id, person)
            return {
                "success": response.success,
                "message": response.message,
                "data": response.data.dict() if response.data else None,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to update person: {str(e)}",
                "errors": [str(e)]
            }
    
    async def delete_person(self, person_id: str) -> Dict[str, Any]:
        """Delete a person."""
        try:
            response = await self.db_interface.delete_person(person_id)
            return {
                "success": response.success,
                "message": response.message,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to delete person: {str(e)}",
                "errors": [str(e)]
            }
    
    # Student operations
    async def create_student(self, student_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new student."""
        try:
            student = Student(**student_data)
            response = await self.db_interface.create_student(student)
            return {
                "success": response.success,
                "message": response.message,
                "data": response.data.dict() if response.data else None,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to create student: {str(e)}",
                "errors": [str(e)]
            }
    
    async def get_student(self, student_id: str) -> Dict[str, Any]:
        """Get a student by ID."""
        try:
            response = await self.db_interface.get_student(student_id)
            return {
                "success": response.success,
                "message": response.message,
                "data": response.data.dict() if response.data else None,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to get student: {str(e)}",
                "errors": [str(e)]
            }
    
    async def update_student(self, student_id: str, student_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update a student."""
        try:
            student = Student(**student_data)
            response = await self.db_interface.update_student(student_id, student)
            return {
                "success": response.success,
                "message": response.message,
                "data": response.data.dict() if response.data else None,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to update student: {str(e)}",
                "errors": [str(e)]
            }
    
    async def delete_student(self, student_id: str) -> Dict[str, Any]:
        """Delete a student."""
        try:
            response = await self.db_interface.delete_student(student_id)
            return {
                "success": response.success,
                "message": response.message,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to delete student: {str(e)}",
                "errors": [str(e)]
            }
    
    # Teacher operations
    async def create_teacher(self, teacher_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new teacher."""
        try:
            teacher = Teacher(**teacher_data)
            response = await self.db_interface.create_teacher(teacher)
            return {
                "success": response.success,
                "message": response.message,
                "data": response.data.dict() if response.data else None,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to create teacher: {str(e)}",
                "errors": [str(e)]
            }
    
    async def get_teacher(self, teacher_id: str) -> Dict[str, Any]:
        """Get a teacher by ID."""
        try:
            response = await self.db_interface.get_teacher(teacher_id)
            return {
                "success": response.success,
                "message": response.message,
                "data": response.data.dict() if response.data else None,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to get teacher: {str(e)}",
                "errors": [str(e)]
            }
    
    async def update_teacher(self, teacher_id: str, teacher_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update a teacher."""
        try:
            teacher = Teacher(**teacher_data)
            response = await self.db_interface.update_teacher(teacher_id, teacher)
            return {
                "success": response.success,
                "message": response.message,
                "data": response.data.dict() if response.data else None,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to update teacher: {str(e)}",
                "errors": [str(e)]
            }
    
    async def delete_teacher(self, teacher_id: str) -> Dict[str, Any]:
        """Delete a teacher."""
        try:
            response = await self.db_interface.delete_teacher(teacher_id)
            return {
                "success": response.success,
                "message": response.message,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to delete teacher: {str(e)}",
                "errors": [str(e)]
            }
    
    # Class operations
    async def create_class(self, class_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new class."""
        try:
            class_obj = Class(**class_data)
            response = await self.db_interface.create_class(class_obj)
            return {
                "success": response.success,
                "message": response.message,
                "data": response.data.dict() if response.data else None,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to create class: {str(e)}",
                "errors": [str(e)]
            }
    
    async def get_class(self, class_id: str) -> Dict[str, Any]:
        """Get a class by ID."""
        try:
            response = await self.db_interface.get_class(class_id)
            return {
                "success": response.success,
                "message": response.message,
                "data": response.data.dict() if response.data else None,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to get class: {str(e)}",
                "errors": [str(e)]
            }
    
    async def update_class(self, class_id: str, class_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update a class."""
        try:
            class_obj = Class(**class_data)
            response = await self.db_interface.update_class(class_id, class_obj)
            return {
                "success": response.success,
                "message": response.message,
                "data": response.data.dict() if response.data else None,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to update class: {str(e)}",
                "errors": [str(e)]
            }
    
    async def delete_class(self, class_id: str) -> Dict[str, Any]:
        """Delete a class."""
        try:
            response = await self.db_interface.delete_class(class_id)
            return {
                "success": response.success,
                "message": response.message,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to delete class: {str(e)}",
                "errors": [str(e)]
            }
    
    # Relationship operations
    async def add_students_to_class(self, class_id: str, student_ids: List[str]) -> Dict[str, Any]:
        """Add students to a class."""
        try:
            response = await self.db_interface.add_students_to_class(class_id, student_ids)
            return {
                "success": response.success,
                "message": response.message,
                "total_processed": response.total_processed,
                "successful": response.successful,
                "failed": response.failed,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to add students to class: {str(e)}",
                "errors": [str(e)]
            }
    
    async def add_teacher_to_class(self, class_id: str, teacher_id: str, subject: str) -> Dict[str, Any]:
        """Add a teacher to a class for a specific subject."""
        try:
            response = await self.db_interface.add_teacher_to_class(class_id, teacher_id, subject)
            return {
                "success": response.success,
                "message": response.message,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to add teacher to class: {str(e)}",
                "errors": [str(e)]
            }
    
    async def add_scores_to_students(self, scores_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Add scores to students."""
        try:
            scores = [Score(**score_data) for score_data in scores_data]
            response = await self.db_interface.add_scores_to_students(scores)
            return {
                "success": response.success,
                "message": response.message,
                "total_processed": response.total_processed,
                "successful": response.successful,
                "failed": response.failed,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to add scores: {str(e)}",
                "errors": [str(e)]
            }
    
    # Bulk operations
    async def bulk_operation(self, operation_data: Dict[str, Any]) -> Dict[str, Any]:
        """Perform bulk operations."""
        try:
            operation = BulkOperation(**operation_data)
            response = await self.db_interface.bulk_operation(operation)
            return {
                "success": response.success,
                "message": response.message,
                "total_processed": response.total_processed,
                "successful": response.successful,
                "failed": response.failed,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to perform bulk operation: {str(e)}",
                "errors": [str(e)]
            }
    
    # Aggregate operations
    async def get_students_per_class(self, class_id: Optional[str] = None) -> Dict[str, Any]:
        """Get students per class."""
        try:
            response = await self.db_interface.get_students_per_class(class_id)
            return {
                "success": response.success,
                "message": response.message,
                "data": response.data,
                "count": response.count,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to get students per class: {str(e)}",
                "errors": [str(e)]
            }
    
    async def get_avg_score_per_class(self, class_id: Optional[str] = None) -> Dict[str, Any]:
        """Get average score per class."""
        try:
            response = await self.db_interface.get_avg_score_per_class(class_id)
            return {
                "success": response.success,
                "message": response.message,
                "data": response.data,
                "count": response.count,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to get average scores per class: {str(e)}",
                "errors": [str(e)]
            }
    
    async def get_teachers_per_class(self, class_id: Optional[str] = None) -> Dict[str, Any]:
        """Get teachers per class."""
        try:
            response = await self.db_interface.get_teachers_per_class(class_id)
            return {
                "success": response.success,
                "message": response.message,
                "data": response.data,
                "count": response.count,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to get teachers per class: {str(e)}",
                "errors": [str(e)]
            }
    
    async def get_subjects_per_class(self, class_id: Optional[str] = None) -> Dict[str, Any]:
        """Get subjects per class."""
        try:
            response = await self.db_interface.get_subjects_per_class(class_id)
            return {
                "success": response.success,
                "message": response.message,
                "data": response.data,
                "count": response.count,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to get subjects per class: {str(e)}",
                "errors": [str(e)]
            }
    
    async def aggregate_query(self, query_data: Dict[str, Any]) -> Dict[str, Any]:
        """Perform custom aggregate queries."""
        try:
            query = AggregateQuery(**query_data)
            response = await self.db_interface.aggregate_query(query)
            return {
                "success": response.success,
                "message": response.message,
                "data": response.data,
                "count": response.count,
                "errors": response.errors
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to perform aggregate query: {str(e)}",
                "errors": [str(e)]
            }


# Global server instance
server = DataSourceMCPServer()


async def main():
    """Main function to run the MCP server."""
    try:
        await server.initialize()
        print("Data Source MCP Server initialized successfully")
        
        # Keep the server running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("Shutting down server...")
    except Exception as e:
        print(f"Server error: {e}")
    finally:
        await server.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
