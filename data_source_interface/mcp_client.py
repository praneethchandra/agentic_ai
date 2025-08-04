"""
MCP Client implementation for the Data Source Interface application.
Provides a client interface to interact with the MCP server.
"""

import asyncio
import json
from typing import Any, Dict, List, Optional
from datetime import datetime

from .mcp_server import DataSourceMCPServer


class DataSourceMCPClient:
    """MCP Client for Data Source Interface operations."""
    
    def __init__(self):
        self.server = DataSourceMCPServer()
    
    async def initialize(self):
        """Initialize the client and server connection."""
        await self.server.initialize()
    
    async def cleanup(self):
        """Cleanup connections."""
        await self.server.cleanup()
    
    # Person operations
    async def create_person(self, first_name: str, last_name: str, email: str, 
                          phone: Optional[str] = None, date_of_birth: Optional[str] = None,
                          address: Optional[str] = None) -> Dict[str, Any]:
        """Create a new person."""
        person_data = {
            "first_name": first_name,
            "last_name": last_name,
            "email": email
        }
        if phone:
            person_data["phone"] = phone
        if date_of_birth:
            person_data["date_of_birth"] = date_of_birth
        if address:
            person_data["address"] = address
        
        return await self.server.create_person(person_data)
    
    async def get_person(self, person_id: str) -> Dict[str, Any]:
        """Get a person by ID."""
        return await self.server.get_person(person_id)
    
    async def update_person(self, person_id: str, **kwargs) -> Dict[str, Any]:
        """Update a person."""
        return await self.server.update_person(person_id, kwargs)
    
    async def delete_person(self, person_id: str) -> Dict[str, Any]:
        """Delete a person."""
        return await self.server.delete_person(person_id)
    
    # Student operations
    async def create_student(self, first_name: str, last_name: str, email: str,
                           student_id: Optional[str] = None, grade_level: Optional[int] = None,
                           phone: Optional[str] = None, date_of_birth: Optional[str] = None,
                           address: Optional[str] = None, guardian_contact: Optional[str] = None) -> Dict[str, Any]:
        """Create a new student."""
        student_data = {
            "first_name": first_name,
            "last_name": last_name,
            "email": email
        }
        if student_id:
            student_data["student_id"] = student_id
        if grade_level:
            student_data["grade_level"] = grade_level
        if phone:
            student_data["phone"] = phone
        if date_of_birth:
            student_data["date_of_birth"] = date_of_birth
        if address:
            student_data["address"] = address
        if guardian_contact:
            student_data["guardian_contact"] = guardian_contact
        
        return await self.server.create_student(student_data)
    
    async def get_student(self, student_id: str) -> Dict[str, Any]:
        """Get a student by ID."""
        return await self.server.get_student(student_id)
    
    async def update_student(self, student_id: str, **kwargs) -> Dict[str, Any]:
        """Update a student."""
        return await self.server.update_student(student_id, kwargs)
    
    async def delete_student(self, student_id: str) -> Dict[str, Any]:
        """Delete a student."""
        return await self.server.delete_student(student_id)
    
    # Teacher operations
    async def create_teacher(self, first_name: str, last_name: str, email: str,
                           employee_id: Optional[str] = None, subjects: Optional[List[str]] = None,
                           phone: Optional[str] = None, date_of_birth: Optional[str] = None,
                           address: Optional[str] = None, department: Optional[str] = None,
                           qualification: Optional[str] = None) -> Dict[str, Any]:
        """Create a new teacher."""
        teacher_data = {
            "first_name": first_name,
            "last_name": last_name,
            "email": email
        }
        if employee_id:
            teacher_data["employee_id"] = employee_id
        if subjects:
            teacher_data["subjects"] = subjects
        if phone:
            teacher_data["phone"] = phone
        if date_of_birth:
            teacher_data["date_of_birth"] = date_of_birth
        if address:
            teacher_data["address"] = address
        if department:
            teacher_data["department"] = department
        if qualification:
            teacher_data["qualification"] = qualification
        
        return await self.server.create_teacher(teacher_data)
    
    async def get_teacher(self, teacher_id: str) -> Dict[str, Any]:
        """Get a teacher by ID."""
        return await self.server.get_teacher(teacher_id)
    
    async def update_teacher(self, teacher_id: str, **kwargs) -> Dict[str, Any]:
        """Update a teacher."""
        return await self.server.update_teacher(teacher_id, kwargs)
    
    async def delete_teacher(self, teacher_id: str) -> Dict[str, Any]:
        """Delete a teacher."""
        return await self.server.delete_teacher(teacher_id)
    
    # Class operations
    async def create_class(self, name: str, academic_year: str, grade_level: Optional[int] = None,
                         description: Optional[str] = None, capacity: Optional[int] = None,
                         location: Optional[str] = None, class_code: Optional[str] = None,
                         semester: Optional[str] = None) -> Dict[str, Any]:
        """Create a new class."""
        class_data = {
            "name": name,
            "academic_year": academic_year
        }
        if grade_level:
            class_data["grade_level"] = grade_level
        if description:
            class_data["description"] = description
        if capacity:
            class_data["capacity"] = capacity
        if location:
            class_data["location"] = location
        if class_code:
            class_data["class_code"] = class_code
        if semester:
            class_data["semester"] = semester
        
        return await self.server.create_class(class_data)
    
    async def get_class(self, class_id: str) -> Dict[str, Any]:
        """Get a class by ID."""
        return await self.server.get_class(class_id)
    
    async def update_class(self, class_id: str, **kwargs) -> Dict[str, Any]:
        """Update a class."""
        return await self.server.update_class(class_id, kwargs)
    
    async def delete_class(self, class_id: str) -> Dict[str, Any]:
        """Delete a class."""
        return await self.server.delete_class(class_id)
    
    # Relationship operations
    async def add_students_to_class(self, class_id: str, student_ids: List[str]) -> Dict[str, Any]:
        """Add students to a class."""
        return await self.server.add_students_to_class(class_id, student_ids)
    
    async def add_teacher_to_class(self, class_id: str, teacher_id: str, subject: str) -> Dict[str, Any]:
        """Add a teacher to a class for a specific subject."""
        return await self.server.add_teacher_to_class(class_id, teacher_id, subject)
    
    async def add_scores_to_students(self, scores_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Add scores to students."""
        return await self.server.add_scores_to_students(scores_data)
    
    # Bulk operations
    async def bulk_operation(self, operation_type: str, entity_type: str, 
                           data: List[Dict[str, Any]], batch_size: int = 100) -> Dict[str, Any]:
        """Perform bulk operations."""
        operation_data = {
            "operation_type": operation_type,
            "entity_type": entity_type,
            "data": data,
            "batch_size": batch_size
        }
        return await self.server.bulk_operation(operation_data)
    
    # Aggregate operations
    async def get_students_per_class(self, class_id: Optional[str] = None) -> Dict[str, Any]:
        """Get students per class."""
        return await self.server.get_students_per_class(class_id)
    
    async def get_avg_score_per_class(self, class_id: Optional[str] = None) -> Dict[str, Any]:
        """Get average score per class."""
        return await self.server.get_avg_score_per_class(class_id)
    
    async def get_teachers_per_class(self, class_id: Optional[str] = None) -> Dict[str, Any]:
        """Get teachers per class."""
        return await self.server.get_teachers_per_class(class_id)
    
    async def get_subjects_per_class(self, class_id: Optional[str] = None) -> Dict[str, Any]:
        """Get subjects per class."""
        return await self.server.get_subjects_per_class(class_id)
    
    async def aggregate_query(self, query_type: str, filters: Optional[Dict[str, Any]] = None,
                            group_by: Optional[List[str]] = None, sort_by: Optional[str] = None,
                            sort_order: str = "asc", limit: Optional[int] = None) -> Dict[str, Any]:
        """Perform custom aggregate queries."""
        query_data = {
            "query_type": query_type,
            "sort_order": sort_order
        }
        if filters:
            query_data["filters"] = filters
        if group_by:
            query_data["group_by"] = group_by
        if sort_by:
            query_data["sort_by"] = sort_by
        if limit:
            query_data["limit"] = limit
        
        return await self.server.aggregate_query(query_data)


async def main():
    """Example usage of the MCP client."""
    client = DataSourceMCPClient()
    
    try:
        await client.initialize()
        print("MCP Client initialized successfully")
        
        # Example operations
        print("\n=== Creating a student ===")
        student_result = await client.create_student(
            first_name="John",
            last_name="Doe",
            email="john.doe@example.com",
            student_id="STU001",
            grade_level=10
        )
        print(f"Student creation result: {student_result}")
        
        if student_result["success"]:
            student_id = student_result["data"]["id"]
            
            print(f"\n=== Getting student {student_id} ===")
            get_result = await client.get_student(student_id)
            print(f"Get student result: {get_result}")
        
        print("\n=== Creating a teacher ===")
        teacher_result = await client.create_teacher(
            first_name="Jane",
            last_name="Smith",
            email="jane.smith@example.com",
            employee_id="EMP001",
            subjects=["mathematics", "physics"],
            department="Science"
        )
        print(f"Teacher creation result: {teacher_result}")
        
        print("\n=== Creating a class ===")
        class_result = await client.create_class(
            name="Physics 101",
            academic_year="2024-2025",
            grade_level=10,
            capacity=30,
            location="Room 101"
        )
        print(f"Class creation result: {class_result}")
        
        # More examples can be added here...
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await client.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
