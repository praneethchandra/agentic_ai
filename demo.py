"""
Comprehensive demo script for the Data Source Interface MCP Server and Client architecture.
Demonstrates CRUD operations, bulk operations, and aggregations across different database backends.
"""

import asyncio
import os
import json
from datetime import datetime
from typing import Dict, Any

# Set environment variables for demo
os.environ['DATABASE_TYPE'] = 'mongodb'  # Change to 'elasticsearch' or 'postgresql' to test other backends
os.environ['DATABASE_CONNECTION_STRING'] = 'mongodb://localhost:27017'
os.environ['DATABASE_NAME'] = 'school_management_demo'

from data_source_interface import DataSourceMCPClient


class DataSourceDemo:
    """Demo class to showcase the Data Source Interface functionality."""
    
    def __init__(self):
        self.client = DataSourceMCPClient()
        self.created_entities = {
            'students': [],
            'teachers': [],
            'classes': [],
            'persons': []
        }
    
    async def initialize(self):
        """Initialize the demo."""
        print("🚀 Initializing Data Source Interface Demo...")
        await self.client.initialize()
        print("✅ Demo initialized successfully!")
    
    async def cleanup(self):
        """Cleanup demo resources."""
        print("\n🧹 Cleaning up demo resources...")
        await self.client.cleanup()
        print("✅ Cleanup completed!")
    
    def print_result(self, operation: str, result: Dict[str, Any]):
        """Print operation results in a formatted way."""
        print(f"\n📋 {operation}")
        print("=" * 50)
        if result.get('success'):
            print(f"✅ Success: {result.get('message', 'Operation completed')}")
            if result.get('data'):
                if isinstance(result['data'], dict):
                    for key, value in result['data'].items():
                        if key not in ['created_at', 'updated_at']:
                            print(f"   {key}: {value}")
                else:
                    print(f"   Data: {result['data']}")
        else:
            print(f"❌ Failed: {result.get('message', 'Operation failed')}")
            if result.get('errors'):
                for error in result['errors']:
                    print(f"   Error: {error}")
    
    async def demo_person_operations(self):
        """Demo person CRUD operations."""
        print("\n" + "="*60)
        print("👤 PERSON OPERATIONS DEMO")
        print("="*60)
        
        # Create a person
        person_result = await self.client.create_person(
            first_name="Alice",
            last_name="Johnson",
            email="alice.johnson@example.com",
            phone="+1-555-0101",
            address="123 Main St, Anytown, USA"
        )
        self.print_result("Creating Person", person_result)
        
        if person_result['success']:
            person_id = person_result['data']['id']
            self.created_entities['persons'].append(person_id)
            
            # Get the person
            get_result = await self.client.get_person(person_id)
            self.print_result("Getting Person", get_result)
            
            # Update the person
            update_result = await self.client.update_person(
                person_id,
                phone="+1-555-0102",
                address="456 Oak Ave, Newtown, USA"
            )
            self.print_result("Updating Person", update_result)
    
    async def demo_student_operations(self):
        """Demo student CRUD operations."""
        print("\n" + "="*60)
        print("🎓 STUDENT OPERATIONS DEMO")
        print("="*60)
        
        # Create students
        students_data = [
            {
                "first_name": "John",
                "last_name": "Doe",
                "email": "john.doe@school.edu",
                "student_id": "STU001",
                "grade_level": 10,
                "guardian_contact": "parent1@example.com"
            },
            {
                "first_name": "Jane",
                "last_name": "Smith",
                "email": "jane.smith@school.edu",
                "student_id": "STU002",
                "grade_level": 10,
                "guardian_contact": "parent2@example.com"
            },
            {
                "first_name": "Bob",
                "last_name": "Wilson",
                "email": "bob.wilson@school.edu",
                "student_id": "STU003",
                "grade_level": 11,
                "guardian_contact": "parent3@example.com"
            }
        ]
        
        for student_data in students_data:
            result = await self.client.create_student(**student_data)
            self.print_result(f"Creating Student {student_data['first_name']}", result)
            if result['success']:
                self.created_entities['students'].append(result['data']['id'])
    
    async def demo_teacher_operations(self):
        """Demo teacher CRUD operations."""
        print("\n" + "="*60)
        print("👨‍🏫 TEACHER OPERATIONS DEMO")
        print("="*60)
        
        # Create teachers
        teachers_data = [
            {
                "first_name": "Dr. Sarah",
                "last_name": "Brown",
                "email": "sarah.brown@school.edu",
                "employee_id": "EMP001",
                "subjects": ["mathematics", "statistics"],
                "department": "Mathematics",
                "qualification": "PhD in Mathematics"
            },
            {
                "first_name": "Prof. Michael",
                "last_name": "Davis",
                "email": "michael.davis@school.edu",
                "employee_id": "EMP002",
                "subjects": ["physics", "chemistry"],
                "department": "Science",
                "qualification": "PhD in Physics"
            }
        ]
        
        for teacher_data in teachers_data:
            result = await self.client.create_teacher(**teacher_data)
            self.print_result(f"Creating Teacher {teacher_data['first_name']}", result)
            if result['success']:
                self.created_entities['teachers'].append(result['data']['id'])
    
    async def demo_class_operations(self):
        """Demo class CRUD operations."""
        print("\n" + "="*60)
        print("🏫 CLASS OPERATIONS DEMO")
        print("="*60)
        
        # Create classes
        classes_data = [
            {
                "name": "Advanced Mathematics",
                "academic_year": "2024-2025",
                "grade_level": 10,
                "capacity": 25,
                "location": "Room 201",
                "class_code": "MATH-10A",
                "semester": "Fall"
            },
            {
                "name": "Physics Fundamentals",
                "academic_year": "2024-2025",
                "grade_level": 11,
                "capacity": 20,
                "location": "Lab 301",
                "class_code": "PHYS-11A",
                "semester": "Fall"
            }
        ]
        
        for class_data in classes_data:
            result = await self.client.create_class(**class_data)
            self.print_result(f"Creating Class {class_data['name']}", result)
            if result['success']:
                self.created_entities['classes'].append(result['data']['id'])
    
    async def demo_relationship_operations(self):
        """Demo relationship operations (students to classes, teachers to classes)."""
        print("\n" + "="*60)
        print("🔗 RELATIONSHIP OPERATIONS DEMO")
        print("="*60)
        
        if not (self.created_entities['students'] and self.created_entities['classes'] and self.created_entities['teachers']):
            print("❌ Cannot demo relationships - missing required entities")
            return
        
        # Add students to first class
        class_id = self.created_entities['classes'][0]
        student_ids = self.created_entities['students'][:2]  # First 2 students
        
        result = await self.client.add_students_to_class(class_id, student_ids)
        self.print_result("Adding Students to Class", result)
        
        # Add teacher to class
        if self.created_entities['teachers']:
            teacher_id = self.created_entities['teachers'][0]
            result = await self.client.add_teacher_to_class(class_id, teacher_id, "mathematics")
            self.print_result("Adding Teacher to Class", result)
    
    async def demo_scoring_operations(self):
        """Demo adding scores to students."""
        print("\n" + "="*60)
        print("📊 SCORING OPERATIONS DEMO")
        print("="*60)
        
        if not (self.created_entities['students'] and self.created_entities['classes']):
            print("❌ Cannot demo scoring - missing required entities")
            return
        
        # Create sample scores
        scores_data = []
        for i, student_id in enumerate(self.created_entities['students'][:2]):
            scores_data.append({
                "student_id": student_id,
                "class_id": self.created_entities['classes'][0],
                "subject": "mathematics",
                "score": 85.5 + (i * 5),  # Different scores for each student
                "max_score": 100.0,
                "assessment_type": "midterm_exam",
                "assessment_date": datetime.utcnow().isoformat()
            })
        
        result = await self.client.add_scores_to_students(scores_data)
        self.print_result("Adding Scores to Students", result)
    
    async def demo_bulk_operations(self):
        """Demo bulk operations."""
        print("\n" + "="*60)
        print("📦 BULK OPERATIONS DEMO")
        print("="*60)
        
        # Bulk create students
        bulk_students = [
            {
                "first_name": f"Student{i}",
                "last_name": f"Bulk{i}",
                "email": f"student{i}@bulk.edu",
                "student_id": f"BULK{i:03d}",
                "grade_level": 9 + (i % 3)
            }
            for i in range(1, 6)  # Create 5 students
        ]
        
        result = await self.client.bulk_operation(
            operation_type="create",
            entity_type="student",
            data=bulk_students,
            batch_size=3
        )
        self.print_result("Bulk Creating Students", result)
    
    async def demo_aggregate_operations(self):
        """Demo aggregate operations."""
        print("\n" + "="*60)
        print("📈 AGGREGATE OPERATIONS DEMO")
        print("="*60)
        
        # Get students per class
        result = await self.client.get_students_per_class()
        self.print_result("Students Per Class", result)
        
        # Get average scores per class
        result = await self.client.get_avg_score_per_class()
        self.print_result("Average Scores Per Class", result)
        
        # Get teachers per class
        result = await self.client.get_teachers_per_class()
        self.print_result("Teachers Per Class", result)
        
        # Get subjects per class
        result = await self.client.get_subjects_per_class()
        self.print_result("Subjects Per Class", result)
        
        # Custom aggregate query
        result = await self.client.aggregate_query(
            query_type="students",
            filters={"grade_level": 10},
            sort_by="last_name",
            sort_order="asc",
            limit=5
        )
        self.print_result("Custom Aggregate Query (Grade 10 Students)", result)
    
    async def run_full_demo(self):
        """Run the complete demo."""
        try:
            await self.initialize()
            
            print(f"\n🎯 Running demo with {os.environ.get('DATABASE_TYPE', 'mongodb').upper()} backend")
            
            # Run all demo operations
            await self.demo_person_operations()
            await self.demo_student_operations()
            await self.demo_teacher_operations()
            await self.demo_class_operations()
            await self.demo_relationship_operations()
            await self.demo_scoring_operations()
            await self.demo_bulk_operations()
            await self.demo_aggregate_operations()
            
            print("\n" + "="*60)
            print("🎉 DEMO COMPLETED SUCCESSFULLY!")
            print("="*60)
            print(f"Created entities summary:")
            for entity_type, ids in self.created_entities.items():
                print(f"  {entity_type}: {len(ids)} created")
            
        except Exception as e:
            print(f"\n❌ Demo failed with error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await self.cleanup()


async def main():
    """Main function to run the demo."""
    print("🌟 Welcome to the Data Source Interface MCP Demo!")
    print("This demo showcases CRUD operations, bulk operations, and aggregations")
    print("across different database backends (MongoDB, Elasticsearch, PostgreSQL)")
    
    demo = DataSourceDemo()
    await demo.run_full_demo()


if __name__ == "__main__":
    asyncio.run(main())
