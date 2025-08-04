"""
Integration tests for CRUD operations across all database backends.
"""
import pytest
from typing import Dict, Any


class TestCRUDOperations:
    """Test CRUD operations for all entities across all database backends."""

    @pytest.mark.asyncio
    async def test_person_crud(self, database_interface, sample_person_data):
        """Test complete CRUD operations for Person entity."""
        interface, db_type = database_interface
        
        # CREATE
        person_id = await interface.create_person(sample_person_data)
        assert person_id is not None
        print(f"✓ [{db_type}] Person created with ID: {person_id}")
        
        # READ
        person = await interface.get_person(person_id)
        assert person is not None
        assert person["first_name"] == sample_person_data["first_name"]
        assert person["last_name"] == sample_person_data["last_name"]
        assert person["email"] == sample_person_data["email"]
        print(f"✓ [{db_type}] Person retrieved successfully")
        
        # UPDATE
        update_data = {"phone": "+1-555-9999", "address": "456 Updated Street"}
        updated_person = await interface.update_person(person_id, update_data)
        assert updated_person["phone"] == update_data["phone"]
        assert updated_person["address"] == update_data["address"]
        print(f"✓ [{db_type}] Person updated successfully")
        
        # DELETE
        deleted = await interface.delete_person(person_id)
        assert deleted is True
        
        # Verify deletion
        deleted_person = await interface.get_person(person_id)
        assert deleted_person is None
        print(f"✓ [{db_type}] Person deleted successfully")

    @pytest.mark.asyncio
    async def test_student_crud(self, database_interface, sample_student_data):
        """Test complete CRUD operations for Student entity."""
        interface, db_type = database_interface
        
        # CREATE
        student_id = await interface.create_student(sample_student_data)
        assert student_id is not None
        print(f"✓ [{db_type}] Student created with ID: {student_id}")
        
        # READ
        student = await interface.get_student(student_id)
        assert student is not None
        assert student["first_name"] == sample_student_data["first_name"]
        assert student["student_id"] == sample_student_data["student_id"]
        assert student["grade_level"] == sample_student_data["grade_level"]
        print(f"✓ [{db_type}] Student retrieved successfully")
        
        # UPDATE
        update_data = {"grade_level": 11, "guardian_contact": "updated_parent@test.com"}
        updated_student = await interface.update_student(student_id, update_data)
        assert updated_student["grade_level"] == update_data["grade_level"]
        assert updated_student["guardian_contact"] == update_data["guardian_contact"]
        print(f"✓ [{db_type}] Student updated successfully")
        
        # DELETE
        deleted = await interface.delete_student(student_id)
        assert deleted is True
        
        # Verify deletion
        deleted_student = await interface.get_student(student_id)
        assert deleted_student is None
        print(f"✓ [{db_type}] Student deleted successfully")

    @pytest.mark.asyncio
    async def test_teacher_crud(self, database_interface, sample_teacher_data):
        """Test complete CRUD operations for Teacher entity."""
        interface, db_type = database_interface
        
        # CREATE
        teacher_id = await interface.create_teacher(sample_teacher_data)
        assert teacher_id is not None
        print(f"✓ [{db_type}] Teacher created with ID: {teacher_id}")
        
        # READ
        teacher = await interface.get_teacher(teacher_id)
        assert teacher is not None
        assert teacher["first_name"] == sample_teacher_data["first_name"]
        assert teacher["employee_id"] == sample_teacher_data["employee_id"]
        assert teacher["department"] == sample_teacher_data["department"]
        print(f"✓ [{db_type}] Teacher retrieved successfully")
        
        # UPDATE
        update_data = {
            "subjects": ["mathematics", "statistics", "calculus"],
            "qualification": "PhD in Applied Mathematics"
        }
        updated_teacher = await interface.update_teacher(teacher_id, update_data)
        assert len(updated_teacher["subjects"]) == 3
        assert "calculus" in updated_teacher["subjects"]
        print(f"✓ [{db_type}] Teacher updated successfully")
        
        # DELETE
        deleted = await interface.delete_teacher(teacher_id)
        assert deleted is True
        
        # Verify deletion
        deleted_teacher = await interface.get_teacher(teacher_id)
        assert deleted_teacher is None
        print(f"✓ [{db_type}] Teacher deleted successfully")

    @pytest.mark.asyncio
    async def test_class_crud(self, database_interface, sample_class_data):
        """Test complete CRUD operations for Class entity."""
        interface, db_type = database_interface
        
        # CREATE
        class_id = await interface.create_class(sample_class_data)
        assert class_id is not None
        print(f"✓ [{db_type}] Class created with ID: {class_id}")
        
        # READ
        class_obj = await interface.get_class(class_id)
        assert class_obj is not None
        assert class_obj["name"] == sample_class_data["name"]
        assert class_obj["academic_year"] == sample_class_data["academic_year"]
        assert class_obj["class_code"] == sample_class_data["class_code"]
        print(f"✓ [{db_type}] Class retrieved successfully")
        
        # UPDATE
        update_data = {
            "capacity": 30,
            "location": "Room 301",
            "schedule": {"monday": "09:00-10:30", "wednesday": "09:00-10:30"}
        }
        updated_class = await interface.update_class(class_id, update_data)
        assert updated_class["capacity"] == update_data["capacity"]
        assert updated_class["location"] == update_data["location"]
        print(f"✓ [{db_type}] Class updated successfully")
        
        # DELETE
        deleted = await interface.delete_class(class_id)
        assert deleted is True
        
        # Verify deletion
        deleted_class = await interface.get_class(class_id)
        assert deleted_class is None
        print(f"✓ [{db_type}] Class deleted successfully")

    @pytest.mark.asyncio
    async def test_unique_constraints(self, database_interface, sample_student_data):
        """Test unique constraint enforcement."""
        interface, db_type = database_interface
        
        # Create first student
        student_id1 = await interface.create_student(sample_student_data)
        assert student_id1 is not None
        
        # Try to create another student with same email
        duplicate_data = sample_student_data.copy()
        duplicate_data["student_id"] = "STU002"
        
        with pytest.raises(Exception):  # Should raise constraint violation
            await interface.create_student(duplicate_data)
        
        print(f"✓ [{db_type}] Unique constraint properly enforced")
        
        # Cleanup
        await interface.delete_student(student_id1)

    @pytest.mark.asyncio
    async def test_nonexistent_entity_operations(self, database_interface):
        """Test operations on non-existent entities."""
        interface, db_type = database_interface
        
        fake_id = "00000000-0000-0000-0000-000000000000"
        
        # Test GET operations
        assert await interface.get_person(fake_id) is None
        assert await interface.get_student(fake_id) is None
        assert await interface.get_teacher(fake_id) is None
        assert await interface.get_class(fake_id) is None
        
        # Test UPDATE operations
        update_data = {"first_name": "Updated"}
        assert await interface.update_person(fake_id, update_data) is None
        assert await interface.update_student(fake_id, update_data) is None
        assert await interface.update_teacher(fake_id, update_data) is None
        assert await interface.update_class(fake_id, update_data) is None
        
        # Test DELETE operations
        assert await interface.delete_person(fake_id) is False
        assert await interface.delete_student(fake_id) is False
        assert await interface.delete_teacher(fake_id) is False
        assert await interface.delete_class(fake_id) is False
        
        print(f"✓ [{db_type}] Non-existent entity operations handled correctly")
