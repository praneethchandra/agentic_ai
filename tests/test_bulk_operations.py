"""
Integration tests for bulk operations across all database backends.
"""
import pytest
from typing import Dict, Any, List


class TestBulkOperations:
    """Test bulk operations for all database backends."""

    @pytest.mark.asyncio
    async def test_bulk_create_students(self, database_interface, bulk_students_data):
        """Test bulk creation of students."""
        interface, db_type = database_interface
        
        # Bulk create students
        result = await interface.bulk_operation(
            operation_type="create",
            entity_type="student",
            data=bulk_students_data,
            batch_size=5
        )
        
        assert result["success"] is True
        assert result["processed"] == len(bulk_students_data)
        assert len(result["created_ids"]) == len(bulk_students_data)
        
        print(f"✓ [{db_type}] Bulk created {len(bulk_students_data)} students")
        
        # Verify students were created
        for student_id in result["created_ids"]:
            student = await interface.get_student(student_id)
            assert student is not None
        
        print(f"✓ [{db_type}] All bulk created students verified")
        
        # Cleanup
        for student_id in result["created_ids"]:
            await interface.delete_student(student_id)

    @pytest.mark.asyncio
    async def test_bulk_update_students(self, database_interface, bulk_students_data):
        """Test bulk update of students."""
        interface, db_type = database_interface
        
        # First create students
        create_result = await interface.bulk_operation(
            operation_type="create",
            entity_type="student",
            data=bulk_students_data,
            batch_size=5
        )
        
        student_ids = create_result["created_ids"]
        
        # Prepare update data
        update_data = []
        for i, student_id in enumerate(student_ids):
            update_data.append({
                "id": student_id,
                "grade_level": 12,
                "guardian_contact": f"updated_parent{i}@test.com"
            })
        
        # Bulk update students
        update_result = await interface.bulk_operation(
            operation_type="update",
            entity_type="student",
            data=update_data,
            batch_size=3
        )
        
        assert update_result["success"] is True
        assert update_result["processed"] == len(update_data)
        
        print(f"✓ [{db_type}] Bulk updated {len(update_data)} students")
        
        # Verify updates
        for student_id in student_ids:
            student = await interface.get_student(student_id)
            assert student["grade_level"] == 12
            assert "updated_parent" in student["guardian_contact"]
        
        print(f"✓ [{db_type}] All bulk updates verified")
        
        # Cleanup
        for student_id in student_ids:
            await interface.delete_student(student_id)

    @pytest.mark.asyncio
    async def test_bulk_delete_students(self, database_interface, bulk_students_data):
        """Test bulk deletion of students."""
        interface, db_type = database_interface
        
        # First create students
        create_result = await interface.bulk_operation(
            operation_type="create",
            entity_type="student",
            data=bulk_students_data,
            batch_size=5
        )
        
        student_ids = create_result["created_ids"]
        
        # Bulk delete students
        delete_result = await interface.bulk_operation(
            operation_type="delete",
            entity_type="student",
            data=[{"id": sid} for sid in student_ids],
            batch_size=4
        )
        
        assert delete_result["success"] is True
        assert delete_result["processed"] == len(student_ids)
        
        print(f"✓ [{db_type}] Bulk deleted {len(student_ids)} students")
        
        # Verify deletions
        for student_id in student_ids:
            student = await interface.get_student(student_id)
            assert student is None
        
        print(f"✓ [{db_type}] All bulk deletions verified")

    @pytest.mark.asyncio
    async def test_bulk_mixed_entities(self, database_interface):
        """Test bulk operations with mixed entity types."""
        interface, db_type = database_interface
        
        # Create test data for different entities
        persons_data = [
            {
                "first_name": f"Person{i}",
                "last_name": "Test",
                "email": f"person{i}@test.com"
            }
            for i in range(1, 4)
        ]
        
        teachers_data = [
            {
                "first_name": f"Teacher{i}",
                "last_name": "Test",
                "email": f"teacher{i}@test.com",
                "employee_id": f"EMP{i:03d}",
                "subjects": ["mathematics"],
                "department": "Math"
            }
            for i in range(1, 4)
        ]
        
        # Bulk create persons
        persons_result = await interface.bulk_operation(
            operation_type="create",
            entity_type="person",
            data=persons_data,
            batch_size=2
        )
        
        # Bulk create teachers
        teachers_result = await interface.bulk_operation(
            operation_type="create",
            entity_type="teacher",
            data=teachers_data,
            batch_size=2
        )
        
        assert persons_result["success"] is True
        assert teachers_result["success"] is True
        assert len(persons_result["created_ids"]) == 3
        assert len(teachers_result["created_ids"]) == 3
        
        print(f"✓ [{db_type}] Bulk created mixed entities successfully")
        
        # Cleanup
        for person_id in persons_result["created_ids"]:
            await interface.delete_person(person_id)
        for teacher_id in teachers_result["created_ids"]:
            await interface.delete_teacher(teacher_id)

    @pytest.mark.asyncio
    async def test_bulk_operation_error_handling(self, database_interface):
        """Test bulk operation error handling."""
        interface, db_type = database_interface
        
        # Create data with duplicate emails (should cause errors)
        duplicate_data = [
            {
                "first_name": "Student1",
                "last_name": "Test",
                "email": "duplicate@test.com",
                "student_id": "STU001"
            },
            {
                "first_name": "Student2",
                "last_name": "Test",
                "email": "duplicate@test.com",  # Duplicate email
                "student_id": "STU002"
            }
        ]
        
        # This should handle errors gracefully
        result = await interface.bulk_operation(
            operation_type="create",
            entity_type="student",
            data=duplicate_data,
            batch_size=2
        )
        
        # Should have partial success or proper error handling
        assert "success" in result
        assert "processed" in result
        assert "errors" in result or result["processed"] < len(duplicate_data)
        
        print(f"✓ [{db_type}] Bulk operation error handling works correctly")
        
        # Cleanup any successfully created records
        if "created_ids" in result:
            for student_id in result["created_ids"]:
                try:
                    await interface.delete_student(student_id)
                except:
                    pass  # Ignore cleanup errors

    @pytest.mark.asyncio
    async def test_bulk_operation_batch_sizes(self, database_interface):
        """Test bulk operations with different batch sizes."""
        interface, db_type = database_interface
        
        # Create test data
        test_data = [
            {
                "first_name": f"BatchTest{i}",
                "last_name": "Student",
                "email": f"batch{i}@test.com",
                "student_id": f"BATCH{i:03d}"
            }
            for i in range(1, 8)  # 7 students
        ]
        
        # Test with batch size 3 (should create 3 batches: 3, 3, 1)
        result = await interface.bulk_operation(
            operation_type="create",
            entity_type="student",
            data=test_data,
            batch_size=3
        )
        
        assert result["success"] is True
        assert result["processed"] == 7
        assert len(result["created_ids"]) == 7
        
        print(f"✓ [{db_type}] Bulk operation with batch size 3 successful")
        
        # Cleanup
        for student_id in result["created_ids"]:
            await interface.delete_student(student_id)
