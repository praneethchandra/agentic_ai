"""
Integration tests for MCP Client functionality.
"""
import pytest
from typing import Dict, Any, List


class TestMCPClient:
    """Test MCP Client operations."""

    @pytest.mark.asyncio
    async def test_mcp_client_person_operations(self, mcp_client, sample_person_data):
        """Test MCP client person operations."""
        # CREATE
        person_id = await mcp_client.create_person(
            first_name=sample_person_data["first_name"],
            last_name=sample_person_data["last_name"],
            email=sample_person_data["email"],
            phone=sample_person_data.get("phone"),
            address=sample_person_data.get("address")
        )
        assert person_id is not None
        print(f"✓ [MCP Client] Person created with ID: {person_id}")
        
        # READ
        person = await mcp_client.get_person(person_id)
        assert person is not None
        assert person["first_name"] == sample_person_data["first_name"]
        assert person["email"] == sample_person_data["email"]
        print(f"✓ [MCP Client] Person retrieved successfully")
        
        # UPDATE
        updated_person = await mcp_client.update_person(
            person_id=person_id,
            phone="+1-555-9999",
            address="456 Updated Street"
        )
        assert updated_person["phone"] == "+1-555-9999"
        print(f"✓ [MCP Client] Person updated successfully")
        
        # DELETE
        deleted = await mcp_client.delete_person(person_id)
        assert deleted is True
        print(f"✓ [MCP Client] Person deleted successfully")

    @pytest.mark.asyncio
    async def test_mcp_client_student_operations(self, mcp_client, sample_student_data):
        """Test MCP client student operations."""
        # CREATE
        student_id = await mcp_client.create_student(
            first_name=sample_student_data["first_name"],
            last_name=sample_student_data["last_name"],
            email=sample_student_data["email"],
            student_id=sample_student_data.get("student_id"),
            grade_level=sample_student_data.get("grade_level"),
            guardian_contact=sample_student_data.get("guardian_contact")
        )
        assert student_id is not None
        print(f"✓ [MCP Client] Student created with ID: {student_id}")
        
        # READ
        student = await mcp_client.get_student(student_id)
        assert student is not None
        assert student["first_name"] == sample_student_data["first_name"]
        assert student["student_id"] == sample_student_data["student_id"]
        print(f"✓ [MCP Client] Student retrieved successfully")
        
        # UPDATE
        updated_student = await mcp_client.update_student(
            student_id=student_id,
            grade_level=11,
            guardian_contact="updated_parent@test.com"
        )
        assert updated_student["grade_level"] == 11
        print(f"✓ [MCP Client] Student updated successfully")
        
        # DELETE
        deleted = await mcp_client.delete_student(student_id)
        assert deleted is True
        print(f"✓ [MCP Client] Student deleted successfully")

    @pytest.mark.asyncio
    async def test_mcp_client_teacher_operations(self, mcp_client, sample_teacher_data):
        """Test MCP client teacher operations."""
        # CREATE
        teacher_id = await mcp_client.create_teacher(
            first_name=sample_teacher_data["first_name"],
            last_name=sample_teacher_data["last_name"],
            email=sample_teacher_data["email"],
            employee_id=sample_teacher_data.get("employee_id"),
            subjects=sample_teacher_data.get("subjects", []),
            department=sample_teacher_data.get("department"),
            qualification=sample_teacher_data.get("qualification")
        )
        assert teacher_id is not None
        print(f"✓ [MCP Client] Teacher created with ID: {teacher_id}")
        
        # READ
        teacher = await mcp_client.get_teacher(teacher_id)
        assert teacher is not None
        assert teacher["first_name"] == sample_teacher_data["first_name"]
        assert teacher["employee_id"] == sample_teacher_data["employee_id"]
        print(f"✓ [MCP Client] Teacher retrieved successfully")
        
        # UPDATE
        updated_teacher = await mcp_client.update_teacher(
            teacher_id=teacher_id,
            subjects=["mathematics", "statistics", "calculus"],
            qualification="PhD in Applied Mathematics"
        )
        assert len(updated_teacher["subjects"]) == 3
        print(f"✓ [MCP Client] Teacher updated successfully")
        
        # DELETE
        deleted = await mcp_client.delete_teacher(teacher_id)
        assert deleted is True
        print(f"✓ [MCP Client] Teacher deleted successfully")

    @pytest.mark.asyncio
    async def test_mcp_client_class_operations(self, mcp_client, sample_class_data):
        """Test MCP client class operations."""
        # CREATE
        class_id = await mcp_client.create_class(
            name=sample_class_data["name"],
            academic_year=sample_class_data["academic_year"],
            grade_level=sample_class_data.get("grade_level"),
            capacity=sample_class_data.get("capacity"),
            location=sample_class_data.get("location"),
            class_code=sample_class_data.get("class_code"),
            semester=sample_class_data.get("semester")
        )
        assert class_id is not None
        print(f"✓ [MCP Client] Class created with ID: {class_id}")
        
        # READ
        class_obj = await mcp_client.get_class(class_id)
        assert class_obj is not None
        assert class_obj["name"] == sample_class_data["name"]
        assert class_obj["class_code"] == sample_class_data["class_code"]
        print(f"✓ [MCP Client] Class retrieved successfully")
        
        # UPDATE
        updated_class = await mcp_client.update_class(
            class_id=class_id,
            capacity=30,
            location="Room 301"
        )
        assert updated_class["capacity"] == 30
        print(f"✓ [MCP Client] Class updated successfully")
        
        # DELETE
        deleted = await mcp_client.delete_class(class_id)
        assert deleted is True
        print(f"✓ [MCP Client] Class deleted successfully")

    @pytest.mark.asyncio
    async def test_mcp_client_relationship_operations(self, mcp_client, sample_class_data, sample_student_data, sample_teacher_data):
        """Test MCP client relationship operations."""
        # Create entities
        class_id = await mcp_client.create_class(
            name=sample_class_data["name"],
            academic_year=sample_class_data["academic_year"],
            class_code=sample_class_data["class_code"]
        )
        
        student_id = await mcp_client.create_student(
            first_name=sample_student_data["first_name"],
            last_name=sample_student_data["last_name"],
            email=sample_student_data["email"],
            student_id=sample_student_data["student_id"]
        )
        
        teacher_id = await mcp_client.create_teacher(
            first_name=sample_teacher_data["first_name"],
            last_name=sample_teacher_data["last_name"],
            email=sample_teacher_data["email"],
            employee_id=sample_teacher_data["employee_id"],
            subjects=sample_teacher_data["subjects"]
        )
        
        # Add student to class
        result = await mcp_client.add_students_to_class(class_id, [student_id])
        assert result["success"] is True
        print(f"✓ [MCP Client] Student added to class")
        
        # Add teacher to class
        result = await mcp_client.add_teacher_to_class(class_id, teacher_id, "mathematics")
        assert result["success"] is True
        print(f"✓ [MCP Client] Teacher added to class")
        
        # Add scores
        scores_data = [
            {
                "student_id": student_id,
                "class_id": class_id,
                "subject": "mathematics",
                "score": 85.5,
                "max_score": 100.0,
                "assessment_type": "midterm",
                "teacher_id": teacher_id
            }
        ]
        
        result = await mcp_client.add_scores_to_students(scores_data)
        assert result["success"] is True
        print(f"✓ [MCP Client] Scores added to student")
        
        # Cleanup
        await mcp_client.delete_student(student_id)
        await mcp_client.delete_teacher(teacher_id)
        await mcp_client.delete_class(class_id)

    @pytest.mark.asyncio
    async def test_mcp_client_bulk_operations(self, mcp_client, bulk_students_data):
        """Test MCP client bulk operations."""
        # Bulk create students
        result = await mcp_client.bulk_operation(
            operation_type="create",
            entity_type="student",
            data=bulk_students_data[:5],  # Use first 5 students
            batch_size=3
        )
        
        assert result["success"] is True
        assert result["processed"] == 5
        assert len(result["created_ids"]) == 5
        
        print(f"✓ [MCP Client] Bulk created 5 students")
        
        # Cleanup
        for student_id in result["created_ids"]:
            await mcp_client.delete_student(student_id)

    @pytest.mark.asyncio
    async def test_mcp_client_aggregate_operations(self, mcp_client, sample_class_data, sample_student_data, sample_teacher_data):
        """Test MCP client aggregate operations."""
        # Create test setup
        class_id = await mcp_client.create_class(
            name=sample_class_data["name"],
            academic_year=sample_class_data["academic_year"],
            class_code=sample_class_data["class_code"]
        )
        
        student_id = await mcp_client.create_student(
            first_name=sample_student_data["first_name"],
            last_name=sample_student_data["last_name"],
            email=sample_student_data["email"],
            student_id=sample_student_data["student_id"]
        )
        
        teacher_id = await mcp_client.create_teacher(
            first_name=sample_teacher_data["first_name"],
            last_name=sample_teacher_data["last_name"],
            email=sample_teacher_data["email"],
            employee_id=sample_teacher_data["employee_id"],
            subjects=sample_teacher_data["subjects"]
        )
        
        # Set up relationships
        await mcp_client.add_students_to_class(class_id, [student_id])
        await mcp_client.add_teacher_to_class(class_id, teacher_id, "mathematics")
        
        # Add scores
        scores_data = [
            {
                "student_id": student_id,
                "class_id": class_id,
                "subject": "mathematics",
                "score": 90.0,
                "max_score": 100.0,
                "assessment_type": "test",
                "teacher_id": teacher_id
            }
        ]
        await mcp_client.add_scores_to_students(scores_data)
        
        # Test aggregate operations
        students_result = await mcp_client.get_students_per_class(class_id)
        assert students_result["total_students"] == 1
        print(f"✓ [MCP Client] Students per class aggregate working")
        
        teachers_result = await mcp_client.get_teachers_per_class(class_id)
        assert teachers_result["total_teachers"] == 1
        print(f"✓ [MCP Client] Teachers per class aggregate working")
        
        subjects_result = await mcp_client.get_subjects_per_class(class_id)
        assert subjects_result["total_subjects"] == 1
        print(f"✓ [MCP Client] Subjects per class aggregate working")
        
        avg_scores_result = await mcp_client.get_avg_score_per_class(class_id)
        assert len(avg_scores_result["class_averages"]) == 1
        assert abs(avg_scores_result["class_averages"][0]["average_score"] - 90.0) < 0.1
        print(f"✓ [MCP Client] Average scores per class aggregate working")
        
        # Test custom aggregate
        custom_query = {
            "entity": "student",
            "group_by": "grade_level",
            "filters": {},
            "aggregations": {
                "count": {"$count": {}}
            }
        }
        
        custom_result = await mcp_client.aggregate_query(custom_query)
        assert custom_result["success"] is True
        print(f"✓ [MCP Client] Custom aggregate query working")
        
        # Cleanup
        await mcp_client.delete_student(student_id)
        await mcp_client.delete_teacher(teacher_id)
        await mcp_client.delete_class(class_id)

    @pytest.mark.asyncio
    async def test_mcp_client_error_handling(self, mcp_client):
        """Test MCP client error handling."""
        fake_id = "00000000-0000-0000-0000-000000000000"
        
        # Test operations on non-existent entities
        person = await mcp_client.get_person(fake_id)
        assert person is None
        
        student = await mcp_client.get_student(fake_id)
        assert student is None
        
        teacher = await mcp_client.get_teacher(fake_id)
        assert teacher is None
        
        class_obj = await mcp_client.get_class(fake_id)
        assert class_obj is None
        
        # Test update operations on non-existent entities
        updated_person = await mcp_client.update_person(fake_id, first_name="Updated")
        assert updated_person is None
        
        # Test delete operations on non-existent entities
        deleted = await mcp_client.delete_person(fake_id)
        assert deleted is False
        
        print(f"✓ [MCP Client] Error handling working correctly")
