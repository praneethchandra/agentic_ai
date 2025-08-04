"""
Integration tests for relationship management across all database backends.
"""
import pytest
from typing import Dict, Any, List


class TestRelationships:
    """Test relationship management operations for all database backends."""

    @pytest.mark.asyncio
    async def test_add_students_to_class(self, database_interface, sample_class_data, bulk_students_data):
        """Test adding students to a class."""
        interface, db_type = database_interface
        
        # Create a class
        class_id = await interface.create_class(sample_class_data)
        assert class_id is not None
        
        # Create students
        student_ids = []
        for student_data in bulk_students_data[:5]:  # Use first 5 students
            student_id = await interface.create_student(student_data)
            student_ids.append(student_id)
        
        # Add students to class
        result = await interface.add_students_to_class(class_id, student_ids)
        assert result["success"] is True
        assert result["enrolled_count"] == len(student_ids)
        
        print(f"✓ [{db_type}] Added {len(student_ids)} students to class")
        
        # Verify enrollments
        enrollments = await interface.get_students_per_class(class_id)
        assert len(enrollments["students"]) == len(student_ids)
        
        enrolled_student_ids = [s["id"] for s in enrollments["students"]]
        for student_id in student_ids:
            assert student_id in enrolled_student_ids
        
        print(f"✓ [{db_type}] All student enrollments verified")
        
        # Cleanup
        for student_id in student_ids:
            await interface.delete_student(student_id)
        await interface.delete_class(class_id)

    @pytest.mark.asyncio
    async def test_add_teacher_to_class(self, database_interface, sample_class_data, sample_teacher_data):
        """Test adding a teacher to a class for specific subjects."""
        interface, db_type = database_interface
        
        # Create a class
        class_id = await interface.create_class(sample_class_data)
        assert class_id is not None
        
        # Create a teacher
        teacher_id = await interface.create_teacher(sample_teacher_data)
        assert teacher_id is not None
        
        # Add teacher to class for mathematics
        result = await interface.add_teacher_to_class(class_id, teacher_id, "mathematics")
        assert result["success"] is True
        
        print(f"✓ [{db_type}] Added teacher to class for mathematics")
        
        # Add teacher to class for statistics
        result = await interface.add_teacher_to_class(class_id, teacher_id, "statistics")
        assert result["success"] is True
        
        print(f"✓ [{db_type}] Added teacher to class for statistics")
        
        # Verify teacher assignments
        teachers = await interface.get_teachers_per_class(class_id)
        assert len(teachers["teachers"]) >= 1
        
        # Find our teacher in the results
        our_teacher = None
        for teacher in teachers["teachers"]:
            if teacher["id"] == teacher_id:
                our_teacher = teacher
                break
        
        assert our_teacher is not None
        assert "mathematics" in our_teacher["subjects"]
        assert "statistics" in our_teacher["subjects"]
        
        print(f"✓ [{db_type}] Teacher assignments verified")
        
        # Cleanup
        await interface.delete_teacher(teacher_id)
        await interface.delete_class(class_id)

    @pytest.mark.asyncio
    async def test_add_scores_to_students(self, database_interface, sample_class_data, sample_student_data, sample_teacher_data):
        """Test adding scores to students."""
        interface, db_type = database_interface
        
        # Create entities
        class_id = await interface.create_class(sample_class_data)
        student_id = await interface.create_student(sample_student_data)
        teacher_id = await interface.create_teacher(sample_teacher_data)
        
        # Add student to class
        await interface.add_students_to_class(class_id, [student_id])
        
        # Add teacher to class
        await interface.add_teacher_to_class(class_id, teacher_id, "mathematics")
        
        # Prepare scores data
        scores_data = [
            {
                "student_id": student_id,
                "class_id": class_id,
                "subject": "mathematics",
                "score": 85.5,
                "max_score": 100.0,
                "assessment_type": "midterm",
                "teacher_id": teacher_id
            },
            {
                "student_id": student_id,
                "class_id": class_id,
                "subject": "mathematics",
                "score": 92.0,
                "max_score": 100.0,
                "assessment_type": "quiz",
                "teacher_id": teacher_id
            }
        ]
        
        # Add scores
        result = await interface.add_scores_to_students(scores_data)
        assert result["success"] is True
        assert result["scores_added"] == len(scores_data)
        
        print(f"✓ [{db_type}] Added {len(scores_data)} scores to student")
        
        # Verify scores through aggregate query
        avg_scores = await interface.get_avg_score_per_class(class_id)
        assert len(avg_scores["class_averages"]) > 0
        
        math_avg = None
        for avg in avg_scores["class_averages"]:
            if avg["subject"] == "mathematics":
                math_avg = avg
                break
        
        assert math_avg is not None
        expected_avg = (85.5 + 92.0) / 2
        assert abs(math_avg["average_score"] - expected_avg) < 0.1
        
        print(f"✓ [{db_type}] Score averages calculated correctly")
        
        # Cleanup
        await interface.delete_student(student_id)
        await interface.delete_teacher(teacher_id)
        await interface.delete_class(class_id)

    @pytest.mark.asyncio
    async def test_complex_class_setup(self, database_interface, sample_class_data):
        """Test setting up a complex class with multiple students, teachers, and subjects."""
        interface, db_type = database_interface
        
        # Create a class
        class_id = await interface.create_class(sample_class_data)
        
        # Create multiple students
        students_data = [
            {
                "first_name": f"Student{i}",
                "last_name": "Complex",
                "email": f"complex_student{i}@test.com",
                "student_id": f"COMP{i:03d}",
                "grade_level": 10
            }
            for i in range(1, 6)  # 5 students
        ]
        
        student_ids = []
        for student_data in students_data:
            student_id = await interface.create_student(student_data)
            student_ids.append(student_id)
        
        # Create multiple teachers
        teachers_data = [
            {
                "first_name": "Math",
                "last_name": "Teacher",
                "email": "math_teacher@test.com",
                "employee_id": "MATH001",
                "subjects": ["mathematics", "algebra"],
                "department": "Mathematics"
            },
            {
                "first_name": "Science",
                "last_name": "Teacher",
                "email": "science_teacher@test.com",
                "employee_id": "SCI001",
                "subjects": ["physics", "chemistry"],
                "department": "Science"
            }
        ]
        
        teacher_ids = []
        for teacher_data in teachers_data:
            teacher_id = await interface.create_teacher(teacher_data)
            teacher_ids.append(teacher_id)
        
        # Add all students to class
        result = await interface.add_students_to_class(class_id, student_ids)
        assert result["success"] is True
        assert result["enrolled_count"] == len(student_ids)
        
        # Add teachers to class for different subjects
        await interface.add_teacher_to_class(class_id, teacher_ids[0], "mathematics")
        await interface.add_teacher_to_class(class_id, teacher_ids[0], "algebra")
        await interface.add_teacher_to_class(class_id, teacher_ids[1], "physics")
        await interface.add_teacher_to_class(class_id, teacher_ids[1], "chemistry")
        
        # Add scores for all students in multiple subjects
        scores_data = []
        subjects = ["mathematics", "algebra", "physics", "chemistry"]
        
        for student_id in student_ids:
            for i, subject in enumerate(subjects):
                teacher_id = teacher_ids[0] if subject in ["mathematics", "algebra"] else teacher_ids[1]
                scores_data.append({
                    "student_id": student_id,
                    "class_id": class_id,
                    "subject": subject,
                    "score": 80 + (i * 5) + (student_ids.index(student_id) * 2),  # Varied scores
                    "max_score": 100.0,
                    "assessment_type": "midterm",
                    "teacher_id": teacher_id
                })
        
        result = await interface.add_scores_to_students(scores_data)
        assert result["success"] is True
        
        print(f"✓ [{db_type}] Complex class setup completed")
        
        # Verify the complete setup
        students_result = await interface.get_students_per_class(class_id)
        teachers_result = await interface.get_teachers_per_class(class_id)
        subjects_result = await interface.get_subjects_per_class(class_id)
        avg_scores_result = await interface.get_avg_score_per_class(class_id)
        
        assert len(students_result["students"]) == 5
        assert len(teachers_result["teachers"]) == 2
        assert len(subjects_result["subjects"]) == 4
        assert len(avg_scores_result["class_averages"]) == 4
        
        print(f"✓ [{db_type}] Complex class verification completed")
        
        # Cleanup
        for student_id in student_ids:
            await interface.delete_student(student_id)
        for teacher_id in teacher_ids:
            await interface.delete_teacher(teacher_id)
        await interface.delete_class(class_id)

    @pytest.mark.asyncio
    async def test_duplicate_relationship_handling(self, database_interface, sample_class_data, sample_student_data, sample_teacher_data):
        """Test handling of duplicate relationships."""
        interface, db_type = database_interface
        
        # Create entities
        class_id = await interface.create_class(sample_class_data)
        student_id = await interface.create_student(sample_student_data)
        teacher_id = await interface.create_teacher(sample_teacher_data)
        
        # Add student to class
        result1 = await interface.add_students_to_class(class_id, [student_id])
        assert result1["success"] is True
        
        # Try to add the same student again (should handle gracefully)
        result2 = await interface.add_students_to_class(class_id, [student_id])
        # Should either succeed (idempotent) or handle the duplicate gracefully
        assert "success" in result2
        
        # Add teacher to class for a subject
        result3 = await interface.add_teacher_to_class(class_id, teacher_id, "mathematics")
        assert result3["success"] is True
        
        # Try to add the same teacher for the same subject again
        result4 = await interface.add_teacher_to_class(class_id, teacher_id, "mathematics")
        # Should either succeed (idempotent) or handle the duplicate gracefully
        assert "success" in result4
        
        print(f"✓ [{db_type}] Duplicate relationship handling works correctly")
        
        # Cleanup
        await interface.delete_student(student_id)
        await interface.delete_teacher(teacher_id)
        await interface.delete_class(class_id)

    @pytest.mark.asyncio
    async def test_relationship_constraints(self, database_interface, sample_class_data, sample_student_data):
        """Test relationship constraints and error handling."""
        interface, db_type = database_interface
        
        # Create a class and student
        class_id = await interface.create_class(sample_class_data)
        student_id = await interface.create_student(sample_student_data)
        
        fake_id = "00000000-0000-0000-0000-000000000000"
        
        # Try to add non-existent student to class
        with pytest.raises(Exception):
            await interface.add_students_to_class(class_id, [fake_id])
        
        # Try to add student to non-existent class
        with pytest.raises(Exception):
            await interface.add_students_to_class(fake_id, [student_id])
        
        # Try to add non-existent teacher to class
        with pytest.raises(Exception):
            await interface.add_teacher_to_class(class_id, fake_id, "mathematics")
        
        print(f"✓ [{db_type}] Relationship constraints properly enforced")
        
        # Cleanup
        await interface.delete_student(student_id)
        await interface.delete_class(class_id)
