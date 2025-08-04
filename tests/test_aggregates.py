"""
Integration tests for aggregate operations across all database backends.
"""
import pytest
from typing import Dict, Any, List


class TestAggregateOperations:
    """Test aggregate operations for all database backends."""

    @pytest.mark.asyncio
    async def test_students_per_class_aggregate(self, database_interface, sample_class_data):
        """Test students per class aggregate query."""
        interface, db_type = database_interface
        
        # Create multiple classes
        class_data_1 = sample_class_data.copy()
        class_data_1["class_code"] = "MATH-10A"
        class_data_1["name"] = "Mathematics 10A"
        
        class_data_2 = sample_class_data.copy()
        class_data_2["class_code"] = "MATH-10B"
        class_data_2["name"] = "Mathematics 10B"
        
        class_id_1 = await interface.create_class(class_data_1)
        class_id_2 = await interface.create_class(class_data_2)
        
        # Create students
        students_data = [
            {
                "first_name": f"Student{i}",
                "last_name": "Aggregate",
                "email": f"agg_student{i}@test.com",
                "student_id": f"AGG{i:03d}",
                "grade_level": 10
            }
            for i in range(1, 8)  # 7 students
        ]
        
        student_ids = []
        for student_data in students_data:
            student_id = await interface.create_student(student_data)
            student_ids.append(student_id)
        
        # Add students to classes (5 to class 1, 3 to class 2, 1 overlap)
        await interface.add_students_to_class(class_id_1, student_ids[:5])
        await interface.add_students_to_class(class_id_2, student_ids[4:7])  # student_ids[4] is in both
        
        # Test aggregate for specific class
        result_1 = await interface.get_students_per_class(class_id_1)
        assert result_1["class_id"] == class_id_1
        assert len(result_1["students"]) == 5
        assert result_1["total_students"] == 5
        
        result_2 = await interface.get_students_per_class(class_id_2)
        assert result_2["class_id"] == class_id_2
        assert len(result_2["students"]) == 3
        assert result_2["total_students"] == 3
        
        # Test aggregate for all classes
        all_classes_result = await interface.get_students_per_class()
        assert len(all_classes_result["classes"]) >= 2
        
        # Find our classes in the results
        class_1_found = False
        class_2_found = False
        for class_info in all_classes_result["classes"]:
            if class_info["class_id"] == class_id_1:
                assert class_info["student_count"] == 5
                class_1_found = True
            elif class_info["class_id"] == class_id_2:
                assert class_info["student_count"] == 3
                class_2_found = True
        
        assert class_1_found and class_2_found
        
        print(f"✓ [{db_type}] Students per class aggregate working correctly")
        
        # Cleanup
        for student_id in student_ids:
            await interface.delete_student(student_id)
        await interface.delete_class(class_id_1)
        await interface.delete_class(class_id_2)

    @pytest.mark.asyncio
    async def test_avg_score_per_class_aggregate(self, database_interface, sample_class_data):
        """Test average score per class aggregate query."""
        interface, db_type = database_interface
        
        # Create class, students, and teacher
        class_id = await interface.create_class(sample_class_data)
        
        students_data = [
            {
                "first_name": f"ScoreStudent{i}",
                "last_name": "Test",
                "email": f"score_student{i}@test.com",
                "student_id": f"SCORE{i:03d}",
                "grade_level": 10
            }
            for i in range(1, 4)  # 3 students
        ]
        
        student_ids = []
        for student_data in students_data:
            student_id = await interface.create_student(student_data)
            student_ids.append(student_id)
        
        teacher_data = {
            "first_name": "Score",
            "last_name": "Teacher",
            "email": "score_teacher@test.com",
            "employee_id": "SCORE001",
            "subjects": ["mathematics", "physics"],
            "department": "Science"
        }
        teacher_id = await interface.create_teacher(teacher_data)
        
        # Add students to class and teacher assignments
        await interface.add_students_to_class(class_id, student_ids)
        await interface.add_teacher_to_class(class_id, teacher_id, "mathematics")
        await interface.add_teacher_to_class(class_id, teacher_id, "physics")
        
        # Add scores for different subjects
        scores_data = [
            # Mathematics scores
            {"student_id": student_ids[0], "class_id": class_id, "subject": "mathematics", "score": 85.0, "max_score": 100.0, "assessment_type": "test", "teacher_id": teacher_id},
            {"student_id": student_ids[1], "class_id": class_id, "subject": "mathematics", "score": 90.0, "max_score": 100.0, "assessment_type": "test", "teacher_id": teacher_id},
            {"student_id": student_ids[2], "class_id": class_id, "subject": "mathematics", "score": 95.0, "max_score": 100.0, "assessment_type": "test", "teacher_id": teacher_id},
            
            # Physics scores
            {"student_id": student_ids[0], "class_id": class_id, "subject": "physics", "score": 80.0, "max_score": 100.0, "assessment_type": "test", "teacher_id": teacher_id},
            {"student_id": student_ids[1], "class_id": class_id, "subject": "physics", "score": 85.0, "max_score": 100.0, "assessment_type": "test", "teacher_id": teacher_id},
            {"student_id": student_ids[2], "class_id": class_id, "subject": "physics", "score": 75.0, "max_score": 100.0, "assessment_type": "test", "teacher_id": teacher_id},
        ]
        
        await interface.add_scores_to_students(scores_data)
        
        # Test aggregate for specific class
        result = await interface.get_avg_score_per_class(class_id)
        assert result["class_id"] == class_id
        assert len(result["class_averages"]) == 2  # mathematics and physics
        
        # Check averages
        math_avg = None
        physics_avg = None
        for avg in result["class_averages"]:
            if avg["subject"] == "mathematics":
                math_avg = avg
            elif avg["subject"] == "physics":
                physics_avg = avg
        
        assert math_avg is not None
        assert physics_avg is not None
        
        # Mathematics average: (85 + 90 + 95) / 3 = 90
        assert abs(math_avg["average_score"] - 90.0) < 0.1
        assert math_avg["student_count"] == 3
        
        # Physics average: (80 + 85 + 75) / 3 = 80
        assert abs(physics_avg["average_score"] - 80.0) < 0.1
        assert physics_avg["student_count"] == 3
        
        # Test aggregate for all classes
        all_classes_result = await interface.get_avg_score_per_class()
        assert len(all_classes_result["classes"]) >= 1
        
        print(f"✓ [{db_type}] Average score per class aggregate working correctly")
        
        # Cleanup
        for student_id in student_ids:
            await interface.delete_student(student_id)
        await interface.delete_teacher(teacher_id)
        await interface.delete_class(class_id)

    @pytest.mark.asyncio
    async def test_teachers_per_class_aggregate(self, database_interface, sample_class_data):
        """Test teachers per class aggregate query."""
        interface, db_type = database_interface
        
        # Create class
        class_id = await interface.create_class(sample_class_data)
        
        # Create multiple teachers
        teachers_data = [
            {
                "first_name": "Math",
                "last_name": "Teacher",
                "email": "math_teacher_agg@test.com",
                "employee_id": "MATH_AGG001",
                "subjects": ["mathematics", "algebra"],
                "department": "Mathematics"
            },
            {
                "first_name": "Science",
                "last_name": "Teacher",
                "email": "science_teacher_agg@test.com",
                "employee_id": "SCI_AGG001",
                "subjects": ["physics", "chemistry"],
                "department": "Science"
            },
            {
                "first_name": "English",
                "last_name": "Teacher",
                "email": "english_teacher_agg@test.com",
                "employee_id": "ENG_AGG001",
                "subjects": ["literature", "writing"],
                "department": "English"
            }
        ]
        
        teacher_ids = []
        for teacher_data in teachers_data:
            teacher_id = await interface.create_teacher(teacher_data)
            teacher_ids.append(teacher_id)
        
        # Assign teachers to class for different subjects
        await interface.add_teacher_to_class(class_id, teacher_ids[0], "mathematics")
        await interface.add_teacher_to_class(class_id, teacher_ids[0], "algebra")
        await interface.add_teacher_to_class(class_id, teacher_ids[1], "physics")
        await interface.add_teacher_to_class(class_id, teacher_ids[1], "chemistry")
        await interface.add_teacher_to_class(class_id, teacher_ids[2], "literature")
        
        # Test aggregate for specific class
        result = await interface.get_teachers_per_class(class_id)
        assert result["class_id"] == class_id
        assert len(result["teachers"]) == 3
        assert result["total_teachers"] == 3
        
        # Verify teacher subjects
        teacher_subjects = {}
        for teacher in result["teachers"]:
            teacher_subjects[teacher["id"]] = teacher["subjects"]
        
        assert len(teacher_subjects[teacher_ids[0]]) == 2  # mathematics, algebra
        assert len(teacher_subjects[teacher_ids[1]]) == 2  # physics, chemistry
        assert len(teacher_subjects[teacher_ids[2]]) == 1  # literature
        
        # Test aggregate for all classes
        all_classes_result = await interface.get_teachers_per_class()
        assert len(all_classes_result["classes"]) >= 1
        
        # Find our class in the results
        our_class = None
        for class_info in all_classes_result["classes"]:
            if class_info["class_id"] == class_id:
                our_class = class_info
                break
        
        assert our_class is not None
        assert our_class["teacher_count"] == 3
        
        print(f"✓ [{db_type}] Teachers per class aggregate working correctly")
        
        # Cleanup
        for teacher_id in teacher_ids:
            await interface.delete_teacher(teacher_id)
        await interface.delete_class(class_id)

    @pytest.mark.asyncio
    async def test_subjects_per_class_aggregate(self, database_interface, sample_class_data):
        """Test subjects per class aggregate query."""
        interface, db_type = database_interface
        
        # Create class and teacher
        class_id = await interface.create_class(sample_class_data)
        
        teacher_data = {
            "first_name": "Multi",
            "last_name": "Subject",
            "email": "multi_subject@test.com",
            "employee_id": "MULTI001",
            "subjects": ["mathematics", "physics", "chemistry", "biology"],
            "department": "Science"
        }
        teacher_id = await interface.create_teacher(teacher_data)
        
        # Assign teacher to multiple subjects
        subjects = ["mathematics", "physics", "chemistry", "biology"]
        for subject in subjects:
            await interface.add_teacher_to_class(class_id, teacher_id, subject)
        
        # Test aggregate for specific class
        result = await interface.get_subjects_per_class(class_id)
        assert result["class_id"] == class_id
        assert len(result["subjects"]) == 4
        assert result["total_subjects"] == 4
        
        # Verify all subjects are present
        subject_names = [s["subject"] for s in result["subjects"]]
        for subject in subjects:
            assert subject in subject_names
        
        # Each subject should have 1 teacher
        for subject_info in result["subjects"]:
            assert subject_info["teacher_count"] == 1
        
        # Test aggregate for all classes
        all_classes_result = await interface.get_subjects_per_class()
        assert len(all_classes_result["classes"]) >= 1
        
        # Find our class in the results
        our_class = None
        for class_info in all_classes_result["classes"]:
            if class_info["class_id"] == class_id:
                our_class = class_info
                break
        
        assert our_class is not None
        assert our_class["subject_count"] == 4
        
        print(f"✓ [{db_type}] Subjects per class aggregate working correctly")
        
        # Cleanup
        await interface.delete_teacher(teacher_id)
        await interface.delete_class(class_id)

    @pytest.mark.asyncio
    async def test_custom_aggregate_query(self, database_interface, sample_class_data):
        """Test custom aggregate query functionality."""
        interface, db_type = database_interface
        
        # Create test data
        class_id = await interface.create_class(sample_class_data)
        
        students_data = [
            {
                "first_name": f"CustomStudent{i}",
                "last_name": "Test",
                "email": f"custom_student{i}@test.com",
                "student_id": f"CUSTOM{i:03d}",
                "grade_level": 10 + (i % 2)  # Mix of grade 10 and 11
            }
            for i in range(1, 6)  # 5 students
        ]
        
        student_ids = []
        for student_data in students_data:
            student_id = await interface.create_student(student_data)
            student_ids.append(student_id)
        
        await interface.add_students_to_class(class_id, student_ids)
        
        # Test custom aggregate query - students by grade level
        custom_query = {
            "entity": "student",
            "group_by": "grade_level",
            "filters": {"grade_level": {"$in": [10, 11]}},
            "aggregations": {
                "count": {"$count": {}},
                "avg_grade": {"$avg": "grade_level"}
            }
        }
        
        result = await interface.aggregate_query(custom_query)
        assert result["success"] is True
        assert len(result["results"]) >= 1
        
        # Verify grouping by grade level
        grade_10_count = 0
        grade_11_count = 0
        
        for group in result["results"]:
            if group["grade_level"] == 10:
                grade_10_count = group["count"]
            elif group["grade_level"] == 11:
                grade_11_count = group["count"]
        
        assert grade_10_count + grade_11_count == 5
        
        print(f"✓ [{db_type}] Custom aggregate query working correctly")
        
        # Cleanup
        for student_id in student_ids:
            await interface.delete_student(student_id)
        await interface.delete_class(class_id)

    @pytest.mark.asyncio
    async def test_empty_aggregate_results(self, database_interface, sample_class_data):
        """Test aggregate operations with empty results."""
        interface, db_type = database_interface
        
        # Create an empty class
        class_id = await interface.create_class(sample_class_data)
        
        # Test aggregates on empty class
        students_result = await interface.get_students_per_class(class_id)
        assert students_result["class_id"] == class_id
        assert len(students_result["students"]) == 0
        assert students_result["total_students"] == 0
        
        teachers_result = await interface.get_teachers_per_class(class_id)
        assert teachers_result["class_id"] == class_id
        assert len(teachers_result["teachers"]) == 0
        assert teachers_result["total_teachers"] == 0
        
        subjects_result = await interface.get_subjects_per_class(class_id)
        assert subjects_result["class_id"] == class_id
        assert len(subjects_result["subjects"]) == 0
        assert subjects_result["total_subjects"] == 0
        
        avg_scores_result = await interface.get_avg_score_per_class(class_id)
        assert avg_scores_result["class_id"] == class_id
        assert len(avg_scores_result["class_averages"]) == 0
        
        print(f"✓ [{db_type}] Empty aggregate results handled correctly")
        
        # Cleanup
        await interface.delete_class(class_id)
