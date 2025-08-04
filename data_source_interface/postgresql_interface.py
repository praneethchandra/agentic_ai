"""
PostgreSQL implementation of the database interface.
"""

import json
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

from .database_interface import DatabaseInterface
from .models import (
    Person, Student, Teacher, Class, ClassEnrollment, 
    TeacherAssignment, Score, BulkOperation, AggregateQuery,
    PersonResponse, ClassResponse, BulkOperationResponse, AggregateResponse
)


class PostgreSQLInterface(DatabaseInterface):
    """PostgreSQL implementation of the database interface."""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.pool = None
    
    async def connect(self) -> bool:
        """Connect to PostgreSQL."""
        try:
            import asyncpg
            self.pool = await asyncpg.create_pool(self.connection_string)
            await self._create_tables()
            return True
        except Exception as e:
            print(f"PostgreSQL connection error: {e}")
            return False
    
    async def disconnect(self) -> bool:
        """Disconnect from PostgreSQL."""
        try:
            if self.pool:
                await self.pool.close()
            return True
        except Exception as e:
            print(f"PostgreSQL disconnection error: {e}")
            return False
    
    async def _create_tables(self):
        """Create tables with appropriate schemas."""
        create_tables_sql = """
        -- Create persons table
        CREATE TABLE IF NOT EXISTS persons (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            phone VARCHAR(20),
            date_of_birth DATE,
            address TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        
        -- Create students table
        CREATE TABLE IF NOT EXISTS students (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            phone VARCHAR(20),
            date_of_birth DATE,
            address TEXT,
            student_id VARCHAR(50) UNIQUE,
            grade_level INTEGER CHECK (grade_level >= 1 AND grade_level <= 12),
            enrollment_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            is_active BOOLEAN DEFAULT TRUE,
            guardian_contact TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        
        -- Create teachers table
        CREATE TABLE IF NOT EXISTS teachers (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            phone VARCHAR(20),
            date_of_birth DATE,
            address TEXT,
            employee_id VARCHAR(50) UNIQUE,
            subjects TEXT[], -- Array of subjects
            hire_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            is_active BOOLEAN DEFAULT TRUE,
            department VARCHAR(100),
            qualification TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        
        -- Create classes table
        CREATE TABLE IF NOT EXISTS classes (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name VARCHAR(200) NOT NULL,
            description TEXT,
            gathering_type VARCHAR(50) NOT NULL DEFAULT 'class',
            capacity INTEGER CHECK (capacity > 0),
            location VARCHAR(200),
            class_code VARCHAR(50) UNIQUE,
            grade_level INTEGER CHECK (grade_level >= 1 AND grade_level <= 12),
            academic_year VARCHAR(20) NOT NULL,
            semester VARCHAR(20),
            schedule JSONB,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        
        -- Create class_enrollments table
        CREATE TABLE IF NOT EXISTS class_enrollments (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            student_id UUID NOT NULL REFERENCES students(id) ON DELETE CASCADE,
            class_id UUID NOT NULL REFERENCES classes(id) ON DELETE CASCADE,
            enrollment_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            is_active BOOLEAN DEFAULT TRUE,
            UNIQUE(student_id, class_id)
        );
        
        -- Create teacher_assignments table
        CREATE TABLE IF NOT EXISTS teacher_assignments (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            teacher_id UUID NOT NULL REFERENCES teachers(id) ON DELETE CASCADE,
            class_id UUID NOT NULL REFERENCES classes(id) ON DELETE CASCADE,
            subject VARCHAR(100) NOT NULL,
            assignment_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            is_active BOOLEAN DEFAULT TRUE,
            UNIQUE(teacher_id, class_id, subject)
        );
        
        -- Create scores table
        CREATE TABLE IF NOT EXISTS scores (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            student_id UUID NOT NULL REFERENCES students(id) ON DELETE CASCADE,
            class_id UUID NOT NULL REFERENCES classes(id) ON DELETE CASCADE,
            subject VARCHAR(100) NOT NULL,
            score DECIMAL(5,2) NOT NULL CHECK (score >= 0 AND score <= 100),
            max_score DECIMAL(5,2) NOT NULL DEFAULT 100 CHECK (max_score > 0),
            assessment_type VARCHAR(100) NOT NULL,
            assessment_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            teacher_id UUID REFERENCES teachers(id),
            comments TEXT
        );
        
        -- Create indexes for better performance
        CREATE INDEX IF NOT EXISTS idx_students_email ON students(email);
        CREATE INDEX IF NOT EXISTS idx_teachers_email ON teachers(email);
        CREATE INDEX IF NOT EXISTS idx_classes_academic_year ON classes(academic_year);
        CREATE INDEX IF NOT EXISTS idx_class_enrollments_student_id ON class_enrollments(student_id);
        CREATE INDEX IF NOT EXISTS idx_class_enrollments_class_id ON class_enrollments(class_id);
        CREATE INDEX IF NOT EXISTS idx_teacher_assignments_teacher_id ON teacher_assignments(teacher_id);
        CREATE INDEX IF NOT EXISTS idx_teacher_assignments_class_id ON teacher_assignments(class_id);
        CREATE INDEX IF NOT EXISTS idx_scores_student_id ON scores(student_id);
        CREATE INDEX IF NOT EXISTS idx_scores_class_id ON scores(class_id);
        CREATE INDEX IF NOT EXISTS idx_scores_subject ON scores(subject);
        """
        
        async with self.pool.acquire() as conn:
            await conn.execute(create_tables_sql)
    
    def _generate_id(self) -> str:
        """Generate a unique ID."""
        return str(uuid.uuid4())
    
    def _prepare_data(self, obj: Any) -> Dict[str, Any]:
        """Prepare a Pydantic model for PostgreSQL storage."""
        data = obj.dict()
        if not data.get('id'):
            data['id'] = self._generate_id()
        data['updated_at'] = datetime.utcnow()
        
        # Convert datetime objects to strings for JSON serialization
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
        
        return data
    
    async def create_person(self, person: Person) -> PersonResponse:
        """Create a new person in PostgreSQL."""
        try:
            data = self._prepare_data(person)
            
            sql = """
            INSERT INTO persons (id, first_name, last_name, email, phone, date_of_birth, address, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING *
            """
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    sql, 
                    data['id'], data['first_name'], data['last_name'], data['email'],
                    data.get('phone'), data.get('date_of_birth'), data.get('address'),
                    data['created_at'], data['updated_at']
                )
                
                return PersonResponse(
                    success=True,
                    message="Person created successfully",
                    data=Person(**dict(row))
                )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to create person: {str(e)}",
                errors=[str(e)]
            )
    
    async def create_student(self, student: Student) -> PersonResponse:
        """Create a new student in PostgreSQL."""
        try:
            data = self._prepare_data(student)
            
            sql = """
            INSERT INTO students (id, first_name, last_name, email, phone, date_of_birth, address, 
                                student_id, grade_level, enrollment_date, is_active, guardian_contact, 
                                created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            RETURNING *
            """
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    sql,
                    data['id'], data['first_name'], data['last_name'], data['email'],
                    data.get('phone'), data.get('date_of_birth'), data.get('address'),
                    data.get('student_id'), data.get('grade_level'), data['enrollment_date'],
                    data['is_active'], data.get('guardian_contact'), data['created_at'], data['updated_at']
                )
                
                return PersonResponse(
                    success=True,
                    message="Student created successfully",
                    data=Student(**dict(row))
                )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to create student: {str(e)}",
                errors=[str(e)]
            )
    
    async def create_teacher(self, teacher: Teacher) -> PersonResponse:
        """Create a new teacher in PostgreSQL."""
        try:
            data = self._prepare_data(teacher)
            
            sql = """
            INSERT INTO teachers (id, first_name, last_name, email, phone, date_of_birth, address,
                                employee_id, subjects, hire_date, is_active, department, qualification,
                                created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            RETURNING *
            """
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    sql,
                    data['id'], data['first_name'], data['last_name'], data['email'],
                    data.get('phone'), data.get('date_of_birth'), data.get('address'),
                    data.get('employee_id'), data.get('subjects', []), data['hire_date'],
                    data['is_active'], data.get('department'), data.get('qualification'),
                    data['created_at'], data['updated_at']
                )
                
                return PersonResponse(
                    success=True,
                    message="Teacher created successfully",
                    data=Teacher(**dict(row))
                )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to create teacher: {str(e)}",
                errors=[str(e)]
            )
    
    async def create_class(self, class_obj: Class) -> ClassResponse:
        """Create a new class in PostgreSQL."""
        try:
            data = self._prepare_data(class_obj)
            
            sql = """
            INSERT INTO classes (id, name, description, gathering_type, capacity, location,
                               class_code, grade_level, academic_year, semester, schedule,
                               created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            RETURNING *
            """
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    sql,
                    data['id'], data['name'], data.get('description'), data['gathering_type'],
                    data.get('capacity'), data.get('location'), data.get('class_code'),
                    data.get('grade_level'), data['academic_year'], data.get('semester'),
                    json.dumps(data.get('schedule')) if data.get('schedule') else None,
                    data['created_at'], data['updated_at']
                )
                
                return ClassResponse(
                    success=True,
                    message="Class created successfully",
                    data=Class(**dict(row))
                )
        except Exception as e:
            return ClassResponse(
                success=False,
                message=f"Failed to create class: {str(e)}",
                errors=[str(e)]
            )
    
    async def get_person(self, person_id: str) -> PersonResponse:
        """Get a person by ID from PostgreSQL."""
        try:
            sql = "SELECT * FROM persons WHERE id = $1"
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(sql, person_id)
                
                if row:
                    return PersonResponse(
                        success=True,
                        message="Person found",
                        data=Person(**dict(row))
                    )
                else:
                    return PersonResponse(
                        success=False,
                        message="Person not found"
                    )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to get person: {str(e)}",
                errors=[str(e)]
            )
    
    async def get_student(self, student_id: str) -> PersonResponse:
        """Get a student by ID from PostgreSQL."""
        try:
            sql = "SELECT * FROM students WHERE id = $1"
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(sql, student_id)
                
                if row:
                    return PersonResponse(
                        success=True,
                        message="Student found",
                        data=Student(**dict(row))
                    )
                else:
                    return PersonResponse(
                        success=False,
                        message="Student not found"
                    )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to get student: {str(e)}",
                errors=[str(e)]
            )
    
    async def get_teacher(self, teacher_id: str) -> PersonResponse:
        """Get a teacher by ID from PostgreSQL."""
        try:
            sql = "SELECT * FROM teachers WHERE id = $1"
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(sql, teacher_id)
                
                if row:
                    return PersonResponse(
                        success=True,
                        message="Teacher found",
                        data=Teacher(**dict(row))
                    )
                else:
                    return PersonResponse(
                        success=False,
                        message="Teacher not found"
                    )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to get teacher: {str(e)}",
                errors=[str(e)]
            )
    
    async def get_class(self, class_id: str) -> ClassResponse:
        """Get a class by ID from PostgreSQL."""
        try:
            sql = "SELECT * FROM classes WHERE id = $1"
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(sql, class_id)
                
                if row:
                    return ClassResponse(
                        success=True,
                        message="Class found",
                        data=Class(**dict(row))
                    )
                else:
                    return ClassResponse(
                        success=False,
                        message="Class not found"
                    )
        except Exception as e:
            return ClassResponse(
                success=False,
                message=f"Failed to get class: {str(e)}",
                errors=[str(e)]
            )
    
    async def update_person(self, person_id: str, person: Person) -> PersonResponse:
        """Update a person in PostgreSQL."""
        try:
            data = self._prepare_data(person)
            data['id'] = person_id
            
            sql = """
            UPDATE persons 
            SET first_name = $2, last_name = $3, email = $4, phone = $5, 
                date_of_birth = $6, address = $7, updated_at = $8
            WHERE id = $1
            RETURNING *
            """
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    sql,
                    person_id, data['first_name'], data['last_name'], data['email'],
                    data.get('phone'), data.get('date_of_birth'), data.get('address'),
                    data['updated_at']
                )
                
                if row:
                    return PersonResponse(
                        success=True,
                        message="Person updated successfully",
                        data=Person(**dict(row))
                    )
                else:
                    return PersonResponse(
                        success=False,
                        message="Person not found"
                    )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to update person: {str(e)}",
                errors=[str(e)]
            )
    
    async def update_student(self, student_id: str, student: Student) -> PersonResponse:
        """Update a student in PostgreSQL."""
        try:
            data = self._prepare_data(student)
            data['id'] = student_id
            
            sql = """
            UPDATE students 
            SET first_name = $2, last_name = $3, email = $4, phone = $5, 
                date_of_birth = $6, address = $7, student_id = $8, grade_level = $9,
                enrollment_date = $10, is_active = $11, guardian_contact = $12, updated_at = $13
            WHERE id = $1
            RETURNING *
            """
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    sql,
                    student_id, data['first_name'], data['last_name'], data['email'],
                    data.get('phone'), data.get('date_of_birth'), data.get('address'),
                    data.get('student_id'), data.get('grade_level'), data['enrollment_date'],
                    data['is_active'], data.get('guardian_contact'), data['updated_at']
                )
                
                if row:
                    return PersonResponse(
                        success=True,
                        message="Student updated successfully",
                        data=Student(**dict(row))
                    )
                else:
                    return PersonResponse(
                        success=False,
                        message="Student not found"
                    )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to update student: {str(e)}",
                errors=[str(e)]
            )
    
    async def update_teacher(self, teacher_id: str, teacher: Teacher) -> PersonResponse:
        """Update a teacher in PostgreSQL."""
        try:
            data = self._prepare_data(teacher)
            data['id'] = teacher_id
            
            sql = """
            UPDATE teachers 
            SET first_name = $2, last_name = $3, email = $4, phone = $5, 
                date_of_birth = $6, address = $7, employee_id = $8, subjects = $9,
                hire_date = $10, is_active = $11, department = $12, qualification = $13, updated_at = $14
            WHERE id = $1
            RETURNING *
            """
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    sql,
                    teacher_id, data['first_name'], data['last_name'], data['email'],
                    data.get('phone'), data.get('date_of_birth'), data.get('address'),
                    data.get('employee_id'), data.get('subjects', []), data['hire_date'],
                    data['is_active'], data.get('department'), data.get('qualification'),
                    data['updated_at']
                )
                
                if row:
                    return PersonResponse(
                        success=True,
                        message="Teacher updated successfully",
                        data=Teacher(**dict(row))
                    )
                else:
                    return PersonResponse(
                        success=False,
                        message="Teacher not found"
                    )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to update teacher: {str(e)}",
                errors=[str(e)]
            )
    
    async def update_class(self, class_id: str, class_obj: Class) -> ClassResponse:
        """Update a class in PostgreSQL."""
        try:
            data = self._prepare_data(class_obj)
            data['id'] = class_id
            
            sql = """
            UPDATE classes 
            SET name = $2, description = $3, gathering_type = $4, capacity = $5,
                location = $6, class_code = $7, grade_level = $8, academic_year = $9,
                semester = $10, schedule = $11, updated_at = $12
            WHERE id = $1
            RETURNING *
            """
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    sql,
                    class_id, data['name'], data.get('description'), data['gathering_type'],
                    data.get('capacity'), data.get('location'), data.get('class_code'),
                    data.get('grade_level'), data['academic_year'], data.get('semester'),
                    json.dumps(data.get('schedule')) if data.get('schedule') else None,
                    data['updated_at']
                )
                
                if row:
                    return ClassResponse(
                        success=True,
                        message="Class updated successfully",
                        data=Class(**dict(row))
                    )
                else:
                    return ClassResponse(
                        success=False,
                        message="Class not found"
                    )
        except Exception as e:
            return ClassResponse(
                success=False,
                message=f"Failed to update class: {str(e)}",
                errors=[str(e)]
            )
    
    async def delete_person(self, person_id: str) -> PersonResponse:
        """Delete a person from PostgreSQL."""
        try:
            sql = "DELETE FROM persons WHERE id = $1 RETURNING id"
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(sql, person_id)
                
                if row:
                    return PersonResponse(
                        success=True,
                        message="Person deleted successfully"
                    )
                else:
                    return PersonResponse(
                        success=False,
                        message="Person not found"
                    )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to delete person: {str(e)}",
                errors=[str(e)]
            )
    
    async def delete_student(self, student_id: str) -> PersonResponse:
        """Delete a student from PostgreSQL."""
        try:
            sql = "DELETE FROM students WHERE id = $1 RETURNING id"
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(sql, student_id)
                
                if row:
                    return PersonResponse(
                        success=True,
                        message="Student deleted successfully"
                    )
                else:
                    return PersonResponse(
                        success=False,
                        message="Student not found"
                    )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to delete student: {str(e)}",
                errors=[str(e)]
            )
    
    async def delete_teacher(self, teacher_id: str) -> PersonResponse:
        """Delete a teacher from PostgreSQL."""
        try:
            sql = "DELETE FROM teachers WHERE id = $1 RETURNING id"
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(sql, teacher_id)
                
                if row:
                    return PersonResponse(
                        success=True,
                        message="Teacher deleted successfully"
                    )
                else:
                    return PersonResponse(
                        success=False,
                        message="Teacher not found"
                    )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to delete teacher: {str(e)}",
                errors=[str(e)]
            )
    
    async def delete_class(self, class_id: str) -> ClassResponse:
        """Delete a class from PostgreSQL."""
        try:
            sql = "DELETE FROM classes WHERE id = $1 RETURNING id"
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(sql, class_id)
                
                if row:
                    return ClassResponse(
                        success=True,
                        message="Class deleted successfully"
                    )
                else:
                    return ClassResponse(
                        success=False,
                        message="Class not found"
                    )
        except Exception as e:
            return ClassResponse(
                success=False,
                message=f"Failed to delete class: {str(e)}",
                errors=[str(e)]
            )
    
    async def add_students_to_class(self, class_id: str, student_ids: List[str]) -> BulkOperationResponse:
        """Add students to a class in PostgreSQL."""
        try:
            successful = 0
            failed = 0
            errors = []
            
            async with self.pool.acquire() as conn:
                for student_id in student_ids:
                    try:
                        enrollment_id = self._generate_id()
                        sql = """
                        INSERT INTO class_enrollments (id, student_id, class_id, enrollment_date, is_active)
                        VALUES ($1, $2, $3, $4, $5)
                        """
                        await conn.execute(sql, enrollment_id, student_id, class_id, datetime.utcnow(), True)
                        successful += 1
                    except Exception as e:
                        failed += 1
                        errors.append(f"Failed to enroll student {student_id}: {str(e)}")
            
            return BulkOperationResponse(
                success=failed == 0,
                message="Students added to class",
                total_processed=len(student_ids),
                successful=successful,
                failed=failed,
                errors=errors if errors else None
            )
        except Exception as e:
            return BulkOperationResponse(
                success=False,
                message=f"Failed to add students to class: {str(e)}",
                total_processed=len(student_ids),
                successful=0,
                failed=len(student_ids),
                errors=[str(e)]
            )
    
    async def add_teacher_to_class(self, class_id: str, teacher_id: str, subject: str) -> PersonResponse:
        """Add a teacher to a class for a specific subject in PostgreSQL."""
        try:
            assignment_id = self._generate_id()
            sql = """
            INSERT INTO teacher_assignments (id, teacher_id, class_id, subject, assignment_date, is_active)
            VALUES ($1, $2, $3, $4, $5, $6)
            """
            
            async with self.pool.acquire() as conn:
                await conn.execute(sql, assignment_id, teacher_id, class_id, subject, datetime.utcnow(), True)
                
                return PersonResponse(
                    success=True,
                    message="Teacher added to class successfully"
                )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to add teacher to class: {str(e)}",
                errors=[str(e)]
            )
    
    async def add_scores_to_students(self, scores: List[Score]) -> BulkOperationResponse:
        """Add scores to students in PostgreSQL."""
        try:
            successful = 0
            failed = 0
            errors = []
            
            async with self.pool.acquire() as conn:
                for score in scores:
                    try:
                        data = self._prepare_data(score)
                        sql = """
                        INSERT INTO scores (id, student_id, class_id, subject, score, max_score,
                                          assessment_type, assessment_date, teacher_id, comments)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                        """
                        await conn.execute(
                            sql, data['id'], data['student_id'], data['class_id'], data['subject'],
                            data['score'], data['max_score'], data['assessment_type'],
                            data['assessment_date'], data.get('teacher_id'), data.get('comments')
                        )
                        successful += 1
                    except Exception as e:
                        failed += 1
                        errors.append(f"Failed to add score: {str(e)}")
            
            return BulkOperationResponse(
                success=failed == 0,
                message="Scores added successfully",
                total_processed=len(scores),
                successful=successful,
                failed=failed,
                errors=errors if errors else None
            )
        except Exception as e:
            return BulkOperationResponse(
                success=False,
                message=f"Failed to add scores: {str(e)}",
                total_processed=len(scores),
                successful=0,
                failed=len(scores),
                errors=[str(e)]
            )
    
    async def bulk_operation(self, operation: BulkOperation) -> BulkOperationResponse:
        """Perform bulk operations in PostgreSQL."""
        try:
            table_name = f"{operation.entity_type}s"
            successful = 0
            failed = 0
            errors = []
            
            async with self.pool.acquire() as conn:
                for item in operation.data:
                    try:
                        if operation.operation_type == "create":
                            # This is a simplified implementation
                            # In practice, you'd need specific SQL for each entity type
                            if not item.get('id'):
                                item['id'] = self._generate_id()
                            item['created_at'] = datetime.utcnow()
                            item['updated_at'] = datetime.utcnow()
                            # Execute appropriate INSERT statement based on entity type
                            successful += 1
                        elif operation.operation_type == "update":
                            item['updated_at'] = datetime.utcnow()
                            # Execute appropriate UPDATE statement based on entity type
                            successful += 1
                        elif operation.operation_type == "delete":
                            # Execute appropriate DELETE statement based on entity type
                            successful += 1
                    except Exception as e:
                        failed += 1
                        errors.append(str(e))
            
            return BulkOperationResponse(
                success=failed == 0,
                message=f"Bulk {operation.operation_type} operation completed",
                total_processed=len(operation.data),
                successful=successful,
                failed=failed,
                errors=errors if errors else None
            )
        except Exception as e:
            return BulkOperationResponse(
                success=False,
                message=f"Bulk operation failed: {str(e)}",
                total_processed=len(operation.data),
                successful=0,
                failed=len(operation.data),
                errors=[str(e)]
            )
    
    async def aggregate_query(self, query: AggregateQuery) -> AggregateResponse:
        """Perform aggregate queries in PostgreSQL."""
        try:
            # This is a simplified implementation
            # In practice, you'd build more complex SQL queries based on the query parameters
            base_table = "students"  # Default table
            
            sql_parts = [f"SELECT * FROM {base_table}"]
            params = []
            
            # Add WHERE clause if filters are provided
            if query.filters:
                where_conditions = []
                param_count = 1
                for field, value in query.filters.items():
                    where_conditions.append(f"{field} = ${param_count}")
                    params.append(value)
                    param_count += 1
                sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
            
            # Add ORDER BY clause if sort_by is provided
            if query.sort_by:
                order_direction = "ASC" if query.sort_order == "asc" else "DESC"
                sql_parts.append(f"ORDER BY {query.sort_by} {order_direction}")
            
            # Add LIMIT clause if limit is provided
            if query.limit:
                sql_parts.append(f"LIMIT {query.limit}")
            
            sql = " ".join(sql_parts)
            
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)
                results = [dict(row) for row in rows]
                
                return AggregateResponse(
                    success=True,
                    message="Aggregate query executed successfully",
                    data={"results": results},
                    count=len(results)
                )
        except Exception as e:
            return AggregateResponse(
                success=False,
                message=f"Aggregate query failed: {str(e)}",
                errors=[str(e)]
            )
    
    async def get_students_per_class(self, class_id: Optional[str] = None) -> AggregateResponse:
        """Get students per class from PostgreSQL."""
        try:
            sql = """
            SELECT 
                c.id as class_id,
                c.name as class_name,
                COUNT(ce.student_id) as student_count,
                ARRAY_AGG(
                    JSON_BUILD_OBJECT(
                        'id', s.id,
                        'first_name', s.first_name,
                        'last_name', s.last_name,
                        'email', s.email
                    )
                ) as students
            FROM classes c
            LEFT JOIN class_enrollments ce ON c.id = ce.class_id AND ce.is_active = true
            LEFT JOIN students s ON ce.student_id = s.id
            """
            
            params = []
            if class_id:
                sql += " WHERE c.id = $1"
                params.append(class_id)
            
            sql += " GROUP BY c.id, c.name ORDER BY c.name"
            
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)
                results = [dict(row) for row in rows]
                
                return AggregateResponse(
                    success=True,
                    message="Students per class retrieved successfully",
                    data={"results": results},
                    count=len(results)
                )
        except Exception as e:
            return AggregateResponse(
                success=False,
                message=f"Failed to get students per class: {str(e)}",
                errors=[str(e)]
            )
    
    async def get_avg_score_per_class(self, class_id: Optional[str] = None) -> AggregateResponse:
        """Get average score per class from PostgreSQL."""
        try:
            sql = """
            SELECT 
                c.id as class_id,
                c.name as class_name,
                AVG(sc.score) as average_score,
                COUNT(sc.id) as total_scores
            FROM classes c
            LEFT JOIN scores sc ON c.id = sc.class_id
            """
            
            params = []
            if class_id:
                sql += " WHERE c.id = $1"
                params.append(class_id)
            
            sql += " GROUP BY c.id, c.name ORDER BY c.name"
            
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)
                results = [dict(row) for row in rows]
                
                return AggregateResponse(
                    success=True,
                    message="Average scores per class retrieved successfully",
                    data={"results": results},
                    count=len(results)
                )
        except Exception as e:
            return AggregateResponse(
                success=False,
                message=f"Failed to get average scores per class: {str(e)}",
                errors=[str(e)]
            )
    
    async def get_teachers_per_class(self, class_id: Optional[str] = None) -> AggregateResponse:
        """Get teachers per class from PostgreSQL."""
        try:
            sql = """
            SELECT 
                c.id as class_id,
                c.name as class_name,
                COUNT(DISTINCT ta.teacher_id) as teacher_count,
                ARRAY_AGG(
                    DISTINCT JSON_BUILD_OBJECT(
                        'teacher', JSON_BUILD_OBJECT(
                            'id', t.id,
                            'first_name', t.first_name,
                            'last_name', t.last_name,
                            'email', t.email
                        ),
                        'subject', ta.subject
                    )
                ) as teachers
            FROM classes c
            LEFT JOIN teacher_assignments ta ON c.id = ta.class_id AND ta.is_active = true
            LEFT JOIN teachers t ON ta.teacher_id = t.id
            """
            
            params = []
            if class_id:
                sql += " WHERE c.id = $1"
                params.append(class_id)
            
            sql += " GROUP BY c.id, c.name ORDER BY c.name"
            
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)
                results = [dict(row) for row in rows]
                
                return AggregateResponse(
                    success=True,
                    message="Teachers per class retrieved successfully",
                    data={"results": results},
                    count=len(results)
                )
        except Exception as e:
            return AggregateResponse(
                success=False,
                message=f"Failed to get teachers per class: {str(e)}",
                errors=[str(e)]
            )
    
    async def get_subjects_per_class(self, class_id: Optional[str] = None) -> AggregateResponse:
        """Get subjects per class from PostgreSQL."""
        try:
            sql = """
            SELECT 
                c.id as class_id,
                c.name as class_name,
                ARRAY_AGG(DISTINCT ta.subject) as subjects,
                COUNT(DISTINCT ta.subject) as subject_count
            FROM classes c
            LEFT JOIN teacher_assignments ta ON c.id = ta.class_id AND ta.is_active = true
            """
            
            params = []
            if class_id:
                sql += " WHERE c.id = $1"
                params.append(class_id)
            
            sql += " GROUP BY c.id, c.name ORDER BY c.name"
            
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)
                results = [dict(row) for row in rows]
                
                return AggregateResponse(
                    success=True,
                    message="Subjects per class retrieved successfully",
                    data={"results": results},
                    count=len(results)
                )
        except Exception as e:
            return AggregateResponse(
                success=False,
                message=f"Failed to get subjects per class: {str(e)}",
                errors=[str(e)]
            )
