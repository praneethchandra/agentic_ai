"""
Database interface layer with abstract base classes and concrete implementations
for MongoDB, Elasticsearch, and PostgreSQL.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
import json
import uuid
from datetime import datetime

from .models import (
    Person, Student, Teacher, Class, ClassEnrollment, 
    TeacherAssignment, Score, BulkOperation, AggregateQuery,
    PersonResponse, ClassResponse, BulkOperationResponse, AggregateResponse
)


class DatabaseInterface(ABC):
    """Abstract base class for database operations."""
    
    @abstractmethod
    async def connect(self) -> bool:
        """Connect to the database."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> bool:
        """Disconnect from the database."""
        pass
    
    async def initialize(self) -> bool:
        """Initialize the database connection."""
        return await self.connect()
    
    async def cleanup(self) -> bool:
        """Cleanup database connections."""
        return await self.disconnect()
    
    @abstractmethod
    async def create_person(self, person: Person) -> PersonResponse:
        """Create a new person."""
        pass
    
    @abstractmethod
    async def create_student(self, student: Student) -> PersonResponse:
        """Create a new student."""
        pass
    
    @abstractmethod
    async def create_teacher(self, teacher: Teacher) -> PersonResponse:
        """Create a new teacher."""
        pass
    
    @abstractmethod
    async def create_class(self, class_obj: Class) -> ClassResponse:
        """Create a new class."""
        pass
    
    @abstractmethod
    async def get_person(self, person_id: str) -> PersonResponse:
        """Get a person by ID."""
        pass
    
    @abstractmethod
    async def get_student(self, student_id: str) -> PersonResponse:
        """Get a student by ID."""
        pass
    
    @abstractmethod
    async def get_teacher(self, teacher_id: str) -> PersonResponse:
        """Get a teacher by ID."""
        pass
    
    @abstractmethod
    async def get_class(self, class_id: str) -> ClassResponse:
        """Get a class by ID."""
        pass
    
    @abstractmethod
    async def update_person(self, person_id: str, person: Person) -> PersonResponse:
        """Update a person."""
        pass
    
    @abstractmethod
    async def update_student(self, student_id: str, student: Student) -> PersonResponse:
        """Update a student."""
        pass
    
    @abstractmethod
    async def update_teacher(self, teacher_id: str, teacher: Teacher) -> PersonResponse:
        """Update a teacher."""
        pass
    
    @abstractmethod
    async def update_class(self, class_id: str, class_obj: Class) -> ClassResponse:
        """Update a class."""
        pass
    
    @abstractmethod
    async def delete_person(self, person_id: str) -> PersonResponse:
        """Delete a person."""
        pass
    
    @abstractmethod
    async def delete_student(self, student_id: str) -> PersonResponse:
        """Delete a student."""
        pass
    
    @abstractmethod
    async def delete_teacher(self, teacher_id: str) -> PersonResponse:
        """Delete a teacher."""
        pass
    
    @abstractmethod
    async def delete_class(self, class_id: str) -> ClassResponse:
        """Delete a class."""
        pass
    
    @abstractmethod
    async def add_students_to_class(self, class_id: str, student_ids: List[str]) -> BulkOperationResponse:
        """Add students to a class."""
        pass
    
    @abstractmethod
    async def add_teacher_to_class(self, class_id: str, teacher_id: str, subject: str) -> PersonResponse:
        """Add a teacher to a class for a specific subject."""
        pass
    
    @abstractmethod
    async def add_scores_to_students(self, scores: List[Score]) -> BulkOperationResponse:
        """Add scores to students."""
        pass
    
    @abstractmethod
    async def bulk_operation(self, operation: BulkOperation) -> BulkOperationResponse:
        """Perform bulk operations."""
        pass
    
    @abstractmethod
    async def aggregate_query(self, query: AggregateQuery) -> AggregateResponse:
        """Perform aggregate queries."""
        pass
    
    @abstractmethod
    async def get_students_per_class(self, class_id: Optional[str] = None) -> AggregateResponse:
        """Get students per class."""
        pass
    
    @abstractmethod
    async def get_avg_score_per_class(self, class_id: Optional[str] = None) -> AggregateResponse:
        """Get average score per class."""
        pass
    
    @abstractmethod
    async def get_teachers_per_class(self, class_id: Optional[str] = None) -> AggregateResponse:
        """Get teachers per class."""
        pass
    
    @abstractmethod
    async def get_subjects_per_class(self, class_id: Optional[str] = None) -> AggregateResponse:
        """Get subjects per class."""
        pass


class MongoDBInterface(DatabaseInterface):
    """MongoDB implementation of the database interface."""
    
    def __init__(self, connection_string: str, database_name: str):
        self.connection_string = connection_string
        self.database_name = database_name
        self.client = None
        self.db = None
    
    async def connect(self) -> bool:
        """Connect to MongoDB."""
        try:
            from motor.motor_asyncio import AsyncIOMotorClient
            self.client = AsyncIOMotorClient(self.connection_string)
            self.db = self.client[self.database_name]
            # Test connection
            await self.client.admin.command('ping')
            return True
        except Exception as e:
            print(f"MongoDB connection error: {e}")
            return False
    
    async def disconnect(self) -> bool:
        """Disconnect from MongoDB."""
        try:
            if self.client:
                self.client.close()
            return True
        except Exception as e:
            print(f"MongoDB disconnection error: {e}")
            return False
    
    def _generate_id(self) -> str:
        """Generate a unique ID."""
        return str(uuid.uuid4())
    
    def _prepare_document(self, obj: Any) -> Dict[str, Any]:
        """Prepare a Pydantic model for MongoDB storage."""
        doc = obj.dict()
        if not doc.get('id'):
            doc['id'] = self._generate_id()
        doc['_id'] = doc['id']
        doc['updated_at'] = datetime.utcnow()
        return doc
    
    async def create_person(self, person: Person) -> PersonResponse:
        """Create a new person in MongoDB."""
        try:
            doc = self._prepare_document(person)
            await self.db.persons.insert_one(doc)
            return PersonResponse(
                success=True,
                message="Person created successfully",
                data=Person(**doc)
            )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to create person: {str(e)}",
                errors=[str(e)]
            )
    
    async def create_student(self, student: Student) -> PersonResponse:
        """Create a new student in MongoDB."""
        try:
            doc = self._prepare_document(student)
            await self.db.students.insert_one(doc)
            return PersonResponse(
                success=True,
                message="Student created successfully",
                data=Student(**doc)
            )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to create student: {str(e)}",
                errors=[str(e)]
            )
    
    async def create_teacher(self, teacher: Teacher) -> PersonResponse:
        """Create a new teacher in MongoDB."""
        try:
            doc = self._prepare_document(teacher)
            await self.db.teachers.insert_one(doc)
            return PersonResponse(
                success=True,
                message="Teacher created successfully",
                data=Teacher(**doc)
            )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to create teacher: {str(e)}",
                errors=[str(e)]
            )
    
    async def create_class(self, class_obj: Class) -> ClassResponse:
        """Create a new class in MongoDB."""
        try:
            doc = self._prepare_document(class_obj)
            await self.db.classes.insert_one(doc)
            return ClassResponse(
                success=True,
                message="Class created successfully",
                data=Class(**doc)
            )
        except Exception as e:
            return ClassResponse(
                success=False,
                message=f"Failed to create class: {str(e)}",
                errors=[str(e)]
            )
    
    async def get_person(self, person_id: str) -> PersonResponse:
        """Get a person by ID from MongoDB."""
        try:
            doc = await self.db.persons.find_one({"_id": person_id})
            if doc:
                return PersonResponse(
                    success=True,
                    message="Person found",
                    data=Person(**doc)
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
        """Get a student by ID from MongoDB."""
        try:
            doc = await self.db.students.find_one({"_id": student_id})
            if doc:
                return PersonResponse(
                    success=True,
                    message="Student found",
                    data=Student(**doc)
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
        """Get a teacher by ID from MongoDB."""
        try:
            doc = await self.db.teachers.find_one({"_id": teacher_id})
            if doc:
                return PersonResponse(
                    success=True,
                    message="Teacher found",
                    data=Teacher(**doc)
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
        """Get a class by ID from MongoDB."""
        try:
            doc = await self.db.classes.find_one({"_id": class_id})
            if doc:
                return ClassResponse(
                    success=True,
                    message="Class found",
                    data=Class(**doc)
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
        """Update a person in MongoDB."""
        try:
            doc = self._prepare_document(person)
            doc['_id'] = person_id
            doc['id'] = person_id
            result = await self.db.persons.replace_one({"_id": person_id}, doc)
            if result.matched_count:
                return PersonResponse(
                    success=True,
                    message="Person updated successfully",
                    data=Person(**doc)
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
        """Update a student in MongoDB."""
        try:
            doc = self._prepare_document(student)
            doc['_id'] = student_id
            doc['id'] = student_id
            result = await self.db.students.replace_one({"_id": student_id}, doc)
            if result.matched_count:
                return PersonResponse(
                    success=True,
                    message="Student updated successfully",
                    data=Student(**doc)
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
        """Update a teacher in MongoDB."""
        try:
            doc = self._prepare_document(teacher)
            doc['_id'] = teacher_id
            doc['id'] = teacher_id
            result = await self.db.teachers.replace_one({"_id": teacher_id}, doc)
            if result.matched_count:
                return PersonResponse(
                    success=True,
                    message="Teacher updated successfully",
                    data=Teacher(**doc)
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
        """Update a class in MongoDB."""
        try:
            doc = self._prepare_document(class_obj)
            doc['_id'] = class_id
            doc['id'] = class_id
            result = await self.db.classes.replace_one({"_id": class_id}, doc)
            if result.matched_count:
                return ClassResponse(
                    success=True,
                    message="Class updated successfully",
                    data=Class(**doc)
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
        """Delete a person from MongoDB."""
        try:
            result = await self.db.persons.delete_one({"_id": person_id})
            if result.deleted_count:
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
        """Delete a student from MongoDB."""
        try:
            result = await self.db.students.delete_one({"_id": student_id})
            if result.deleted_count:
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
        """Delete a teacher from MongoDB."""
        try:
            result = await self.db.teachers.delete_one({"_id": teacher_id})
            if result.deleted_count:
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
        """Delete a class from MongoDB."""
        try:
            result = await self.db.classes.delete_one({"_id": class_id})
            if result.deleted_count:
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
        """Add students to a class in MongoDB."""
        try:
            enrollments = []
            for student_id in student_ids:
                enrollment = ClassEnrollment(
                    id=self._generate_id(),
                    student_id=student_id,
                    class_id=class_id
                )
                enrollments.append(self._prepare_document(enrollment))
            
            result = await self.db.class_enrollments.insert_many(enrollments)
            return BulkOperationResponse(
                success=True,
                message="Students added to class successfully",
                total_processed=len(student_ids),
                successful=len(result.inserted_ids),
                failed=0
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
        """Add a teacher to a class for a specific subject in MongoDB."""
        try:
            assignment = TeacherAssignment(
                id=self._generate_id(),
                teacher_id=teacher_id,
                class_id=class_id,
                subject=subject
            )
            doc = self._prepare_document(assignment)
            await self.db.teacher_assignments.insert_one(doc)
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
        """Add scores to students in MongoDB."""
        try:
            score_docs = [self._prepare_document(score) for score in scores]
            result = await self.db.scores.insert_many(score_docs)
            return BulkOperationResponse(
                success=True,
                message="Scores added successfully",
                total_processed=len(scores),
                successful=len(result.inserted_ids),
                failed=0
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
        """Perform bulk operations in MongoDB."""
        try:
            collection_name = f"{operation.entity_type}s"
            collection = self.db[collection_name]
            
            successful = 0
            failed = 0
            errors = []
            
            if operation.operation_type == "create":
                for item in operation.data:
                    try:
                        if not item.get('id'):
                            item['id'] = self._generate_id()
                        item['_id'] = item['id']
                        item['created_at'] = datetime.utcnow()
                        item['updated_at'] = datetime.utcnow()
                        await collection.insert_one(item)
                        successful += 1
                    except Exception as e:
                        failed += 1
                        errors.append(str(e))
            
            elif operation.operation_type == "update":
                for item in operation.data:
                    try:
                        item_id = item.get('id')
                        if item_id:
                            item['updated_at'] = datetime.utcnow()
                            result = await collection.replace_one({"_id": item_id}, item)
                            if result.matched_count:
                                successful += 1
                            else:
                                failed += 1
                                errors.append(f"Item with id {item_id} not found")
                        else:
                            failed += 1
                            errors.append("Item ID is required for update operation")
                    except Exception as e:
                        failed += 1
                        errors.append(str(e))
            
            elif operation.operation_type == "delete":
                for item in operation.data:
                    try:
                        item_id = item.get('id')
                        if item_id:
                            result = await collection.delete_one({"_id": item_id})
                            if result.deleted_count:
                                successful += 1
                            else:
                                failed += 1
                                errors.append(f"Item with id {item_id} not found")
                        else:
                            failed += 1
                            errors.append("Item ID is required for delete operation")
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
        """Perform aggregate queries in MongoDB."""
        try:
            # This is a simplified implementation - in practice, you'd build more complex aggregation pipelines
            pipeline = []
            
            # Add match stage if filters are provided
            if query.filters:
                pipeline.append({"$match": query.filters})
            
            # Add group stage if group_by is provided
            if query.group_by:
                group_stage = {"$group": {"_id": {}}}
                for field in query.group_by:
                    group_stage["$group"]["_id"][field] = f"${field}"
                group_stage["$group"]["count"] = {"$sum": 1}
                pipeline.append(group_stage)
            
            # Add sort stage if sort_by is provided
            if query.sort_by:
                sort_order = 1 if query.sort_order == "asc" else -1
                pipeline.append({"$sort": {query.sort_by: sort_order}})
            
            # Add limit stage if limit is provided
            if query.limit:
                pipeline.append({"$limit": query.limit})
            
            # Execute the aggregation
            collection_name = "students"  # Default collection, should be determined by query_type
            collection = self.db[collection_name]
            cursor = collection.aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
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
        """Get students per class from MongoDB."""
        try:
            pipeline = [
                {
                    "$lookup": {
                        "from": "students",
                        "localField": "student_id",
                        "foreignField": "_id",
                        "as": "student"
                    }
                },
                {
                    "$lookup": {
                        "from": "classes",
                        "localField": "class_id",
                        "foreignField": "_id",
                        "as": "class"
                    }
                },
                {
                    "$group": {
                        "_id": "$class_id",
                        "class_name": {"$first": {"$arrayElemAt": ["$class.name", 0]}},
                        "student_count": {"$sum": 1},
                        "students": {"$push": {"$arrayElemAt": ["$student", 0]}}
                    }
                }
            ]
            
            if class_id:
                pipeline.insert(0, {"$match": {"class_id": class_id}})
            
            cursor = self.db.class_enrollments.aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
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
        """Get average score per class from MongoDB."""
        try:
            pipeline = [
                {
                    "$lookup": {
                        "from": "classes",
                        "localField": "class_id",
                        "foreignField": "_id",
                        "as": "class"
                    }
                },
                {
                    "$group": {
                        "_id": "$class_id",
                        "class_name": {"$first": {"$arrayElemAt": ["$class.name", 0]}},
                        "average_score": {"$avg": "$score"},
                        "total_scores": {"$sum": 1}
                    }
                }
            ]
            
            if class_id:
                pipeline.insert(0, {"$match": {"class_id": class_id}})
            
            cursor = self.db.scores.aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
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
        """Get teachers per class from MongoDB."""
        try:
            pipeline = [
                {
                    "$lookup": {
                        "from": "teachers",
                        "localField": "teacher_id",
                        "foreignField": "_id",
                        "as": "teacher"
                    }
                },
                {
                    "$lookup": {
                        "from": "classes",
                        "localField": "class_id",
                        "foreignField": "_id",
                        "as": "class"
                    }
                },
                {
                    "$group": {
                        "_id": "$class_id",
                        "class_name": {"$first": {"$arrayElemAt": ["$class.name", 0]}},
                        "teacher_count": {"$sum": 1},
                        "teachers": {"$push": {
                            "teacher": {"$arrayElemAt": ["$teacher", 0]},
                            "subject": "$subject"
                        }}
                    }
                }
            ]
            
            if class_id:
                pipeline.insert(0, {"$match": {"class_id": class_id}})
            
            cursor = self.db.teacher_assignments.aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
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
        """Get subjects per class from MongoDB."""
        try:
            pipeline = [
                {
                    "$lookup": {
                        "from": "classes",
                        "localField": "class_id",
                        "foreignField": "_id",
                        "as": "class"
                    }
                },
                {
                    "$group": {
                        "_id": "$class_id",
                        "class_name": {"$first": {"$arrayElemAt": ["$class.name", 0]}},
                        "subjects": {"$addToSet": "$subject"},
                        "subject_count": {"$sum": 1}
                    }
                }
            ]
            
            if class_id:
                pipeline.insert(0, {"$match": {"class_id": class_id}})
            
            cursor = self.db.teacher_assignments.aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
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
