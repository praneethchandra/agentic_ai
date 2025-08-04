"""
Elasticsearch implementation of the database interface.
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


class ElasticsearchInterface(DatabaseInterface):
    """Elasticsearch implementation of the database interface."""
    
    def __init__(self, hosts: List[str], index_prefix: str = "school"):
        self.hosts = hosts
        self.index_prefix = index_prefix
        self.client = None
        self.indices = {
            'persons': f"{index_prefix}_persons",
            'students': f"{index_prefix}_students", 
            'teachers': f"{index_prefix}_teachers",
            'classes': f"{index_prefix}_classes",
            'class_enrollments': f"{index_prefix}_class_enrollments",
            'teacher_assignments': f"{index_prefix}_teacher_assignments",
            'scores': f"{index_prefix}_scores"
        }
    
    async def connect(self) -> bool:
        """Connect to Elasticsearch."""
        try:
            from elasticsearch import AsyncElasticsearch
            self.client = AsyncElasticsearch(hosts=self.hosts)
            # Test connection
            await self.client.ping()
            await self._create_indices()
            return True
        except Exception as e:
            print(f"Elasticsearch connection error: {e}")
            return False
    
    async def disconnect(self) -> bool:
        """Disconnect from Elasticsearch."""
        try:
            if self.client:
                await self.client.close()
            return True
        except Exception as e:
            print(f"Elasticsearch disconnection error: {e}")
            return False
    
    async def _create_indices(self):
        """Create indices with appropriate mappings."""
        mappings = {
            'persons': {
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "first_name": {"type": "text"},
                        "last_name": {"type": "text"},
                        "email": {"type": "keyword"},
                        "phone": {"type": "keyword"},
                        "date_of_birth": {"type": "date"},
                        "address": {"type": "text"},
                        "created_at": {"type": "date"},
                        "updated_at": {"type": "date"}
                    }
                }
            },
            'students': {
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "first_name": {"type": "text"},
                        "last_name": {"type": "text"},
                        "email": {"type": "keyword"},
                        "phone": {"type": "keyword"},
                        "date_of_birth": {"type": "date"},
                        "address": {"type": "text"},
                        "student_id": {"type": "keyword"},
                        "grade_level": {"type": "integer"},
                        "enrollment_date": {"type": "date"},
                        "is_active": {"type": "boolean"},
                        "guardian_contact": {"type": "text"},
                        "created_at": {"type": "date"},
                        "updated_at": {"type": "date"}
                    }
                }
            },
            'teachers': {
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "first_name": {"type": "text"},
                        "last_name": {"type": "text"},
                        "email": {"type": "keyword"},
                        "phone": {"type": "keyword"},
                        "date_of_birth": {"type": "date"},
                        "address": {"type": "text"},
                        "employee_id": {"type": "keyword"},
                        "subjects": {"type": "keyword"},
                        "hire_date": {"type": "date"},
                        "is_active": {"type": "boolean"},
                        "department": {"type": "keyword"},
                        "qualification": {"type": "text"},
                        "created_at": {"type": "date"},
                        "updated_at": {"type": "date"}
                    }
                }
            },
            'classes': {
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "name": {"type": "text"},
                        "description": {"type": "text"},
                        "gathering_type": {"type": "keyword"},
                        "capacity": {"type": "integer"},
                        "location": {"type": "text"},
                        "class_code": {"type": "keyword"},
                        "grade_level": {"type": "integer"},
                        "academic_year": {"type": "keyword"},
                        "semester": {"type": "keyword"},
                        "schedule": {"type": "object"},
                        "created_at": {"type": "date"},
                        "updated_at": {"type": "date"}
                    }
                }
            },
            'class_enrollments': {
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "student_id": {"type": "keyword"},
                        "class_id": {"type": "keyword"},
                        "enrollment_date": {"type": "date"},
                        "is_active": {"type": "boolean"}
                    }
                }
            },
            'teacher_assignments': {
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "teacher_id": {"type": "keyword"},
                        "class_id": {"type": "keyword"},
                        "subject": {"type": "keyword"},
                        "assignment_date": {"type": "date"},
                        "is_active": {"type": "boolean"}
                    }
                }
            },
            'scores': {
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "student_id": {"type": "keyword"},
                        "class_id": {"type": "keyword"},
                        "subject": {"type": "keyword"},
                        "score": {"type": "float"},
                        "max_score": {"type": "float"},
                        "assessment_type": {"type": "keyword"},
                        "assessment_date": {"type": "date"},
                        "teacher_id": {"type": "keyword"},
                        "comments": {"type": "text"}
                    }
                }
            }
        }
        
        for index_type, mapping in mappings.items():
            index_name = self.indices[index_type]
            try:
                exists = await self.client.indices.exists(index=index_name)
                if not exists:
                    await self.client.indices.create(index=index_name, body=mapping)
            except Exception as e:
                print(f"Error creating index {index_name}: {e}")
    
    def _generate_id(self) -> str:
        """Generate a unique ID."""
        return str(uuid.uuid4())
    
    def _prepare_document(self, obj: Any) -> Dict[str, Any]:
        """Prepare a Pydantic model for Elasticsearch storage."""
        doc = obj.dict()
        if not doc.get('id'):
            doc['id'] = self._generate_id()
        doc['updated_at'] = datetime.utcnow().isoformat()
        return doc
    
    async def create_person(self, person: Person) -> PersonResponse:
        """Create a new person in Elasticsearch."""
        try:
            doc = self._prepare_document(person)
            doc_id = doc['id']
            await self.client.index(
                index=self.indices['persons'],
                id=doc_id,
                body=doc
            )
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
        """Create a new student in Elasticsearch."""
        try:
            doc = self._prepare_document(student)
            doc_id = doc['id']
            await self.client.index(
                index=self.indices['students'],
                id=doc_id,
                body=doc
            )
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
        """Create a new teacher in Elasticsearch."""
        try:
            doc = self._prepare_document(teacher)
            doc_id = doc['id']
            await self.client.index(
                index=self.indices['teachers'],
                id=doc_id,
                body=doc
            )
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
        """Create a new class in Elasticsearch."""
        try:
            doc = self._prepare_document(class_obj)
            doc_id = doc['id']
            await self.client.index(
                index=self.indices['classes'],
                id=doc_id,
                body=doc
            )
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
        """Get a person by ID from Elasticsearch."""
        try:
            result = await self.client.get(
                index=self.indices['persons'],
                id=person_id
            )
            if result['found']:
                return PersonResponse(
                    success=True,
                    message="Person found",
                    data=Person(**result['_source'])
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
        """Get a student by ID from Elasticsearch."""
        try:
            result = await self.client.get(
                index=self.indices['students'],
                id=student_id
            )
            if result['found']:
                return PersonResponse(
                    success=True,
                    message="Student found",
                    data=Student(**result['_source'])
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
        """Get a teacher by ID from Elasticsearch."""
        try:
            result = await self.client.get(
                index=self.indices['teachers'],
                id=teacher_id
            )
            if result['found']:
                return PersonResponse(
                    success=True,
                    message="Teacher found",
                    data=Teacher(**result['_source'])
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
        """Get a class by ID from Elasticsearch."""
        try:
            result = await self.client.get(
                index=self.indices['classes'],
                id=class_id
            )
            if result['found']:
                return ClassResponse(
                    success=True,
                    message="Class found",
                    data=Class(**result['_source'])
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
        """Update a person in Elasticsearch."""
        try:
            doc = self._prepare_document(person)
            doc['id'] = person_id
            await self.client.index(
                index=self.indices['persons'],
                id=person_id,
                body=doc
            )
            return PersonResponse(
                success=True,
                message="Person updated successfully",
                data=Person(**doc)
            )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to update person: {str(e)}",
                errors=[str(e)]
            )
    
    async def update_student(self, student_id: str, student: Student) -> PersonResponse:
        """Update a student in Elasticsearch."""
        try:
            doc = self._prepare_document(student)
            doc['id'] = student_id
            await self.client.index(
                index=self.indices['students'],
                id=student_id,
                body=doc
            )
            return PersonResponse(
                success=True,
                message="Student updated successfully",
                data=Student(**doc)
            )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to update student: {str(e)}",
                errors=[str(e)]
            )
    
    async def update_teacher(self, teacher_id: str, teacher: Teacher) -> PersonResponse:
        """Update a teacher in Elasticsearch."""
        try:
            doc = self._prepare_document(teacher)
            doc['id'] = teacher_id
            await self.client.index(
                index=self.indices['teachers'],
                id=teacher_id,
                body=doc
            )
            return PersonResponse(
                success=True,
                message="Teacher updated successfully",
                data=Teacher(**doc)
            )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to update teacher: {str(e)}",
                errors=[str(e)]
            )
    
    async def update_class(self, class_id: str, class_obj: Class) -> ClassResponse:
        """Update a class in Elasticsearch."""
        try:
            doc = self._prepare_document(class_obj)
            doc['id'] = class_id
            await self.client.index(
                index=self.indices['classes'],
                id=class_id,
                body=doc
            )
            return ClassResponse(
                success=True,
                message="Class updated successfully",
                data=Class(**doc)
            )
        except Exception as e:
            return ClassResponse(
                success=False,
                message=f"Failed to update class: {str(e)}",
                errors=[str(e)]
            )
    
    async def delete_person(self, person_id: str) -> PersonResponse:
        """Delete a person from Elasticsearch."""
        try:
            await self.client.delete(
                index=self.indices['persons'],
                id=person_id
            )
            return PersonResponse(
                success=True,
                message="Person deleted successfully"
            )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to delete person: {str(e)}",
                errors=[str(e)]
            )
    
    async def delete_student(self, student_id: str) -> PersonResponse:
        """Delete a student from Elasticsearch."""
        try:
            await self.client.delete(
                index=self.indices['students'],
                id=student_id
            )
            return PersonResponse(
                success=True,
                message="Student deleted successfully"
            )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to delete student: {str(e)}",
                errors=[str(e)]
            )
    
    async def delete_teacher(self, teacher_id: str) -> PersonResponse:
        """Delete a teacher from Elasticsearch."""
        try:
            await self.client.delete(
                index=self.indices['teachers'],
                id=teacher_id
            )
            return PersonResponse(
                success=True,
                message="Teacher deleted successfully"
            )
        except Exception as e:
            return PersonResponse(
                success=False,
                message=f"Failed to delete teacher: {str(e)}",
                errors=[str(e)]
            )
    
    async def delete_class(self, class_id: str) -> ClassResponse:
        """Delete a class from Elasticsearch."""
        try:
            await self.client.delete(
                index=self.indices['classes'],
                id=class_id
            )
            return ClassResponse(
                success=True,
                message="Class deleted successfully"
            )
        except Exception as e:
            return ClassResponse(
                success=False,
                message=f"Failed to delete class: {str(e)}",
                errors=[str(e)]
            )
    
    async def add_students_to_class(self, class_id: str, student_ids: List[str]) -> BulkOperationResponse:
        """Add students to a class in Elasticsearch."""
        try:
            actions = []
            for student_id in student_ids:
                enrollment = ClassEnrollment(
                    id=self._generate_id(),
                    student_id=student_id,
                    class_id=class_id
                )
                doc = self._prepare_document(enrollment)
                actions.append({
                    "_index": self.indices['class_enrollments'],
                    "_id": doc['id'],
                    "_source": doc
                })
            
            from elasticsearch.helpers import async_bulk
            success_count, failed_items = await async_bulk(self.client, actions)
            
            return BulkOperationResponse(
                success=len(failed_items) == 0,
                message="Students added to class",
                total_processed=len(student_ids),
                successful=success_count,
                failed=len(failed_items)
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
        """Add a teacher to a class for a specific subject in Elasticsearch."""
        try:
            assignment = TeacherAssignment(
                id=self._generate_id(),
                teacher_id=teacher_id,
                class_id=class_id,
                subject=subject
            )
            doc = self._prepare_document(assignment)
            await self.client.index(
                index=self.indices['teacher_assignments'],
                id=doc['id'],
                body=doc
            )
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
        """Add scores to students in Elasticsearch."""
        try:
            actions = []
            for score in scores:
                doc = self._prepare_document(score)
                actions.append({
                    "_index": self.indices['scores'],
                    "_id": doc['id'],
                    "_source": doc
                })
            
            from elasticsearch.helpers import async_bulk
            success_count, failed_items = await async_bulk(self.client, actions)
            
            return BulkOperationResponse(
                success=len(failed_items) == 0,
                message="Scores added successfully",
                total_processed=len(scores),
                successful=success_count,
                failed=len(failed_items)
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
        """Perform bulk operations in Elasticsearch."""
        try:
            index_name = self.indices.get(f"{operation.entity_type}s")
            if not index_name:
                return BulkOperationResponse(
                    success=False,
                    message=f"Unknown entity type: {operation.entity_type}",
                    total_processed=len(operation.data),
                    successful=0,
                    failed=len(operation.data),
                    errors=[f"Unknown entity type: {operation.entity_type}"]
                )
            
            actions = []
            for item in operation.data:
                if operation.operation_type == "create":
                    if not item.get('id'):
                        item['id'] = self._generate_id()
                    item['created_at'] = datetime.utcnow().isoformat()
                    item['updated_at'] = datetime.utcnow().isoformat()
                    actions.append({
                        "_index": index_name,
                        "_id": item['id'],
                        "_source": item
                    })
                elif operation.operation_type == "update":
                    item['updated_at'] = datetime.utcnow().isoformat()
                    actions.append({
                        "_index": index_name,
                        "_id": item['id'],
                        "_source": item
                    })
                elif operation.operation_type == "delete":
                    actions.append({
                        "_op_type": "delete",
                        "_index": index_name,
                        "_id": item['id']
                    })
            
            from elasticsearch.helpers import async_bulk
            success_count, failed_items = await async_bulk(self.client, actions)
            
            return BulkOperationResponse(
                success=len(failed_items) == 0,
                message=f"Bulk {operation.operation_type} operation completed",
                total_processed=len(operation.data),
                successful=success_count,
                failed=len(failed_items)
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
        """Perform aggregate queries in Elasticsearch."""
        try:
            # Build Elasticsearch aggregation query
            body = {
                "size": 0,
                "aggs": {}
            }
            
            # Add filters if provided
            if query.filters:
                body["query"] = {"bool": {"must": []}}
                for field, value in query.filters.items():
                    body["query"]["bool"]["must"].append({"term": {field: value}})
            
            # Add aggregations based on group_by
            if query.group_by:
                agg_name = "grouped_results"
                body["aggs"][agg_name] = {
                    "terms": {
                        "field": query.group_by[0],  # Simplified - only first group field
                        "size": query.limit or 100
                    }
                }
            
            # Execute the search
            index_name = self.indices['students']  # Default index
            result = await self.client.search(index=index_name, body=body)
            
            return AggregateResponse(
                success=True,
                message="Aggregate query executed successfully",
                data={"results": result['aggregations'] if 'aggregations' in result else []},
                count=result['hits']['total']['value'] if 'hits' in result else 0
            )
        except Exception as e:
            return AggregateResponse(
                success=False,
                message=f"Aggregate query failed: {str(e)}",
                errors=[str(e)]
            )
    
    async def get_students_per_class(self, class_id: Optional[str] = None) -> AggregateResponse:
        """Get students per class from Elasticsearch."""
        try:
            body = {
                "size": 0,
                "aggs": {
                    "classes": {
                        "terms": {
                            "field": "class_id",
                            "size": 100
                        },
                        "aggs": {
                            "student_count": {
                                "value_count": {
                                    "field": "student_id"
                                }
                            }
                        }
                    }
                }
            }
            
            if class_id:
                body["query"] = {
                    "term": {"class_id": class_id}
                }
            
            result = await self.client.search(
                index=self.indices['class_enrollments'],
                body=body
            )
            
            return AggregateResponse(
                success=True,
                message="Students per class retrieved successfully",
                data={"results": result['aggregations']['classes']['buckets']},
                count=len(result['aggregations']['classes']['buckets'])
            )
        except Exception as e:
            return AggregateResponse(
                success=False,
                message=f"Failed to get students per class: {str(e)}",
                errors=[str(e)]
            )
    
    async def get_avg_score_per_class(self, class_id: Optional[str] = None) -> AggregateResponse:
        """Get average score per class from Elasticsearch."""
        try:
            body = {
                "size": 0,
                "aggs": {
                    "classes": {
                        "terms": {
                            "field": "class_id",
                            "size": 100
                        },
                        "aggs": {
                            "average_score": {
                                "avg": {
                                    "field": "score"
                                }
                            }
                        }
                    }
                }
            }
            
            if class_id:
                body["query"] = {
                    "term": {"class_id": class_id}
                }
            
            result = await self.client.search(
                index=self.indices['scores'],
                body=body
            )
            
            return AggregateResponse(
                success=True,
                message="Average scores per class retrieved successfully",
                data={"results": result['aggregations']['classes']['buckets']},
                count=len(result['aggregations']['classes']['buckets'])
            )
        except Exception as e:
            return AggregateResponse(
                success=False,
                message=f"Failed to get average scores per class: {str(e)}",
                errors=[str(e)]
            )
    
    async def get_teachers_per_class(self, class_id: Optional[str] = None) -> AggregateResponse:
        """Get teachers per class from Elasticsearch."""
        try:
            body = {
                "size": 0,
                "aggs": {
                    "classes": {
                        "terms": {
                            "field": "class_id",
                            "size": 100
                        },
                        "aggs": {
                            "teacher_count": {
                                "cardinality": {
                                    "field": "teacher_id"
                                }
                            },
                            "subjects": {
                                "terms": {
                                    "field": "subject",
                                    "size": 20
                                }
                            }
                        }
                    }
                }
            }
            
            if class_id:
                body["query"] = {
                    "term": {"class_id": class_id}
                }
            
            result = await self.client.search(
                index=self.indices['teacher_assignments'],
                body=body
            )
            
            return AggregateResponse(
                success=True,
                message="Teachers per class retrieved successfully",
                data={"results": result['aggregations']['classes']['buckets']},
                count=len(result['aggregations']['classes']['buckets'])
            )
        except Exception as e:
            return AggregateResponse(
                success=False,
                message=f"Failed to get teachers per class: {str(e)}",
                errors=[str(e)]
            )
    
    async def get_subjects_per_class(self, class_id: Optional[str] = None) -> AggregateResponse:
        """Get subjects per class from Elasticsearch."""
        try:
            body = {
                "size": 0,
                "aggs": {
                    "classes": {
                        "terms": {
                            "field": "class_id",
                            "size": 100
                        },
                        "aggs": {
                            "subjects": {
                                "terms": {
                                    "field": "subject",
                                    "size": 20
                                }
                            }
                        }
                    }
                }
            }
            
            if class_id:
                body["query"] = {
                    "term": {"class_id": class_id}
                }
            
            result = await self.client.search(
                index=self.indices['teacher_assignments'],
                body=body
            )
            
            return AggregateResponse(
                success=True,
                message="Subjects per class retrieved successfully",
                data={"results": result['aggregations']['classes']['buckets']},
                count=len(result['aggregations']['classes']['buckets'])
            )
        except Exception as e:
            return AggregateResponse(
                success=False,
                message=f"Failed to get subjects per class: {str(e)}",
                errors=[str(e)]
            )
