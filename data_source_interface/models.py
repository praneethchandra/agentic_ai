"""
Pydantic models for the Data Source Interface application.
Defines the schema for Person, Student, Teacher, Class, and related entities.
"""

from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum


class SubjectEnum(str, Enum):
    MATHEMATICS = "mathematics"
    SCIENCE = "science"
    ENGLISH = "english"
    HISTORY = "history"
    GEOGRAPHY = "geography"
    PHYSICS = "physics"
    CHEMISTRY = "chemistry"
    BIOLOGY = "biology"
    COMPUTER_SCIENCE = "computer_science"
    ART = "art"
    MUSIC = "music"
    PHYSICAL_EDUCATION = "physical_education"


class GatheringTypeEnum(str, Enum):
    CLASS = "class"
    WORKSHOP = "workshop"
    SEMINAR = "seminar"
    CONFERENCE = "conference"


class Person(BaseModel):
    """Base model for a person."""
    id: Optional[str] = Field(None, description="Unique identifier")
    first_name: str = Field(..., description="First name of the person")
    last_name: str = Field(..., description="Last name of the person")
    email: str = Field(..., description="Email address")
    phone: Optional[str] = Field(None, description="Phone number")
    date_of_birth: Optional[datetime] = Field(None, description="Date of birth")
    address: Optional[str] = Field(None, description="Physical address")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    @validator('email')
    def validate_email(cls, v):
        if '@' not in v:
            raise ValueError('Invalid email format')
        return v.lower()
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class Student(Person):
    """Student model extending Person."""
    student_id: Optional[str] = Field(None, description="Student ID number")
    grade_level: Optional[int] = Field(None, ge=1, le=12, description="Grade level (1-12)")
    enrollment_date: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = Field(True, description="Whether student is currently active")
    guardian_contact: Optional[str] = Field(None, description="Guardian contact information")


class Teacher(Person):
    """Teacher model extending Person."""
    employee_id: Optional[str] = Field(None, description="Employee ID number")
    subjects: List[SubjectEnum] = Field(default_factory=list, description="Subjects taught")
    hire_date: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = Field(True, description="Whether teacher is currently active")
    department: Optional[str] = Field(None, description="Department")
    qualification: Optional[str] = Field(None, description="Educational qualification")


class Gathering(BaseModel):
    """Base model for gatherings (classes, workshops, etc.)."""
    id: Optional[str] = Field(None, description="Unique identifier")
    name: str = Field(..., description="Name of the gathering")
    description: Optional[str] = Field(None, description="Description")
    gathering_type: GatheringTypeEnum = Field(..., description="Type of gathering")
    capacity: Optional[int] = Field(None, ge=1, description="Maximum capacity")
    location: Optional[str] = Field(None, description="Location")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class Class(Gathering):
    """Class model extending Gathering."""
    class_code: Optional[str] = Field(None, description="Class code")
    grade_level: Optional[int] = Field(None, ge=1, le=12, description="Grade level")
    academic_year: str = Field(..., description="Academic year (e.g., 2024-2025)")
    semester: Optional[str] = Field(None, description="Semester")
    schedule: Optional[Dict[str, Any]] = Field(None, description="Class schedule")
    
    def __init__(self, **data):
        if 'gathering_type' not in data:
            data['gathering_type'] = GatheringTypeEnum.CLASS
        super().__init__(**data)


class ClassEnrollment(BaseModel):
    """Model for student enrollment in classes."""
    id: Optional[str] = Field(None, description="Unique identifier")
    student_id: str = Field(..., description="Student ID")
    class_id: str = Field(..., description="Class ID")
    enrollment_date: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = Field(True, description="Whether enrollment is active")


class TeacherAssignment(BaseModel):
    """Model for teacher assignment to classes and subjects."""
    id: Optional[str] = Field(None, description="Unique identifier")
    teacher_id: str = Field(..., description="Teacher ID")
    class_id: str = Field(..., description="Class ID")
    subject: SubjectEnum = Field(..., description="Subject taught")
    assignment_date: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = Field(True, description="Whether assignment is active")


class Score(BaseModel):
    """Model for student scores/grades."""
    id: Optional[str] = Field(None, description="Unique identifier")
    student_id: str = Field(..., description="Student ID")
    class_id: str = Field(..., description="Class ID")
    subject: SubjectEnum = Field(..., description="Subject")
    score: float = Field(..., ge=0, le=100, description="Score (0-100)")
    max_score: float = Field(100, ge=1, description="Maximum possible score")
    assessment_type: str = Field(..., description="Type of assessment (exam, quiz, assignment, etc.)")
    assessment_date: datetime = Field(default_factory=datetime.utcnow)
    teacher_id: Optional[str] = Field(None, description="Teacher who assigned the score")
    comments: Optional[str] = Field(None, description="Additional comments")


class BulkOperation(BaseModel):
    """Model for bulk operations."""
    operation_type: str = Field(..., description="Type of operation (create, update, delete)")
    entity_type: str = Field(..., description="Type of entity (student, teacher, class, etc.)")
    data: List[Dict[str, Any]] = Field(..., description="List of data for bulk operation")
    batch_size: int = Field(100, ge=1, le=1000, description="Batch size for processing")


class AggregateQuery(BaseModel):
    """Model for aggregate queries."""
    query_type: str = Field(..., description="Type of aggregate query")
    filters: Optional[Dict[str, Any]] = Field(None, description="Filters to apply")
    group_by: Optional[List[str]] = Field(None, description="Fields to group by")
    sort_by: Optional[str] = Field(None, description="Field to sort by")
    sort_order: str = Field("asc", description="Sort order (asc/desc)")
    limit: Optional[int] = Field(None, ge=1, description="Limit results")


# Response models
class PersonResponse(BaseModel):
    """Response model for person operations."""
    success: bool
    message: str
    data: Optional[Union[Person, Student, Teacher]] = None
    errors: Optional[List[str]] = None


class ClassResponse(BaseModel):
    """Response model for class operations."""
    success: bool
    message: str
    data: Optional[Class] = None
    errors: Optional[List[str]] = None


class BulkOperationResponse(BaseModel):
    """Response model for bulk operations."""
    success: bool
    message: str
    total_processed: int
    successful: int
    failed: int
    errors: Optional[List[str]] = None


class AggregateResponse(BaseModel):
    """Response model for aggregate queries."""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    count: Optional[int] = None
    errors: Optional[List[str]] = None
