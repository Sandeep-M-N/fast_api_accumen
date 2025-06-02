from pydantic import BaseModel, Field, field_validator,model_validator
from datetime import date, datetime
from typing import Optional
from typing import List, Tuple
import re
class ProjectBase(BaseModel):
    ProjectId: Optional[int] = None  # Make it optional
    CustName: str = Field(..., min_length=1, max_length=80, pattern=r'^[a-zA-Z0-9& -]+$')
    ProjectNumber: str = Field(..., min_length=1, max_length=80, pattern=r'^[a-zA-Z0-9\-_]+$')
    ProjectName: str = Field(..., min_length=1, max_length=80, pattern=r'^[a-zA-Z0-9\-_]+$')
    DateCutDate: Optional[datetime] = None  # Changed to date type
    DateExtractionDate: Optional[datetime] = None  # Changed to date type
    IsDatasetUploaded: int = Field(0, ge=0, le=1)
    ProjectStatus: str = Field(
        ..., 
        max_length=255,
        pattern=r'^[a-zA-Z0-9 _-]+$'  # Allow letters, numbers, spaces, underscores, hyphens
    )
    CreatedByEmail: Optional[str] = Field(
        None, 
        max_length=255, 
        pattern=r'^[\w\.-]+@[\w\.-]+\.\w+$'  # Email format
    )
    DateCreated: Optional[datetime] = None
    DateModified: Optional[datetime] = None
    isActive: bool = Field(default=True)
    UploadedBy: Optional[str] = Field(
        None, 
        max_length=255, 
        pattern=r'^[a-zA-Z0-9 _-]+$'
    )
    ModifiedBy: Optional[str] = Field(
        None, 
        max_length=255, 
        pattern=r'^[a-zA-Z0-9 _-]+$'
    )
    IsDeleted: bool = Field(default=False)
    DeletedAt: Optional[datetime] = None
    DeletedAt: Optional[str] = Field(
        None, 
        max_length=255, 
        pattern=r'^[a-zA-Z0-9 _-]+$'
    )

    # @model_validator(mode='after')
    # def validate_dates(self):
    #     if self.DateCreated and self.DateModified:
    #         if self.DateModified < self.DateCreated:
    #             raise ValueError("DateModified cannot be before DateCreated")
    #     return self

    # @field_validator('DateCutDate', 'DateExtractionDate', mode='before')
    # def parse_date(cls, v):
    #     if not v or v == "":
    #         return None
    #     if isinstance(v, date):
    #         return v
    #     try:
    #         return date.fromisoformat(v)  # Strict ISO format parsing
    #     except ValueError:
    #         raise ValueError("Date must be in YYYY-MM-DD format")
        

class ProjectCheckRequest(BaseModel):
    ProjectNumber: str = Field(..., min_length=1, max_length=80)

class ProjectCheckResponse(BaseModel):
    available: bool
    message: str

class ProjectCreate(ProjectBase):
    pass

class ProjectResponse(BaseModel):
    ProjectId: int
    ProjectNumber: str
    ProjectName: str
    CustName: str
    ProjectStatus: str
    DateCutDate: Optional[datetime] = None
    DateExtractionDate: Optional[datetime] = None
    IsDatasetUploaded: bool
    CreatedByEmail: Optional[str] = None
    DateCreated: Optional[datetime] = None
    DateModified: Optional[datetime] = None
    isActive: bool
    UploadedBy: Optional[str] = None
    ModifiedBy: Optional[str] = None
    IsDeleted: bool
    DeletedAt: Optional[datetime] = None
    DeletedBy: Optional[str] = None

    class Config:
        from_attributes = True

class ProjectRequest(BaseModel):
    project_name: str