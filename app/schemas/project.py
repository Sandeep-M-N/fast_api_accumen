from pydantic import BaseModel, Field, field_validator
from datetime import date, datetime
from typing import Optional
import re
class ProjectBase(BaseModel):
    customer_name: str = Field(..., min_length=1, max_length=80, pattern=r'^[a-zA-Z0-9& -]+$')
    project_no: str = Field(..., min_length=1, max_length=80, pattern=r'^[a-zA-Z0-9\-_]+$')
    study_no: str = Field(..., min_length=1, max_length=80, pattern=r'^[a-zA-Z0-9\-_]+$')
    date_cut_date: Optional[date] = None  # Changed to date type
    date_extraction_date: Optional[date] = None  # Changed to date type
    is_uploaded: int = Field(0, ge=0, le=1)

    @field_validator('date_cut_date', 'date_extraction_date', mode='before')
    def parse_date(cls, v):
        if not v or v == "":
            return None
        if isinstance(v, date):
            return v
        try:
            return date.fromisoformat(v)  # Strict ISO format parsing
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format")

class ProjectCreate(ProjectBase):
    pass

class ProjectResponse(ProjectBase):
    class Config:
        json_encoders = {
            date: lambda v: v.isoformat() if v else None  # Output as ISO format
        }