from sqlalchemy import Column, String, Date, Integer,Boolean, DateTime
from app.db.base import Base
from datetime import datetime,timezone

class Project(Base):
    __tablename__ = "project"
    ProjectId = Column(Integer, primary_key=True, index=True)
    ProjectNumber = Column(String(80), unique=True, nullable=False)
    ProjectName = Column(String(80), nullable=False)
    CustName = Column(String(80), nullable=False)
    ProjectStatus = Column(String(255))
    DateCutDate = Column(DateTime, nullable=True)
    DateExtractionDate = Column(DateTime, nullable=True)
    IsDatasetUploaded = Column(Boolean, default=False)
    CreatedByEmail = Column(String(255), nullable=True)
    DateCreated = Column(DateTime, default=datetime.now(timezone.utc))
    DateModified = Column(DateTime, onupdate=datetime.now(timezone.utc))
    isActive = Column(Boolean, default=True)
    UploadedBy = Column(String(255), nullable=True)
    ModifiedBy = Column(String(255), nullable=True)
    IsDeleted = Column(Boolean, default=False)
    DeletedAt = Column(DateTime)
    DeletedBy = Column(String(255), nullable=True)
