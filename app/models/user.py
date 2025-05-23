from sqlalchemy import Column, String, Date, Integer
from app.db.base import Base

class Project(Base):
    __tablename__ = "Accumen project"
    
    project_no = Column(String(80), primary_key=True, index=True)
    customer_name = Column(String(80), nullable=False)
    study_no = Column(String(80), nullable=False)
    date_cut_date = Column(Date, nullable=True)
    date_extraction_date = Column(Date, nullable=True)
    is_uploaded = Column(Integer, default=0)