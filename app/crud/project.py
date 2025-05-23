# app/crud/project.py

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.user import Project
from app.schemas.project import ProjectBase

def check_project_exists(db: AsyncSession, project_no: str) -> bool:
    stmt = select(Project).where(Project.project_no == project_no)
    result =  db.execute(stmt)
    return result.scalars().first() is not None

def create_project(db: AsyncSession, project: ProjectBase):
    db_project = Project(**project.model_dump())
    db.add(db_project)
    db.commit()
    db.refresh(db_project)
    return db_project
