from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from sqlalchemy.orm import Session
from app.schemas.project import ProjectCreate, ProjectResponse,ProjectCheckRequest,ProjectCheckResponse
from app.services.project_service import get_project, create_project, process_uploaded_file
from app.db.session import get_db
from typing import Union
from datetime import date
from typing import Optional

router = APIRouter()
@router.post("/check-project-number", response_model=ProjectCheckResponse)
def check_project_no(
    request: ProjectCheckRequest,
    db: Session = Depends(get_db)
):
    existing = get_project(db, project_no=request.project_no)
    return {
        "available": not existing,
        "message": "Project number available" if not existing 
                  else "Project number already exists"
    }

@router.post("/create", response_model=ProjectResponse)
def create_project_with_upload(
    customer_name: str = Form(...),
    project_no: str = Form(...),
    study_no: str = Form(...),
    date_cut_date: Optional[date] = Form(None),
    date_extraction_date: Optional[date] = Form(None),
    uploaded_file: Union[UploadFile,None] = File(None),  # Explicit Union type
    db: Session = Depends(get_db)
):
    try:
        # Process file upload and Azure Blob Storage upload
        # Rest of your implementation
        if uploaded_file:
            is_uploaded = process_uploaded_file(project_no, uploaded_file)
        else:
            is_uploaded = 0
        
        
        # Create project in database
        project_data = ProjectCreate(
            customer_name=customer_name,
            project_no=project_no,
            study_no=study_no,
            date_cut_date=date_cut_date,
            date_extraction_date=date_extraction_date
        )
        
        db_project = create_project(db, project=project_data)
        db_project.is_uploaded = is_uploaded
        db.commit()
        db.refresh(db_project)
        
        return db_project
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))