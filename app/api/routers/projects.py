from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from sqlalchemy.orm import Session
from app.schemas.project import ProjectCreate, ProjectResponse,ProjectCheckRequest,ProjectCheckResponse, ProjectRequest
from app.services.project_service import get_project, create_project, process_uploaded_file,get_all_projects
from app.db.session import get_db
from typing import Union
from datetime import date,datetime
from typing import Optional
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from typing import List, Tuple
import os
import logging
import time
from app.services.converter import upload_sas_files
# Set up logging
log_file = "logs/upload.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    filename=log_file,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)

logger = logging.getLogger(__name__)

router = APIRouter()
@router.post("/check-project-number", response_model=ProjectCheckResponse)
def check_project_no(
    request: ProjectCheckRequest,
    db: Session = Depends(get_db)
):
    existing = get_project(db, ProjectNumber=request.ProjectNumber)
    return {
        "available": not existing,
        "message": "Project number available" if not existing 
                  else "Project number already exists"
    }
@router.get("/list-projects", response_model=List[ProjectResponse])
def list_projects(db: Session = Depends(get_db)):
    """
    Retrieve all projects from the database.
    
    Returns:
        List of project details in JSON format.
    """
    try:
        start_time = time.time()
        logger.debug(f"[DEBUG] Starting to retrieve all projects")

        # Query all projects
        projects = get_all_projects(db)

        if not projects:
            logger.warning("[WARNING] No projects found")
            return []

        # Convert to Pydantic model
        project_list = [ProjectResponse.from_orm(project) for project in projects]

        end_time = time.time()
        duration = end_time - start_time
        logger.debug(f"[DEBUG] Retrieved {len(projects)} projects in {duration:.2f} seconds")

        return project_list

    except Exception as e:
        logger.error(f"[ERROR] Failed to list projects: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error retrieving projects")

@router.post("/create", response_model=ProjectCreate)
def create_project_with_upload(
    ProjectNumber: str = Form(...),
    ProjectName: str = Form(...),
    CustName: str = Form(...),
    ProjectStatus: str = Form(...),
    DateCutDate: Optional[str] = Form(None),
    DateExtractionDate: Optional[str] = Form(None),
    IsDatasetUploaded: bool = Form(False),
    CreatedByEmail: Optional[str] = Form(None),
    DateCreated: Optional[str] = Form(None),
    DateModified: Optional[str] = Form(None),
    isActive: bool = Form(True),
    UploadedBy: Optional[str] = Form(None),
    ModifiedBy: Optional[str] = Form(None),
    IsDeleted: bool = Form(False),
    DeletedAt: Optional[str] = Form(None),
    DeletedBy: Optional[str] = Form(None),
    uploaded_file: Union[UploadFile, None] = File(None),
    db: Session = Depends(get_db)
):
    try:
        # Convert string dates to datetime objects
        date_cut_date = None
        if DateCutDate:
            date_cut_date = datetime.fromisoformat(DateCutDate)

        date_extraction_date = None
        if DateExtractionDate:
            date_extraction_date = datetime.fromisoformat(DateExtractionDate)

        date_created = None
        if DateCreated:
            date_created = datetime.fromisoformat(DateCreated)

        date_modified = None
        if DateModified:
            date_modified = datetime.fromisoformat(DateModified)

        deleted_at = None
        if DeletedAt:
            deleted_at = datetime.fromisoformat(DeletedAt)
        # Process file upload and Azure Blob Storage upload
        # Rest of your implementation
        if uploaded_file:
            is_uploaded = process_uploaded_file(ProjectNumber, uploaded_file)
        else:
            is_uploaded = 0
        
        
        # Create project in database
        project_data = ProjectCreate(
            CustName=CustName,
            ProjectNumber=ProjectNumber,
            ProjectName=ProjectName,
            DateCutDate=DateCutDate,
            DateExtractionDate=DateExtractionDate,
            ProjectStatus=ProjectStatus,
            IsDatasetUploaded=IsDatasetUploaded,
            CreatedByEmail=CreatedByEmail,
            DateCreated=DateCreated,
            DateModified=DateModified,
            isActive=isActive,
            UploadedBy=UploadedBy,
            ModifiedBy=ModifiedBy,
            IsDeleted=IsDeleted,
            DeletedAt=DeletedAt,
            DeletedBy=DeletedBy
        )
        
        db_project = create_project(db, project=project_data)
        db_project.is_uploaded = is_uploaded
        db.commit()
        db.refresh(db_project)
        
        return db_project
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    
# @router.post("/upload-sas/")
# def upload_sas(req: ProjectRequest):
#     return upload_sas_files(req)