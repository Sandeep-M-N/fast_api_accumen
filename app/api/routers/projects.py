from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form,status
from sqlalchemy.orm import Session
from app.schemas.project import ProjectCreate, ProjectResponse,ProjectCheckRequest,ProjectCheckResponse, ProjectRequest
from app.services.project_service import get_project, create_project, process_uploaded_file,get_all_projects
from app.db.session import get_db
from typing import Union
from datetime import date,datetime
from typing import Optional
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
@router.post(
    "/check-project-number",
    response_model=ProjectCheckResponse,
    responses={
        200: {"description": "Project number is available"},
        400: {"description": "Project number already exists"}
    }
)
def check_project_no(
    request: ProjectCheckRequest,
    db: Session = Depends(get_db)
):
    existing = get_project(db, ProjectNumber=request.ProjectNumber)

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "available": False,
                "message": "Project number already exists"
            }
        )
    else:
        return {
            "available": True,
            "message": "Project number is available"
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
    uploaded_files: List[UploadFile] = File(default=None),
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
        if uploaded_files:
            IsDatasetUploaded = process_uploaded_file(ProjectNumber, uploaded_files)
        else:
            IsDatasetUploaded = False
        
        
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
        db_project.IsDatasetUploaded = IsDatasetUploaded
        db.commit()
        db.refresh(db_project)
        
        return db_project
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    
@router.put("/edit/{ProjectNumber}", response_model=ProjectResponse)
def edit_project_by_number(
    ProjectNumber: str,
    ProjectName: str = Form(None),
    CustName: str = Form(None),
    ProjectStatus: str = Form(None),
    DateCutDate: Optional[str] = Form(None),
    DateExtractionDate: Optional[str] = Form(None),
    IsDatasetUploaded: bool = Form(None),
    CreatedByEmail: Optional[str] = Form(None),
    DateCreated: Optional[str] = Form(None),
    DateModified: Optional[str] = Form(None),
    isActive: bool = Form(None),
    UploadedBy: Optional[str] = Form(None),
    ModifiedBy: Optional[str] = Form(None),
    IsDeleted: bool = Form(None),
    DeletedAt: Optional[str] = Form(None),
    DeletedBy: Optional[str] = Form(None),
    uploaded_files: List[UploadFile] = File(default=None),
    db: Session = Depends(get_db)
):
    try:
        # Find the project by ProjectNumber
        project = get_project(db, ProjectNumber)
        if not project:
            raise HTTPException(status_code=404, detail=f"Project with number {ProjectNumber} not found.")

        # Update only the fields that are provided
        if ProjectName is not None:
            project.ProjectName = ProjectName
        if CustName is not None:
            project.CustName = CustName
        if ProjectStatus is not None:
            project.ProjectStatus = ProjectStatus
        if DateCutDate is not None:
            project.DateCutDate = datetime.fromisoformat(DateCutDate)
        if DateExtractionDate is not None:
            project.DateExtractionDate = datetime.fromisoformat(DateExtractionDate)
        if IsDatasetUploaded is not None:
            project.IsDatasetUploaded = IsDatasetUploaded
        if CreatedByEmail is not None:
            project.CreatedByEmail = CreatedByEmail
        if DateCreated is not None:
            project.DateCreated = datetime.fromisoformat(DateCreated)
        if DateModified is not None:
            project.DateModified = datetime.fromisoformat(DateModified)
        if isActive is not None:
            project.isActive = isActive
        if UploadedBy is not None:
            project.UploadedBy = UploadedBy
        if ModifiedBy is not None:
            project.ModifiedBy = ModifiedBy
        if IsDeleted is not None:
            project.IsDeleted = IsDeleted
        if DeletedAt is not None:
            project.DeletedAt = datetime.fromisoformat(DeletedAt)
        if DeletedBy is not None:
            project.DeletedBy = DeletedBy

        # Handle file upload and update IsDatasetUploaded
        if uploaded_files:
            project.IsDatasetUploaded = process_uploaded_file(ProjectNumber, uploaded_files)

        db.commit()
        db.refresh(project)

        return ProjectResponse.from_orm(project)

    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    
# @router.post("/upload-sas/")
# def upload_sas(req: ProjectRequest):
#     return upload_sas_files(req)