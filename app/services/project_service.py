from sqlalchemy.orm import Session
from app.models.user import Project
from app.schemas.project import ProjectCreate
from app.utils.azure_blob import upload_to_azure_blob
import os
import shutil
import zipfile
import tempfile
import re

def get_project(db: Session, project_no: str):
    return db.query(Project).filter(Project.project_no == project_no).first()

def create_project(db: Session, project: ProjectCreate):
    db_project = Project(**project.model_dump())
    db.add(db_project)
    db.commit()
    db.refresh(db_project)
    return db_project

def classify_sas_file(filename: str) -> str:
    filename = filename.lower()
    if re.match(r'^ad[a-z]{1,3}\.sas7bdat$', filename) or re.match(r'^ad[a-z]{1,3}\d+\.sas7bdat$', filename):
        return "ADAM"
    if filename.startswith('supp') and filename.endswith('.sas7bdat'):
        return "SDTM"
    if re.match(r'^[a-z]{2,3}\.sas7bdat$', filename) or re.match(r'^[a-z]{2,3}\d+\.sas7bdat$', filename):
        return "SDTM"
    return None

def process_uploaded_file(project_no: str, uploaded_file):
    processed_files = []
    is_uploaded = 0
    
    if uploaded_file:
        with tempfile.TemporaryDirectory() as tmpdirname:
            file_path = os.path.join(tmpdirname, uploaded_file.filename)
            
            with open(file_path, "wb") as f:
                shutil.copyfileobj(uploaded_file.file, f)
            
            if uploaded_file.filename.lower().endswith('.zip'):
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(tmpdirname)
                
                for root, _, files in os.walk(tmpdirname):
                    for name in files:
                        if name.lower().endswith('.sas7bdat'):
                            file_type = classify_sas_file(name)
                            if file_type:
                                src_path = os.path.join(root, name)
                                processed_files.append((file_type, src_path))
            
            elif uploaded_file.filename.lower().endswith('.sas7bdat'):
                file_type = classify_sas_file(uploaded_file.filename)
                if file_type:
                    processed_files.append((file_type, file_path))
            
            if processed_files:
                is_uploaded = 1
                project_dir = os.path.join(tempfile.gettempdir(), project_no)
                os.makedirs(project_dir, exist_ok=True)
                
                for file_type, src_path in processed_files:
                    dest_dir = os.path.join(project_dir, file_type)
                    os.makedirs(dest_dir, exist_ok=True)
                    dest_path = os.path.join(dest_dir, os.path.basename(src_path))
                    shutil.move(src_path, dest_path)
                    
                    # Upload to Azure Blob Storage
                    blob_path = os.path.join(project_no, file_type, os.path.basename(src_path))
                    upload_to_azure_blob(blob_path, dest_path)
                
                return is_uploaded
    
    return is_uploaded