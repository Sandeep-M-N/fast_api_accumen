import os
import shutil
import zipfile
import re
import tempfile
from typing import List, Tuple, Optional
from sqlalchemy.orm import Session
from app.models.user import Project
from app.schemas.project import ProjectCreate
from app.utils.azure_blob import upload_to_azure_blob, upload_files_in_parallel
from app.core.config import settings
from fastapi import UploadFile
import logging
import time

# Set up logging to file
log_file = "logs/upload.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    filename=log_file,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)

logger = logging.getLogger(__name__)

def get_project(db: Session, project_no: str):
    return db.query(Project).filter(Project.project_no == project_no).first()

def create_project(db: Session, project: ProjectCreate):
    db_project = Project(**project.model_dump())
    db.add(db_project)
    db.commit()
    db.refresh(db_project)
    return db_project

def sanitize_filename(filename: str) -> str:
    """Sanitize filename to prevent path traversal and invalid characters."""
    return re.sub(r'[^\w\.\-]', '_', filename)

def classify_sas_file(filename: str) -> str:
    """Classify SAS files as ADAM or SDTM"""
    filename = filename.lower()
    if re.match(r'^ad[a-z]{1,3}\.sas7bdat$', filename) or re.match(r'^ad[a-z]{1,3}\d+\.sas7bdat$', filename):
        return "ADAM"
    if filename.startswith('supp') and filename.endswith('.sas7bdat'):
        return "SDTM"
    if re.match(r'^[a-z]{2,3}\.sas7bdat$', filename) or re.match(r'^[a-z]{2,3}\d+\.sas7bdat$', filename):
        return "SDTM"
    return None

def process_uploaded_file(project_no: str, uploaded_file: UploadFile) -> int:
    """
    Process an uploaded file (ZIP or single SAS file) and upload to Azure Blob Storage.
    Returns 1 if at least one file was processed and uploaded, else 0.
    """
    is_uploaded = 0
    processed_files = []

    try:
        start_time_total = time.time()
        logger.debug(f"[DEBUG] Start time for full process: {start_time_total}")

        if uploaded_file:
            # Create a fixed temp directory (not deleted automatically)
            tmpdirname = tempfile.mkdtemp()
            file_path = os.path.join(tmpdirname, uploaded_file.filename)

            # Save the uploaded file to the temp directory
            with open(file_path, "wb") as f:
                shutil.copyfileobj(uploaded_file.file, f)
            logger.debug(f"[DEBUG] File saved to: {file_path}")

            # Track time for uploading ZIP to Azure
            zip_upload_start = time.time()
            sanitized_name = sanitize_filename(uploaded_file.filename)
            blob_raw_path = f"raw/{project_no}/{sanitized_name}"
            if upload_to_azure_blob(blob_raw_path, file_path):
                zip_upload_end = time.time()
                zip_duration = zip_upload_end - zip_upload_start
                logger.debug(f"[DEBUG] ZIP upload completed in {zip_duration:.2f} seconds")
                is_uploaded=1
            else:
                logger.error(f"[ERROR] Failed to upload ZIP file: {file_path}")
                raise Exception("Failed to upload ZIP file")

            # Track time for extracting ZIP
            extract_start = time.time()
            if uploaded_file.filename.lower().endswith('.zip'):
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(tmpdirname)
                extract_end = time.time()
                extract_duration = extract_end - extract_start
                logger.debug(f"[DEBUG] ZIP extraction completed in {extract_duration:.2f} seconds")
            elif uploaded_file.filename.lower().endswith('.sas7bdat'):
                pass  # No extraction needed

            # Walk through extracted files
            for root, _, files in os.walk(tmpdirname):
                for name in files:
                    if name.lower().endswith('.sas7bdat'):
                        file_type = classify_sas_file(name)
                        if file_type:
                            src_path = os.path.join(root, name)
                            processed_files.append((file_type, src_path))
                            logger.debug(f"[DEBUG] Found SAS file: {name}, Type: {file_type}, Path: {src_path}")

            # Track time for uploading SAS files
            sas_upload_start = time.time()
            if processed_files:
                files_to_upload = []
                for file_type, src_path in processed_files:
                    sanitized_name = sanitize_filename(os.path.basename(src_path))
                    blob_app_path = f"application/{project_no}/{file_type}/{sanitized_name}"
                    files_to_upload.append((blob_app_path, src_path))

                success_count, failed_count = upload_files_in_parallel(files_to_upload)
                sas_upload_end = time.time()
                sas_duration = sas_upload_end - sas_upload_start
                logger.debug(f"[DEBUG] SAS file upload completed in {sas_duration:.2f} seconds")
                is_uploaded=1

                for i, (blob_path, local_path) in enumerate(files_to_upload):
                    if i < success_count:
                        os.remove(local_path)
                        logger.debug(f"[DEBUG] Deleted local file: {local_path}")

            # Clean up temp directory
            shutil.rmtree(tmpdirname)

            # Total time taken
            total_end = time.time()
            total_duration = total_end - start_time_total
            logger.debug(f"[DEBUG] Total upload duration: {total_duration:.2f} seconds")

        return is_uploaded

    except Exception as e:
        logger.error(f"Error processing file: {str(e)}", exc_info=True)
        raise