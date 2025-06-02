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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Set up logging to file
log_file = "logs/upload.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    filename=log_file,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)

logger = logging.getLogger(__name__)

def get_project(db: Session, ProjectNumber: str):
    return db.query(Project).filter(Project.ProjectNumber == ProjectNumber).first()
def get_all_projects(db: Session):
    """Get all projects from the database."""
    return db.query(Project).all()

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

def process_uploaded_file(ProjectNo: str, uploaded_file: UploadFile) -> int:
    """
    Process an uploaded file (ZIP or single SAS file) and upload to Azure Blob Storage.
    Returns 1 if at least one file was processed and uploaded, else 0.
    """
    is_uploaded = 0
    processed_files = []

    try:
        # Start time for full process
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
        #     if uploaded_file.filename.lower().endswith('.zip'):
        #         zip_upload_start = time.time()
        #         sanitized_name = sanitize_filename(uploaded_file.filename)
        #         blob_raw_path = f"raw/{project_no}/{sanitized_name}"

        #         if upload_to_azure_blob(blob_raw_path, file_path):
        #             zip_upload_end = time.time()
        #             zip_duration = zip_upload_end - zip_upload_start
        #             logger.debug(f"[DEBUG] ZIP uploaded to Azure in {zip_duration:.2f} seconds")
        #             is_uploaded = 1
        # else:
        #     logger.error(f"[ERROR] Failed to upload ZIP file: {file_path}")
        #     raise Exception("Failed to upload ZIP file")

        # # Track time for extracting ZIP
        # extract_start = time.time()
        # if uploaded_file.filename.lower().endswith('.zip'):
        #     with zipfile.ZipFile(file_path, 'r') as zip_ref:
        #         zip_ref.extractall(tmpdirname)
        #     extract_end = time.time()
        #     extract_duration = extract_end - extract_start
        #     logger.debug(f"[DEBUG] ZIP extraction completed in {extract_duration:.2f} seconds")

        #     # Upload all extracted files to application/<project_no>/Extracted/
        #     # extracted_files = []
        #     for root, _, files in os.walk(tmpdirname):
        #         for name in files:
        #             if not name.lower().endswith('.zip'):
        #                 extracted_upload_start=time.time()
        #                 src_path = os.path.join(root, name)
        #                 relative_path = os.path.relpath(src_path, tmpdirname)
        #                 blob_app_path = f"application/{project_no}/Extracted/{relative_path}"
        #                 # extracted_files.append(relative_path)
        #                 if upload_to_azure_blob(blob_app_path, src_path):
        #                 # logger.debug(f"[DEBUG] Uploaded file: {src_path} to {blob_app_path}")
        #                     extracted_upload_end = time.time()
        #                     extracted_duration = extracted_upload_end - extracted_upload_start
        #                     logger.debug(f"[DEBUG] extracted uploaded to Azure in {extracted_duration:.2f} seconds")


            # if extracted_files:
            #     logger.debug(f"[DEBUG] Found {len(extracted_files)} extracted files")
            #     extracted_files_to_upload = []
            #     for src_path in extracted_files:
            #         sanitized_name = sanitize_filename(os.path.basename(src_path))
            #         extracted_blob_path = f"application/{project_no}/Extracted/{sanitized_name}"

            #         extracted_files_to_upload.append((extracted_blob_path, src_path))

            #     success_count, failed_count = upload_files_in_parallel(extracted_files_to_upload)
            #     logger.debug(f"[DEBUG] Uploaded {success_count} extracted files to Extracted folder")

        # elif uploaded_file.filename.lower().endswith('.sas7bdat'):
        #     # If it's not a ZIP, just classify and upload directly
        #     file_type = classify_sas_file(uploaded_file.filename)
        #     if file_type:
        #         sanitized_name = sanitize_filename(uploaded_file.filename)
        #         blob_app_path = f"application/{project_no}/{sanitized_name}"
        #         if upload_to_azure_blob(blob_app_path, file_path):
        #             logger.debug(f"[DEBUG] Uploaded non-ZIP file to raw folder: {blob_app_path}")
        #             is_uploaded = 1

        # Classify and move files from Extracted to ADAM/SDTM
        # if uploaded_file.filename.lower().endswith('.zip'):
        #     # Re-upload the files to ADAM/SDTM after classification
        #     for root, _, files in os.walk(tmpdirname):
        #         for name in files:
        #             if name.lower().endswith('.sas7bdat'):
        #                 file_type = classify_sas_file(name)
        #                 if file_type:
        #                     src_path = os.path.join(root, name)
        #                     processed_files.append((file_type, src_path))

        #     if processed_files:
        #         is_uploaded = 1
        #         files_to_upload = []

        #         for file_type, src_path in processed_files:
        #             sanitized_name = sanitize_filename(os.path.basename(src_path))
        #             final_blob_path = f"application/{project_no}/{file_type}/{sanitized_name}"
        #             files_to_upload.append((final_blob_path, src_path))

        #         success_count, failed_count = upload_files_in_parallel(files_to_upload)
        #         logger.debug(f"[DEBUG] Parallel upload completed. Success: {success_count}, Failed: {failed_count}")

        #         for i, (blob_path, local_path) in enumerate(files_to_upload):
        #             if i < success_count:
        #                 os.remove(local_path)
        #                 logger.debug(f"[DEBUG] Deleted local file: {local_path}")

        # # Total time taken
        # total_end = time.time()
        # total_duration = total_end - start_time_total
        # logger.debug(f"[DEBUG] Total upload duration: {total_duration:.2f} seconds")

        # Clean up temp directory
        shutil.rmtree(tmpdirname)

        return is_uploaded

    except Exception as e:
        logger.error(f"Error processing file: {str(e)}", exc_info=True)
        raise