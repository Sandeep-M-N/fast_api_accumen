from azure.storage.blob import BlobServiceClient, BlobClient, BlobBlock
import os
import uuid
import logging
import time
from app.core.config import settings
from concurrent.futures import ThreadPoolExecutor

# Set up logging to file
log_file = "logs/upload.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create a file handler
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.DEBUG)

# Create a formatter and set it for the handler
formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
file_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(file_handler)

def upload_to_azure_blob(blob_path: str, local_path: str) -> bool:
    """
    Uploads a local file to Azure Blob Storage.
    
    Args:
        blob_path (str): The path in Azure Blob Storage where the file will be uploaded.
        local_path (str): The local file path to upload.
        
    Returns:
        bool: True if upload is successful, False otherwise.
    """
    try:
        # Log start of upload
        logger.debug(f"[DEBUG] Starting upload of {local_path} to {blob_path}")
        start_time = time.time()

        # Check if the file exists and is readable
        if not os.path.exists(local_path):
            logger.error(f"[ERROR] File not found: {local_path}")
            return False

        if not os.path.isfile(local_path):
            logger.error(f"[ERROR] Not a file: {local_path}")
            return False

        # Get connection string from environment
        conn_str = settings.AZURE_STORAGE_CONNECTION_STRING
        if not conn_str:
            logger.error("[ERROR] AZURE_STORAGE_CONNECTION_STRING is not set.")
            return False

        # Create a BlobServiceClient object using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(
            conn_str,
            retry_total=5,
            retry_backoff_factor=0.8,
            timeout=600  # 10 minutes
        )

        container_name = settings.AZURE_STORAGE_CONTAINER_NAME
        if not container_name:
            logger.error("[ERROR] AZURE_STORAGE_CONTAINER_NAME is not set.")
            return False

        container_client = blob_service_client.get_container_client(container_name)

        # Create container if it doesn't exist
        if not container_client.exists():
            container_client.create_container()
            logger.info(f"[INFO] Created container: {container_name}")

        # Get a reference to the blob
        blob_client = container_client.get_blob_client(blob_path)

        # Define chunk size (4MB)
        chunk_size = 1024 * 1024 * 4  # 4 MB per chunk
        block_list = []

        # Open the local file
        with open(local_path, "rb") as f:
            while True:
                read_data = f.read(chunk_size)
                if not read_data:
                    break  # No more data to read

                # Generate a unique block ID
                block_id = str(uuid.uuid4())

                # Stage the block
                blob_client.stage_block(block_id=block_id, data=read_data)

                # Add the block ID to the list
                block_list.append(BlobBlock(block_id=block_id))

        # Commit all blocks
        blob_client.commit_block_list(block_list)

        # Log end of upload
        end_time = time.time()
        duration = end_time - start_time
        logger.debug(f"[DEBUG] Successfully uploaded file '{local_path}' to '{blob_path}' in {duration:.2f} seconds")
        logger.debug(f"[DEBUG] Start time: {start_time}, End time: {end_time}, Duration: {duration:.2f} seconds")
        return True

    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time if 'start_time' in locals() else 0
        logger.error(f"[ERROR] Failed to upload {blob_path}: {str(e)}", exc_info=True)
        logger.debug(f"[DEBUG] Start time: {start_time}, End time: {end_time}, Duration: {duration:.2f} seconds")
        return False


def upload_files_in_parallel(files_to_upload: list[tuple[str, str]]) -> tuple[int, int]:
    """
    Upload multiple files in parallel using thread pool.

    Args:
        files_to_upload (list[tuple[str, str]): List of (blob_path, local_path) tuples.

    Returns:
        tuple[int, int]: (success_count, failed_count)
    """
    success_count = 0
    failed_count = 0

    def _upload_file(blob_path: str, local_path: str):
        nonlocal success_count, failed_count
        try:
            if upload_to_azure_blob(blob_path, local_path):
                success_count += 1
                logger.debug(f"[DEBUG] Uploaded {local_path} to {blob_path}")
            else:
                failed_count += 1
                logger.error(f"[ERROR] Failed to upload {local_path} to {blob_path}")
        except Exception as e:
            failed_count += 1
            logger.error(f"[ERROR] Exception during upload of {local_path}: {str(e)}")

    with ThreadPoolExecutor(max_workers=10) as executor:
        for blob_path, local_path in files_to_upload:
            executor.submit(_upload_file, blob_path, local_path)

    return success_count, failed_count