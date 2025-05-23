import os
from azure.storage.blob import BlobServiceClient
from app.core.config import settings

def upload_to_azure_blob(blob_path: str, local_path: str):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            settings.AZURE_STORAGE_CONNECTION_STRING
        )
        blob_client = blob_service_client.get_blob_client(
            container=settings.AZURE_STORAGE_CONTAINER_NAME,
            blob=blob_path
        )
        
        with open(local_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        return True
    except Exception as e:
        print(f"Error uploading to Azure Blob Storage: {str(e)}")
        return False