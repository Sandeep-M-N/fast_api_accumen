from pydantic_settings import BaseSettings
import os

class Settings(BaseSettings):
    DATABASE_URL: str
    AZURE_STORAGE_CONNECTION_STRING: str
    AZURE_STORAGE_CONTAINER_NAME: str
    SQL_SERVER:str
    DRIVER: str
    USE_WINDOWS_AUTH: bool
    USERNAME: str
    PASSWORD: str
    MAIN_DB_NAME: str
    DOWNLOAD_WORKERS: int
    PROCESSING_WORKERS: int
    MAX_DB_CONNECTIONS: int
    CHUNK_SIZE: int
    AZURE_DOWNLOAD_TIMEOUT: int
    BASE_BLOB_PATH: str

    
    class Config:
        env_file = ".env"

settings = Settings()