from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str
    AZURE_STORAGE_CONNECTION_STRING: str
    AZURE_STORAGE_CONTAINER_NAME: str
    
    class Config:
        env_file = ".env"

settings = Settings()