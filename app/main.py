# app/main.py

from fastapi import FastAPI
from app.api.v1 import router
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.include_router(router.router, prefix="/api/v1", tags=["project"])

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
