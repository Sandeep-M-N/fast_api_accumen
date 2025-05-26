import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker
import os
import sys
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)

from main import app
from app.db.base import Base
from app.db.session import get_db

# Use your actual database but with a test schema
TEST_DB_URL = "postgresql://postgres:Database%40123@localhost:5432/Accumen?options=-c search_path=test"

engine = create_engine(TEST_DB_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@pytest.fixture(scope="module")
def test_schema():
    with engine.connect() as conn:
        # Use text() for raw SQL or proper SQLAlchemy DDL
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS test"))
        conn.commit()
    yield
    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS test CASCADE"))
        conn.commit()

@pytest.fixture(scope="function")
def db_session(test_schema):
    # Set the search path for this connection
    with engine.connect() as conn:
        conn.execute(text("SET search_path TO test"))
        conn.commit()
    
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.rollback()
        Base.metadata.drop_all(bind=engine)
        db.close()

@pytest.fixture(scope="function")
def client(db_session):
    def override_get_db():
        try:
            yield db_session
        finally:
            db_session.rollback()
    
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()