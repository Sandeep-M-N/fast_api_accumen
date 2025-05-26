import pytest
from fastapi import status
from app.schemas.project import ProjectCheckRequest

# Test data
TEST_PROJECT = {
    "customer_name": "Test Customer",
    "project_no": "PRJ012",
    "study_no": "STD012"
}

def test_check_project_available(client):
    # Test with non-existing project
    response = client.post(
        "/api/v1/projects/check",
        json={"project_no": "NEWPROJECT"}
    )
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {
        "available": True,
        "message": "Project number available"
    }

def test_check_project_exists(client, db_session):
    # First create a project
    client.post("/api/v1/projects/", data=TEST_PROJECT)
    
    # Then check it exists
    response = client.post(
        "/api/v1/projects/check",
        json={"project_no": TEST_PROJECT["project_no"]}
    )
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {
        "available": False,
        "message": "Project number already exists"
    }

def test_create_project_success(client):
    response = client.post(
        "/api/v1/projects/",
        data=TEST_PROJECT
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["project_no"] == TEST_PROJECT["project_no"]
    assert data["is_uploaded"] == 0

def test_create_project_duplicate(client):
    # First create
    client.post("/api/v1/projects/", data=TEST_PROJECT)
    
    # Try to create again
    response = client.post(
        "/api/v1/projects/",
        data=TEST_PROJECT
    )
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "already exists" in response.json()["detail"]

def test_create_project_with_file(client, tmp_path):
    # Create a test file
    test_file = tmp_path / "test.sas7bdat"
    test_file.write_text("test content")
    
    with open(test_file, "rb") as f:
        response = client.post(
            "/api/v1/projects/",
            data=TEST_PROJECT,
            files={"uploaded_file": ("test.sas7bdat", f, "application/octet-stream")}
        )
    
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["is_uploaded"] == 1