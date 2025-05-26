import pytest
from fastapi import status

TEST_PROJECT = {
    "customer_name": "Integration Test",
    "project_no": "PRJ014",
    "study_no": "STD012",
    "date_cut_date": "2023-12-31",
    "date_extraction_date": "2024-01-15"
}

def test_full_project_lifecycle(client):
    # 1. Check project doesn't exist
    check_response = client.post(
        "/api/v1/projects/check",
        json={"project_no": TEST_PROJECT["project_no"]}
    )
    assert check_response.status_code == status.HTTP_200_OK
    assert check_response.json()["available"] is True

    # 2. Create project
    create_response = client.post(
        "/api/v1/projects/",
        data=TEST_PROJECT
    )
    assert create_response.status_code == status.HTTP_200_OK
    created_project = create_response.json()
    assert created_project["project_no"] == TEST_PROJECT["project_no"]

    # 3. Verify project exists
    check_response = client.post(
        "/api/v1/projects/check",
        json={"project_no": TEST_PROJECT["project_no"]}
    )
    assert check_response.status_code == status.HTTP_200_OK
    assert check_response.json()["available"] is False

    # 4. Test duplicate prevention
    duplicate_response = client.post(
        "/api/v1/projects/",
        data=TEST_PROJECT
    )
    assert duplicate_response.status_code == status.HTTP_400_BAD_REQUEST