import pytest
from httpx import AsyncClient
from app.main import app
from fastapi.testclient import TestClient


@pytest.mark.asyncio
async def test_check_project_not_exists():
    async with AsyncClient(base_url="http://test",app=app) as ac:
        response = await ac.post("/api/v1/project/check", json={"project_no": "UNIQUE001"})
    assert response.status_code == 200
    assert response.json()["detail"] == "Project number is available"