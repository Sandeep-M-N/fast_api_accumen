import io
import pytest

def test_file_upload_integration(client, tmp_path):
    # Create a test SAS file
    test_file = tmp_path / "adtte1.sas7bdat"
    test_file.write_text("TEST CONTENT")
    
    test_data = {
        "customer_name": "File Test",
        "project_no": "FILE1",
        "study_no": "FSTUDY1"
    }

    with open(test_file, "rb") as f:
        response = client.post(
            "/api/v1/projects/",
            data=test_data,
            files={"uploaded_file": ("adtte1.sas7bdat", f, "application/octet-stream")}
        )
    
    assert response.status_code == 200
    result = response.json()
    assert result["is_uploaded"] == 1
    assert result["project_no"] == "FILE1"