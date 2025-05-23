import pytest

@pytest.fixture(scope="module")
def any_fixture_needed():
    yield "mock data"
