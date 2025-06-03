"""
Pytest configuration and shared fixtures for the Generic Data Management API tests.
"""

import pytest
import os
import sys
from pathlib import Path
from fastapi.testclient import TestClient

# Add the src directory to the Python path
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

from src.main import app


@pytest.fixture(scope="session")
def test_client():
    """
    Create a test client for the FastAPI application.
    Session-scoped to avoid recreating for each test.
    """
    with TestClient(app) as client:
        yield client


@pytest.fixture(scope="session")
def api_base_url():
    """Base URL for API endpoints."""
    return "http://testserver"


@pytest.fixture(scope="session")
def api_endpoints():
    """Common API endpoints used in tests."""
    return {
        "health": "/health",
        "root": "/",
        "datasets": "/api/v1/datasets",
        "wells_production_import": "/api/v1/wells_production/import",
        "wells_production_export": "/api/v1/wells_production/export", 
        "wells_production_stats": "/api/v1/wells_production/stats",
        "wells_production_list": "/api/v1/wells_production/"
    }


@pytest.fixture
def sample_well_code():
    """Sample well code for testing."""
    return 59806


@pytest.fixture
def sample_field_code():
    """Sample field code for testing."""
    return 8908


@pytest.fixture
def expected_well_reference():
    """Expected well reference for test validation."""
    return "1-C-1-BA"


@pytest.fixture
def expected_field_name():
    """Expected field name for test validation."""
    return "Candeias"


@pytest.fixture
def sample_dataset_name():
    """Sample dataset name for testing."""
    return "wells_production"


class TestAssertions:
    """Helper class for common test assertions."""
    
    @staticmethod
    def assert_successful_response(response, expected_status=200):
        """Assert that a response is successful."""
        assert response.status_code == expected_status, f"Expected {expected_status}, got {response.status_code}: {response.text}"
        return response.json()
    
    @staticmethod
    def assert_error_response(response, expected_status):
        """Assert that a response contains an expected error."""
        assert response.status_code == expected_status, f"Expected {expected_status}, got {response.status_code}: {response.text}"
    
    @staticmethod
    def assert_json_structure(data, required_keys):
        """Assert that JSON data contains required keys."""
        for key in required_keys:
            assert key in data, f"Missing required key: {key}"


@pytest.fixture
def test_assertions():
    """Provide test assertion helpers."""
    return TestAssertions 