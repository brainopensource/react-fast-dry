"""
Tests for well production endpoints.
"""

import pytest
from unittest.mock import AsyncMock
from typing import Optional # Added for Optional type hint

from fastapi import status
# Assuming TestClient is globally available or via a fixture like test_client
# from fastapi.testclient import TestClient

# This import is crucial for app.dependency_overrides
from src.main import app

from src.shared.exceptions import ApplicationException, ValidationException, ErrorCode
from src.application.services.well_production_service import WellProductionService

# Helper function for asserting error response structure
def assert_error_response_structure(response_json: dict, expected_error_code: ErrorCode, expected_message: Optional[str] = None):
    assert response_json["success"] is False
    assert "error" in response_json
    error_detail = response_json["error"]
    assert error_detail["error_code"] == expected_error_code.value
    if expected_message:
        assert expected_message in error_detail["message"]
    else:
        assert "message" in error_detail # Just ensure message key exists
    assert "context" in error_detail # Ensure context key exists, even if empty
    assert "metadata" in response_json
    assert "timestamp" in response_json["metadata"]


class TestWellEndpoints:
    """Test class for well production endpoints."""

    def test_import_wells_endpoint(self, test_client, api_endpoints, test_assertions):
        """Test the wells import endpoint."""
        # Act
        response = test_client.post(api_endpoints["import"])
        
        # Assert - Import should return 201 Created
        data = test_assertions.assert_successful_response(response, 201)
        
        # Verify response structure - actual API returns success, data, storage
        test_assertions.assert_json_structure(data, ["success", "data", "storage"])
        
        # Verify success flag
        assert data["success"] is True
        
        # Verify data structure
        data_section = data.get("data", {})
        assert "imported_count" in data_section
        assert "total_records" in data_section
        assert "message" in data_section
        
        # Verify data types
        assert isinstance(data_section["imported_count"], int)
        assert isinstance(data_section["total_records"], int)
        assert data_section["imported_count"] >= 0
        assert data_section["total_records"] >= 0
        
        # Verify storage info
        storage_section = data.get("storage", {})
        assert "duckdb" in storage_section
        assert "csv" in storage_section

    def test_get_stats_endpoint(self, test_client, api_endpoints, test_assertions):
        """Test the wells statistics endpoint."""
        # Act
        response = test_client.get(api_endpoints["stats"])
        
        # Assert
        data = test_assertions.assert_successful_response(response, 200)
        
        # Verify response structure
        test_assertions.assert_json_structure(data, ["total_records", "storage_info"])
        
        # Verify storage info structure
        storage_info = data.get("storage_info", {})
        assert "primary" in storage_info
        assert "secondary" in storage_info
        
        # Verify data types
        assert isinstance(data["total_records"], int)
        assert data["total_records"] >= 0

    def test_get_well_by_code(self, test_client, api_endpoints, sample_well_code, test_assertions):
        """Test getting a specific well by code."""
        # Act
        response = test_client.get(f"{api_endpoints['well']}/{sample_well_code}")
        
        # Assert - Could be 200 (found) or 404 (not found, if data not imported)
        if response.status_code == 200:
            data = test_assertions.assert_successful_response(response, 200)
            
            # Verify response structure for successful retrieval
            expected_keys = ["well_name", "field_name"]
            for key in expected_keys:
                assert key in data, f"Missing key: {key}"
        
        elif response.status_code == 404:
            # Well not found - acceptable if data hasn't been imported
            test_assertions.assert_error_response(response, 404)
        
        else:
            pytest.fail(f"Unexpected status code: {response.status_code}")

    def test_get_well_with_expected_reference(self, test_client, api_endpoints, sample_well_code, expected_well_reference):
        """Test that a specific well returns the expected reference."""
        # Act
        response = test_client.get(f"{api_endpoints['well']}/{sample_well_code}")
        
        # Skip test if well not found (data not imported)
        if response.status_code == 404:
            pytest.skip("Well data not available - import data first")
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        
        # Check for well reference in various possible locations
        well_reference = (
            data.get("metadata", {}).get("well_reference") or 
            data.get("well_reference") or 
            ""
        )
        
        if well_reference:
            assert well_reference == expected_well_reference, f"Expected well reference '{expected_well_reference}', got '{well_reference}'"

    def test_get_field_by_code(self, test_client, api_endpoints, sample_field_code, test_assertions):
        """Test getting wells by field code."""
        # Act
        response = test_client.get(f"{api_endpoints['field']}/{sample_field_code}")
        
        # Assert - Could be 200 (found) or 404 (not found, if data not imported)
        if response.status_code == 200:
            data = test_assertions.assert_successful_response(response, 200)
            
            # Verify response structure for successful retrieval
            expected_keys = ["field_name", "wells_count"]
            for key in expected_keys:
                assert key in data, f"Missing key: {key}"
            
            # Verify data types
            assert isinstance(data["wells_count"], int)
            assert data["wells_count"] >= 0
        
        elif response.status_code == 404:
            # Field not found - acceptable if data hasn't been imported
            test_assertions.assert_error_response(response, 404)
        
        else:
            pytest.fail(f"Unexpected status code: {response.status_code}")

    def test_get_field_with_expected_name(self, test_client, api_endpoints, sample_field_code, expected_field_name):
        """Test that a specific field returns the expected name."""
        # Act
        response = test_client.get(f"{api_endpoints['field']}/{sample_field_code}")
        
        # Skip test if field not found (data not imported)
        if response.status_code == 404:
            pytest.skip("Field data not available - import data first")
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        
        field_name = data.get("field_name", "")
        assert field_name == expected_field_name, f"Expected field name '{expected_field_name}', got '{field_name}'"

    def test_download_csv_endpoint(self, test_client, api_endpoints):
        """Test the CSV download endpoint."""
        # Act
        response = test_client.get(api_endpoints["download"])
        
        # Assert - Could be 200 (data available) or 404 (no data)
        if response.status_code == 200:
            # Verify content type for CSV
            content_type = response.headers.get("content-type", "")
            assert "csv" in content_type.lower() or "text" in content_type.lower()
            
            # Verify content exists
            assert len(response.content) > 0
            
        elif response.status_code == 404:
            # No data available - acceptable
            pass
        
        else:
            pytest.fail(f"Unexpected status code for download: {response.status_code}")

    # --- New tests for standardized error handling ---

    def test_get_well_by_code_not_found(
        self, test_client, api_endpoints, sample_well_code # Assuming test_client and sample_well_code are fixtures
    ):
        """Test GET /well/{well_code} with ValidationException (404) for well not found."""
        # sample_well_code is used from fixture, e.g. 12345
        # api_endpoints is used from fixture

        mock_service_instance = AsyncMock(spec=WellProductionService)
        error_message = f"Well with code {sample_well_code} not found"

        # Configure the mock service method to raise the specific exception
        mock_service_instance.get_production_by_well.side_effect = ValidationException(
            message=error_message,
            field="well_code",
            value=sample_well_code, # The value that was not found
            status_code_override=status.HTTP_404_NOT_FOUND
        )

        # Override the dependency for this test
        # Ensure 'app' is the actual FastAPI application instance
        original_override = app.dependency_overrides.get(WellProductionService)
        app.dependency_overrides[WellProductionService] = lambda: mock_service_instance

        response = test_client.get(f"{api_endpoints['well']}/{sample_well_code}")

        try:
            assert response.status_code == status.HTTP_404_NOT_FOUND
            response_json = response.json()
            assert_error_response_structure(response_json, ErrorCode.NOT_FOUND_ERROR, error_message)
        finally:
            # Restore original dependency override or remove the current one
            if original_override:
                app.dependency_overrides[WellProductionService] = original_override
            else:
                app.dependency_overrides.pop(WellProductionService, None)

    def test_get_well_by_code_validation_error_invalid_date(
        self, test_client, api_endpoints, sample_well_code # Assuming fixtures are available
    ):
        """Test GET /well/{well_code} with ValidationException (400) for invalid date format."""
        mock_service_instance = AsyncMock(spec=WellProductionService)
        error_message = "Invalid period_start format. Use ISO format (YYYY-MM-DD)"

        # This exception should default to HTTP 400 Bad Request
        mock_service_instance.get_production_by_well.side_effect = ValidationException(
            message=error_message,
            field="period_start",
            value="invalid-date-format"
            # No status_code_override, so it uses ValidationException's default (400)
        )

        original_override = app.dependency_overrides.get(WellProductionService)
        app.dependency_overrides[WellProductionService] = lambda: mock_service_instance

        # Make a request that would trigger this validation, e.g., by passing an invalid date query param
        response = test_client.get(f"{api_endpoints['well']}/{sample_well_code}?period_start=invalid-date-format")

        try:
            assert response.status_code == status.HTTP_400_BAD_REQUEST
            response_json = response.json()
            assert_error_response_structure(response_json, ErrorCode.VALIDATION_ERROR, error_message)
        finally:
            if original_override:
                app.dependency_overrides[WellProductionService] = original_override
            else:
                app.dependency_overrides.pop(WellProductionService, None)

    def test_get_field_by_code_not_found(
        self, test_client, api_endpoints, sample_field_code # Assuming fixtures
    ):
        """Test GET /field/{field_code} with ValidationException (404) for field not found."""
        mock_service_instance = AsyncMock(spec=WellProductionService)
        error_message = f"No wells found for field code {sample_field_code}"

        mock_service_instance.get_production_by_field.side_effect = ValidationException(
            message=error_message,
            field="field_code",
            value=sample_field_code,
            status_code_override=status.HTTP_404_NOT_FOUND
        )

        original_override = app.dependency_overrides.get(WellProductionService)
        app.dependency_overrides[WellProductionService] = lambda: mock_service_instance

        response = test_client.get(f"{api_endpoints['field']}/{sample_field_code}")

        try:
            assert response.status_code == status.HTTP_404_NOT_FOUND
            response_json = response.json()
            assert_error_response_structure(response_json, ErrorCode.NOT_FOUND_ERROR, error_message)
        finally:
            if original_override:
                app.dependency_overrides[WellProductionService] = original_override
            else:
                app.dependency_overrides.pop(WellProductionService, None)

    def test_download_no_data_not_found(
        self, test_client, api_endpoints # Assuming fixtures
    ):
        """Test GET /download with ValidationException (404) for no data."""
        mock_service_instance = AsyncMock(spec=WellProductionService)
        error_message = "No well production data available for download"

        # The exception is raised from repository.export_to_csv()
        # The service's download_well_production method calls service.repository.export_to_csv()
        mock_repository = AsyncMock() # Mock the repository object
        mock_repository.export_to_csv.side_effect = ValidationException(
            message=error_message,
            field="csv_export", # Field name as in the original exception
            status_code_override=status.HTTP_404_NOT_FOUND
        )
        # Attach the mocked repository to the mocked service instance
        # This assumes the service has an attribute 'repository' that holds the repo instance
        mock_service_instance.repository = mock_repository

        original_override = app.dependency_overrides.get(WellProductionService)
        app.dependency_overrides[WellProductionService] = lambda: mock_service_instance

        response = test_client.get(api_endpoints["download"])

        try:
            assert response.status_code == status.HTTP_404_NOT_FOUND
            response_json = response.json()
            assert_error_response_structure(response_json, ErrorCode.NOT_FOUND_ERROR, error_message)
        finally:
            if original_override:
                app.dependency_overrides[WellProductionService] = original_override
            else:
                app.dependency_overrides.pop(WellProductionService, None)

    def test_download_generic_application_exception(
        self, test_client, api_endpoints # Assuming fixtures
    ):
        """Test GET /download with a generic ApplicationException (500)."""
        mock_service_instance = AsyncMock(spec=WellProductionService)
        error_message = "A simulated internal error occurred during download"

        mock_repository = AsyncMock()
        mock_repository.export_to_csv.side_effect = ApplicationException(
            message=error_message,
            error_code=ErrorCode.INTERNAL_ERROR
            # http_status_code defaults to 500 in ApplicationException
        )
        mock_service_instance.repository = mock_repository

        original_override = app.dependency_overrides.get(WellProductionService)
        app.dependency_overrides[WellProductionService] = lambda: mock_service_instance

        response = test_client.get(api_endpoints["download"])

        try:
            assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
            response_json = response.json()
            assert_error_response_structure(response_json, ErrorCode.INTERNAL_ERROR, error_message)
        finally:
            if original_override:
                app.dependency_overrides[WellProductionService] = original_override
            else:
                app.dependency_overrides.pop(WellProductionService, None)

    def test_get_stats_generic_application_exception(
        self, test_client, api_endpoints # Assuming fixtures
    ):
        """Test GET /stats with a generic ApplicationException (500)."""
        mock_service_instance = AsyncMock(spec=WellProductionService)
        error_message = "Simulated service layer error for stats"

        mock_service_instance.get_production_statistics.side_effect = ApplicationException(
            message=error_message,
            error_code=ErrorCode.USE_CASE_ERROR # Example specific error code
            # http_status_code defaults to 500 in ApplicationException
        )

        original_override = app.dependency_overrides.get(WellProductionService)
        app.dependency_overrides[WellProductionService] = lambda: mock_service_instance

        response = test_client.get(api_endpoints["stats"])

        try:
            assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
            response_json = response.json()
            assert_error_response_structure(response_json, ErrorCode.USE_CASE_ERROR, error_message)
        finally:
            if original_override:
                app.dependency_overrides[WellProductionService] = original_override
            else:
                app.dependency_overrides.pop(WellProductionService, None)


class TestWellEndpointsIntegration:
    """Integration tests that require data to be imported."""

    def test_full_workflow(self, test_client, api_endpoints, test_assertions):
        """Test the complete workflow: import -> stats -> query."""
        # Step 1: Import data
        import_response = test_client.post(api_endpoints["import"])
        import_data = test_assertions.assert_successful_response(import_response, 201)
        
        imported_count = import_data.get("data", {}).get("imported_count", 0)
        
        # Step 2: Check stats reflect the import
        stats_response = test_client.get(api_endpoints["stats"])
        stats_data = test_assertions.assert_successful_response(stats_response, 200)
        
        total_records = stats_data.get("total_records", 0)
        assert total_records >= imported_count, "Stats should show at least the imported records"
        
        # Step 3: Try to download (should work if data exists)
        if total_records > 0:
            download_response = test_client.get(api_endpoints["download"])
            assert download_response.status_code == 200, "Download should work when data exists" 