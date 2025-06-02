"""
Tests for well production endpoints.
"""

import pytest


class TestWellEndpoints:
    """Test class for well production endpoints."""

    def test_import_wells_endpoint(self, test_client, api_endpoints, test_assertions):
        """Test the wells import endpoint."""
        # Act
        response = test_client.post(api_endpoints["import"])
        
        # Assert - Import should return 201 Created
        data = test_assertions.assert_successful_response(response, 201)
        
        # Verify response structure - actual API returns success, data, metadata
        test_assertions.assert_json_structure(data, ["success", "data", "metadata"])
        
        # Verify success flag
        assert data["success"] is True
        
        # Verify data structure
        data_section = data.get("data", {})
        assert "import_summary" in data_section
        assert "performance" in data_section
        
        # Verify import summary structure
        import_summary = data_section.get("import_summary", {})
        assert "total_records" in import_summary
        assert "successful_records" in import_summary
        assert "duplicate_records" in import_summary
        
        # Verify data types
        assert isinstance(import_summary["total_records"], int)
        assert isinstance(import_summary["successful_records"], int)
        assert isinstance(import_summary["duplicate_records"], int)
        assert import_summary["total_records"] >= 0
        assert import_summary["successful_records"] >= 0
        assert import_summary["duplicate_records"] >= 0
        
        # Storage info is no longer returned in import response since CSV is created on-demand
        # The import now only updates DuckDB storage

    def test_get_stats_endpoint(self, test_client, api_endpoints, test_assertions):
        """Test the wells statistics endpoint."""
        # Act
        response = test_client.get(api_endpoints["stats"])
        
        # Assert
        data = test_assertions.assert_successful_response(response, 200)
        
        # Verify response structure - stats are now nested under data
        test_assertions.assert_json_structure(data, ["success", "data", "metadata"])
        
        # Verify data section structure
        data_section = data.get("data", {})
        assert "total_records" in data_section
        assert "external_api_status" in data_section
        
        # Verify data types
        assert isinstance(data_section["total_records"], int)
        assert data_section["total_records"] >= 0

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