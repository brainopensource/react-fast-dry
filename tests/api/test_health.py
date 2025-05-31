"""
Tests for health check endpoints.
"""

import pytest


class TestHealthEndpoints:
    """Test class for health-related endpoints."""

    def test_health_endpoint_success(self, test_client, api_endpoints, test_assertions):
        """Test that the health endpoint returns a successful response."""
        # Act
        response = test_client.get(api_endpoints["health"])
        
        # Assert
        data = test_assertions.assert_successful_response(response, 200)
        test_assertions.assert_json_structure(data, ["status", "service"])
        
        assert data["status"] == "healthy"
        assert data["service"] == "well-production-api"

    def test_health_endpoint_response_structure(self, test_client, api_endpoints):
        """Test that the health endpoint returns the expected JSON structure."""
        # Act
        response = test_client.get(api_endpoints["health"])
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        
        # Verify required fields
        assert "status" in data
        assert "service" in data
        
        # Verify field types
        assert isinstance(data["status"], str)
        assert isinstance(data["service"], str)

    def test_root_endpoint_success(self, test_client, api_endpoints, test_assertions):
        """Test that the root endpoint returns API information."""
        # Act
        response = test_client.get(api_endpoints["root"])
        
        # Assert
        data = test_assertions.assert_successful_response(response, 200)
        test_assertions.assert_json_structure(data, ["message", "version", "features", "endpoints"])
        
        assert data["message"] == "Well Production API"
        assert data["version"] == "1.0.0"

    def test_root_endpoint_features(self, test_client, api_endpoints):
        """Test that the root endpoint returns expected features information."""
        # Act
        response = test_client.get(api_endpoints["root"])
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        
        # Verify features section
        features = data.get("features", {})
        assert "storage" in features
        assert "performance" in features
        assert "architecture" in features
        
        # Verify endpoints section
        endpoints = data.get("endpoints", {})
        assert "import" in endpoints
        assert "download" in endpoints
        assert "stats" in endpoints
        assert "docs" in endpoints 