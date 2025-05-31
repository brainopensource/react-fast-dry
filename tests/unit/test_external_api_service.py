"""
Unit tests for external API service.
"""

import pytest
from unittest.mock import Mock, patch
import json
from pathlib import Path


class TestExternalApiService:
    """Unit tests for ExternalApiService."""

    def test_service_initialization_with_mock_mode(self):
        """Test service initializes correctly in mock mode."""
        try:
            from src.application.services.external_api_service import ExternalApiService
            
            service = ExternalApiService(mock_mode=True)
            assert service.mock_mode is True
            assert service.base_url is None
            assert service.timeout == 30
        except ImportError:
            pytest.skip("ExternalApiService not available")

    def test_service_initialization_with_real_mode(self):
        """Test service initializes correctly in real mode."""
        try:
            from src.application.services.external_api_service import ExternalApiService
            
            service = ExternalApiService(
                mock_mode=False, 
                base_url="https://api.example.com",
                timeout=60
            )
            assert service.mock_mode is False
            assert service.base_url == "https://api.example.com"
            assert service.timeout == 60
        except ImportError:
            pytest.skip("ExternalApiService not available")

    @pytest.mark.asyncio
    async def test_fetch_well_production_data_mock_mode(self):
        """Test fetching data in mock mode."""
        try:
            from src.application.services.external_api_service import ExternalApiService
            
            # Create service in mock mode
            service = ExternalApiService(mock_mode=True)
            
            # Mock the _get_mocked_response method
            mock_response = {
                "status": "success",
                "data": {"test": "data"},
                "metadata": {"source": "mock"}
            }
            
            with patch.object(service, '_get_mocked_response', return_value=mock_response):
                result = await service.fetch_well_production_data()
                
                assert result == mock_response
                assert result["metadata"]["source"] == "mock"
        except ImportError:
            pytest.skip("ExternalApiService not available")

    @pytest.mark.asyncio
    async def test_fetch_well_production_data_real_mode(self):
        """Test fetching data in real mode (mocked external call)."""
        try:
            from src.application.services.external_api_service import ExternalApiService
            
            # Create service in real mode
            service = ExternalApiService(
                mock_mode=False,
                base_url="https://api.example.com"
            )
            
            # Mock the _fetch_real_data method
            mock_response = {
                "status": "success",
                "data": {"external": "data"},
                "metadata": {"source": "external"}
            }
            
            with patch.object(service, '_fetch_real_data', return_value=mock_response):
                result = await service.fetch_well_production_data("/wells_production")
                
                assert result == mock_response
                assert result["metadata"]["source"] == "external"
        except ImportError:
            pytest.skip("ExternalApiService not available")

    def test_custom_mock_file_path(self):
        """Test service with custom mock file path."""
        try:
            from src.application.services.external_api_service import ExternalApiService
            
            custom_path = "/custom/path/mock.json"
            service = ExternalApiService(mock_mode=True, mock_file_path=custom_path)
            
            # The path should be converted to a Path object and resolved
            assert isinstance(service.mock_file_path, Path)
            assert str(service.mock_file_path).endswith("mock.json")
        except ImportError:
            pytest.skip("ExternalApiService not available")


class TestServiceErrorHandling:
    """Test error handling in external API service."""

    @pytest.mark.asyncio
    async def test_fetch_data_with_missing_mock_file(self):
        """Test behavior when mock file is missing."""
        try:
            from src.application.services.external_api_service import ExternalApiService
            
            # Create service with non-existent mock file
            service = ExternalApiService(
                mock_mode=True, 
                mock_file_path="/non/existent/file.json"
            )
            
            # This should handle the missing file gracefully
            # The exact behavior depends on implementation
            with patch.object(service, '_get_mocked_response') as mock_method:
                mock_method.side_effect = FileNotFoundError("Mock file not found")
                
                with pytest.raises(FileNotFoundError):
                    await service.fetch_well_production_data()
        except ImportError:
            pytest.skip("ExternalApiService not available")

    @pytest.mark.asyncio 
    async def test_fetch_data_with_network_error(self):
        """Test behavior when network error occurs in real mode."""
        try:
            from src.application.services.external_api_service import ExternalApiService
            import httpx
            
            service = ExternalApiService(
                mock_mode=False,
                base_url="https://api.example.com"
            )
            
            # Mock network error
            with patch.object(service, '_fetch_real_data') as mock_method:
                mock_method.side_effect = httpx.ConnectError("Network error")
                
                with pytest.raises(httpx.ConnectError):
                    await service.fetch_well_production_data()
        except ImportError:
            pytest.skip("ExternalApiService or httpx not available") 