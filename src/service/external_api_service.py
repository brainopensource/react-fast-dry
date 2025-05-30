### service/external_api_service.py
import json
import os
import httpx
from typing import Dict, Any, Optional
from ..domain.models import ODataResponse, WellProductionExternal
from datetime import datetime


class ExternalApiService:
    """Service for handling external OData API calls with mocking capability"""
    
    def __init__(self, mock_mode: bool = True, mock_file_path: str = "external/mocked_response.json", 
                 base_url: Optional[str] = None, timeout: int = 30):
        self.mock_mode = mock_mode
        self.mock_file_path = mock_file_path
        self.base_url = base_url
        self.timeout = timeout

    async def fetch_well_production_data(self, endpoint: str = "/wells_production") -> Dict[str, Any]:
        """
        Fetch well production data from external OData API
        Returns a mock response with status code 200 when in mock mode
        """        
        if self.mock_mode:
            return self._get_mocked_response()
        else:
            return await self._fetch_real_data(endpoint)
    
    async def _fetch_real_data(self, endpoint: str) -> Dict[str, Any]:
        """Make actual HTTP call to external API (for production use)"""
        if not self.base_url:
            raise ValueError("base_url must be provided when not in mock mode")
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(f"{self.base_url}{endpoint}")
                return {
                    "status_code": response.status_code,
                    "data": response.json() if response.status_code == 200 else None
                }
        except httpx.TimeoutException:
            raise TimeoutError("External API request timed out")
        except httpx.RequestError as e:
            raise ConnectionError(f"Error connecting to external API: {e}")
        
    def _get_mocked_response(self) -> Dict[str, Any]:
        """Load and return the mocked response from the JSON file"""
        try:
            with open(self.mock_file_path, 'r', encoding='utf-8') as f:
                mock_data = json.load(f)
            
            return {
                "status_code": 200,
                "data": mock_data
            }
        except FileNotFoundError:
            raise FileNotFoundError(f"Mock file not found at {self.mock_file_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in mock file: {e}")
    
    def transform_external_to_internal(self, external_data: list[WellProductionExternal]) -> list[WellProductionExternal]:
        """
        We now use the external model directly, but could process fields here if needed
        """
        # No transformation needed for now - we use the external model directly
        return external_data
