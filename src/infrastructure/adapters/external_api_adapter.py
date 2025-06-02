"""
Improved external API adapter with comprehensive error handling and recovery.
Implements the ExternalApiPort interface without tight coupling.
"""
import json
import logging
import asyncio
from typing import List, Dict, Any, Optional
from pathlib import Path
import httpx
from datetime import datetime
import polars as pl

from ...domain.ports.external_api_port import ExternalApiPort
from ...domain.entities.well_production import WellProduction
from ...shared.config.settings import get_settings # Added import
from ...shared.exceptions import (
    ExternalApiException, 
    FileSystemException,
    ValidationException
)
from ...shared.utils.timing_decorator import async_timed # Added import

logger = logging.getLogger(__name__)


class ExternalApiAdapter(ExternalApiPort):
    """
    Adapter for external API services with comprehensive error handling.
    Supports both real API calls and mock mode for development/testing.
    """
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        mock_mode: bool = True,
        mock_file_path: Optional[str] = None,
        timeout_seconds: int = 30,
        max_retries: int = 3,
        retry_delay_seconds: float = 1.0
    ):
        self.base_url = base_url
        self.api_key = api_key
        self.mock_mode = mock_mode
        self.mock_file_path = Path(mock_file_path) if mock_file_path else get_settings().MOCKED_RESPONSE_PATH
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        
        # Validate configuration
        if not mock_mode and not base_url:
            raise ValueError("base_url is required when not in mock mode")
    
    async def fetch_well_production_data(
        self, 
        endpoint: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[WellProduction]:
        """
        Fetch well production data from external source with retry logic.
        
        Args:
            endpoint: Optional specific endpoint
            filters: Optional filters to apply
            
        Returns:
            List of WellProduction entities
            
        Raises:
            ExternalApiException: When API call fails
            ValidationException: When data validation fails
        """
        if self.mock_mode:
            return await self._fetch_mock_data()
        else:
            return await self._fetch_real_data(endpoint, filters)
    
    async def validate_connection(self) -> bool:
        """
        Validate connection to external API.
        
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            if self.mock_mode:
                # For mock mode, check if mock file exists
                return self.mock_file_path.exists()
            else:
                # For real API, try a simple health check
                async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                    response = await client.get(f"{self.base_url}/health")
                    return response.status_code == 200
        except Exception as e:
            logger.warning(f"Connection validation failed: {str(e)}")
            return False
    
    async def get_api_status(self) -> Dict[str, Any]:
        """
        Get status information from external API.
        
        Returns:
            Dictionary with status information
        """
        try:
            if self.mock_mode:
                return {
                    "status": "mock_mode",
                    "mock_file_exists": self.mock_file_path.exists(),
                    "mock_file_path": str(self.mock_file_path),
                    "last_check": datetime.utcnow().isoformat()
                }
            else:
                is_connected = await self.validate_connection()
                return {
                    "status": "connected" if is_connected else "disconnected",
                    "base_url": self.base_url,
                    "timeout_seconds": self.timeout_seconds,
                    "last_check": datetime.utcnow().isoformat()
                }
        except Exception as e:
            logger.error(f"Error getting API status: {str(e)}")
            return {
                "status": "error",
                "error_message": str(e),
                "last_check": datetime.utcnow().isoformat()
            }
    
    @async_timed # Added decorator
    async def _fetch_mock_data(self) -> pl.DataFrame:
        """
        Fetch data from mock file with error handling, returning a Polars DataFrame.
        
        Returns:
            polars.DataFrame: The data loaded from the mock file.
            
        Raises:
            FileSystemException: When file operations fail
            ValidationException: When data format is invalid or DataFrame creation fails
        """
        try:
            if not self.mock_file_path.exists():
                raise FileSystemException(
                    message=f"Mock file not found: {self.mock_file_path}",
                    file_path=str(self.mock_file_path)
                )
            
            logger.info(f"Loading mock data from {self.mock_file_path} using Polars")
            
            # Read the entire JSON structure first
            with open(self.mock_file_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            
            # Extract the relevant part of the data for Polars
            if 'value' in raw_data:
                wells_data_list = raw_data['value']
            else:
                wells_data_list = raw_data
            
            if not isinstance(wells_data_list, list):
                raise ValidationException(
                    message="Mock data must be a list or contain a 'value' field with a list of records for Polars.",
                    field="wells_data"
                )

            if not wells_data_list: # Handle empty list case
                logger.info("Mock data list is empty. No records to process.")
                return pl.DataFrame() # Return an empty DataFrame

            # Convert the list of dicts to a Polars DataFrame
            try:
                # Polars can infer schema. If specific dtypes or name mapping is needed,
                # it can be specified here. For now, assume direct conversion is okay.
                # The _map_well_data_fields logic would need to be translated to Polars operations
                # if JSON field names differ significantly from desired DataFrame column names
                # or if complex transformations are needed before creating WellProduction entities later.
                df = pl.DataFrame(wells_data_list)
                
                # Optional: Add explicit field mapping or type casting here using Polars if needed
                # e.g., df = df.rename({"json_field_name": "entity_field_name"})
                # e.g., df = df.with_columns([
                # pl.col("date_str_column").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f").alias("created_at"),
                # ])
                # This would replace the _map_well_data_fields and parts of WellProduction instantiation logic later on.

            except Exception as e: # Catch Polars-specific errors if any during DataFrame creation
                logger.error(f"Error creating Polars DataFrame from mock data: {e}", exc_info=True)
                raise ValidationException(
                    message=f"Could not convert mock data to Polars DataFrame: {str(e)}",
                    field="wells_data_conversion"
                )
            
            logger.info(f"Successfully loaded {df.height} well production records into Polars DataFrame")
            return df # Return the DataFrame directly
            
        except (FileSystemException, ValidationException):
            raise
        except json.JSONDecodeError as e:
            raise ValidationException(
                message=f"Invalid JSON in mock file: {str(e)}",
                field="json_format"
            )
        except Exception as e:
            raise FileSystemException(
                message=f"Error reading mock file: {str(e)}",
                file_path=str(self.mock_file_path),
                cause=e
            )
    
    @async_timed # Added decorator
    async def _fetch_real_data(
        self, 
        endpoint: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[WellProduction]:
        """
        Fetch data from real external API with retry logic.
        
        Args:
            endpoint: API endpoint to call
            filters: Query filters
            
        Returns:
            List of WellProduction entities
            
        Raises:
            ExternalApiException: When API call fails
        """
        endpoint = endpoint or "/wells_production"
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Fetching data from {url} (attempt {attempt + 1}/{self.max_retries})")
                
                async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                    # Prepare headers
                    headers = {}
                    if self.api_key:
                        headers["Authorization"] = f"Bearer {self.api_key}"
                    
                    # Prepare query parameters
                    params = filters or {}
                    
                    response = await client.get(url, headers=headers, params=params)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        # Extract wells from OData response format
                        wells_data = data.get('value', data)
                        
                        # Convert to WellProduction entities
                        well_productions = []
                        for well_data in wells_data:
                            try:
                                # Map field names from JSON format to entity format
                                mapped_data = self._map_well_data_fields(well_data)
                                well_production = WellProduction(**mapped_data)
                                well_productions.append(well_production)
                            except Exception as e:
                                logger.warning(f"Skipping invalid well data: {str(e)}")
                        
                        logger.info(f"Successfully fetched {len(well_productions)} records from API")
                        return well_productions
                    
                    else:
                        # Handle HTTP error responses
                        error_msg = f"API returned status {response.status_code}"
                        if response.status_code >= 500:
                            # Server error - retry
                            if attempt < self.max_retries - 1:
                                logger.warning(f"{error_msg}, retrying in {self.retry_delay_seconds}s...")
                                await asyncio.sleep(self.retry_delay_seconds * (attempt + 1))
                                continue
                        
                        # Client error or final server error - don't retry
                        raise ExternalApiException(
                            message=error_msg,
                            endpoint=url,
                            status_code=response.status_code
                        )
            
            except httpx.TimeoutException:
                error_msg = f"Request timeout after {self.timeout_seconds}s"
                if attempt < self.max_retries - 1:
                    logger.warning(f"{error_msg}, retrying in {self.retry_delay_seconds}s...")
                    await asyncio.sleep(self.retry_delay_seconds * (attempt + 1))
                    continue
                else:
                    raise ExternalApiException(
                        message=error_msg,
                        endpoint=url
                    )
            
            except httpx.RequestError as e:
                error_msg = f"Request error: {str(e)}"
                if attempt < self.max_retries - 1:
                    logger.warning(f"{error_msg}, retrying in {self.retry_delay_seconds}s...")
                    await asyncio.sleep(self.retry_delay_seconds * (attempt + 1))
                    continue
                else:
                    raise ExternalApiException(
                        message=error_msg,
                        endpoint=url,
                        cause=e
                    )
            
            except Exception as e:
                # Unexpected error - don't retry
                raise ExternalApiException(
                    message=f"Unexpected error: {str(e)}",
                    endpoint=url,
                    cause=e
                )
        
        # This should never be reached due to the retry logic above
        raise ExternalApiException(
            message=f"Failed to fetch data after {self.max_retries} attempts",
            endpoint=url
        )
    
    def _map_well_data_fields(self, well_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map field names from external API format to WellProduction entity format.
        
        Args:
            well_data: Raw well data from external API
            
        Returns:
            Mapped data with correct field names for WellProduction entity
        """
        mapped_data = well_data.copy()
        
        # Map underscore-prefixed fields to entity field names
        field_mappings = {
            '_field_name': 'field_name',
            '_well_reference': 'well_reference'
        }
        
        for old_key, new_key in field_mappings.items():
            if old_key in mapped_data:
                mapped_data[new_key] = mapped_data.pop(old_key)
        
        return mapped_data 