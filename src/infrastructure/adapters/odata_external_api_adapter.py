"""
OData External API Adapter for fetching well production data from external OData APIs.
Implements hexagonal architecture principles with proper error handling and pagination support.
"""
import logging
import asyncio
from typing import List, Dict, Any, Optional, AsyncGenerator
from datetime import datetime
#import httpx
from requests.auth import HTTPBasicAuth
import polars as pl

from ...domain.ports.odata_external_api_port import ODataExternalApiPort
from ...shared.exceptions import (
    ExternalApiException,
    ValidationException
)
from ...shared.utils.timing_decorator import async_timed

logger = logging.getLogger(__name__)


class ODataExternalApiAdapter(ODataExternalApiPort):
    """
    Adapter for OData external API services with pagination support.
    Handles Basic Authentication and follows OData pagination patterns.
    """
    
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout_seconds: int = 60,
        max_retries: int = 3,
        retry_delay_seconds: float = 2.0,
        max_records_per_request: int = 998
    ):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.max_records_per_request = max_records_per_request
        
        # Validate required configuration
        if not all([base_url, username, password]):
            raise ValueError("base_url, username, and password are required for OData API")
    
    @async_timed
    async def fetch_well_production_data(
        self, 
        endpoint: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> pl.DataFrame:
        """
        Fetch well production data from OData API with pagination support.
        
        Args:
            endpoint: Optional specific endpoint (not used in this implementation)
            filters: Optional filters to apply (not used in this implementation)
            
        Returns:
            Polars DataFrame with all fetched data
            
        Raises:
            ExternalApiException: When API call fails
            ValidationException: When data validation fails
        """
        try:
            logger.info("Starting OData API data fetch with pagination")
            
            all_records = []
            page_count = 0
            total_records = 0
            
            # Start with the base URL
            current_url = self.base_url
            
            while current_url:
                page_count += 1
                logger.info(f"Fetching page {page_count} from: {current_url}")
                
                # Fetch current page
                page_data = await self._fetch_page(current_url)
                
                if not page_data:
                    logger.warning(f"No data returned from page {page_count}")
                    break
                
                # Extract records from OData response
                records = page_data.get('value', [])
                if not records:
                    logger.info(f"No records in page {page_count}, stopping pagination")
                    break
                
                all_records.extend(records)
                total_records += len(records)
                
                logger.info(f"Page {page_count}: fetched {len(records)} records (total: {total_records})")
                
                # Check for next page URL in @odata.nextLink
                current_url = page_data.get('@odata.nextLink')
                if not current_url:
                    logger.info("No more pages available, pagination complete")
                    break
                
                # Add small delay between requests to be respectful to the API
                await asyncio.sleep(0.1)
            
            logger.info(f"Completed OData fetch: {total_records} total records from {page_count} pages")
            
            if not all_records:
                logger.info("No records fetched from OData API")
                return pl.DataFrame()
            
            # Convert to Polars DataFrame
            try:
                df = pl.DataFrame(all_records)
                logger.info(f"Successfully created Polars DataFrame with {df.height} rows and {df.width} columns")
                return df
            except Exception as e:
                logger.error(f"Error creating Polars DataFrame from OData records: {e}", exc_info=True)
                raise ValidationException(
                    message=f"Could not convert OData records to Polars DataFrame: {str(e)}",
                    field="odata_records_conversion"
                )
                
        except (ExternalApiException, ValidationException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error during OData fetch: {str(e)}", exc_info=True)
            raise ExternalApiException(
                message=f"Unexpected error during OData fetch: {str(e)}",
                endpoint=self.base_url,
                cause=e
            )
    
    async def _fetch_page(self, url: str) -> Dict[str, Any]:
        """
        Fetch a single page from the OData API with retry logic.
        
        Args:
            url: URL to fetch
            
        Returns:
            Dictionary with page data
            
        Raises:
            ExternalApiException: When API call fails
        """
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"Fetching URL: {url} (attempt {attempt + 1}/{self.max_retries})")
                
                # Prepare query parameters for pagination
                params = {
                    '$top': self.max_records_per_request,
                    '$format': 'json'
                }
                
                # Use requests with Basic Auth (synchronous call in async context)
                # Note: We could use httpx for async, but requests is simpler for Basic Auth
                response = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: requests.get(
                        url,
                        params=params,
                        auth=HTTPBasicAuth(self.username, self.password),
                        timeout=self.timeout_seconds
                    )
                )
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        return data
                    except ValueError as e:
                        raise ExternalApiException(
                            message=f"Invalid JSON response from OData API: {str(e)}",
                            endpoint=url,
                            status_code=response.status_code
                        )
                
                elif response.status_code >= 500:
                    # Server error - retry
                    error_msg = f"OData API server error: {response.status_code}"
                    if attempt < self.max_retries - 1:
                        logger.warning(f"{error_msg}, retrying in {self.retry_delay_seconds}s...")
                        await asyncio.sleep(self.retry_delay_seconds * (attempt + 1))
                        continue
                    else:
                        raise ExternalApiException(
                            message=error_msg,
                            endpoint=url,
                            status_code=response.status_code
                        )
                
                elif response.status_code == 401:
                    raise ExternalApiException(
                        message="Authentication failed - check username and password",
                        endpoint=url,
                        status_code=response.status_code
                    )
                
                elif response.status_code == 403:
                    raise ExternalApiException(
                        message="Access forbidden - insufficient permissions",
                        endpoint=url,
                        status_code=response.status_code
                    )
                
                else:
                    # Client error - don't retry
                    raise ExternalApiException(
                        message=f"OData API client error: {response.status_code}",
                        endpoint=url,
                        status_code=response.status_code
                    )
            
            except requests.exceptions.Timeout:
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
            
            except requests.exceptions.RequestException as e:
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
                    message=f"Unexpected error during page fetch: {str(e)}",
                    endpoint=url,
                    cause=e
                )
        
        # This should never be reached due to the retry logic above
        raise ExternalApiException(
            message=f"Failed to fetch page after {self.max_retries} attempts",
            endpoint=url
        )
    
    async def validate_connection(self) -> bool:
        """
        Validate connection to OData API.
        
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            logger.info("Validating OData API connection")
            
            # Try to fetch just one record to test connection
            test_params = {
                '$top': 1,
                '$format': 'json'
            }
            
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: requests.get(
                    self.base_url,
                    params=test_params,
                    auth=HTTPBasicAuth(self.username, self.password),
                    timeout=10  # Shorter timeout for connection test
                )
            )
            
            is_valid = response.status_code == 200
            logger.info(f"OData API connection validation: {'successful' if is_valid else 'failed'} (status: {response.status_code})")
            return is_valid
            
        except Exception as e:
            logger.warning(f"OData API connection validation failed: {str(e)}")
            return False
    
    async def get_api_status(self) -> Dict[str, Any]:
        """
        Get status information from OData API.
        
        Returns:
            Dictionary with status information
        """
        try:
            is_connected = await self.validate_connection()
            return {
                "status": "connected" if is_connected else "disconnected",
                "api_type": "odata",
                "base_url": self.base_url,
                "username": self.username,
                "timeout_seconds": self.timeout_seconds,
                "max_records_per_request": self.max_records_per_request,
                "last_check": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Error getting OData API status: {str(e)}")
            return {
                "status": "error",
                "api_type": "odata",
                "error_message": str(e),
                "last_check": datetime.utcnow().isoformat()
            } 