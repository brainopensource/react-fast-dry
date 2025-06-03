"""
OData external service adapter for connecting to real OData APIs.
Implements the IExternalDataService interface with HTTP client functionality.
"""

import logging
import asyncio
import aiohttp
from typing import List, Dict, Any, Optional
from urllib.parse import urlencode

from ...domain.ports.services import IExternalDataService
from ...shared.config.schemas import get_dataset_config
from ...shared.exceptions import ApplicationException, ErrorCode

logger = logging.getLogger(__name__)

class ODataExternalService(IExternalDataService):
    """
    OData external service for connecting to real OData APIs.
    Provides async HTTP client functionality with retry logic.
    """
    
    def __init__(self, dataset_name: str, config: Dict[str, Any]):
        """Initialize OData external service."""
        self.dataset_name = dataset_name
        self.config = config
        self.dataset_config = get_dataset_config(dataset_name)
        self.logger = logging.getLogger(f"{__name__}.{dataset_name}")
        
        # OData configuration
        self.base_url = config.get('base_url', '').rstrip('/')
        self.timeout_seconds = config.get('timeout_seconds', 30)
        self.max_retries = config.get('max_retries', 3)
        self.retry_delay_seconds = config.get('retry_delay_seconds', 1.0)
        
        # Authentication (if needed - extend as required)
        self.username = config.get('username')
        self.password = config.get('password')
        
        # HTTP session
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            # Create connector with appropriate settings
            connector = aiohttp.TCPConnector(
                limit=100,  # Total connection limit
                limit_per_host=30,  # Per-host connection limit
                ttl_dns_cache=300,  # DNS cache TTL
                use_dns_cache=True,
            )
            
            # Create timeout configuration
            timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
            
            # Create basic auth if credentials provided
            auth = None
            if self.username and self.password:
                auth = aiohttp.BasicAuth(self.username, self.password)
            
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                auth=auth,
                headers={
                    'User-Agent': 'Generic-Data-Management-API/2.0.0',
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                }
            )
        
        return self._session
    
    async def fetch_data(self, entity_set: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Fetch data from OData endpoint."""
        try:
            self.logger.info(f"Fetching data from OData endpoint: {entity_set}")
            
            # Build OData URL
            url = self._build_odata_url(entity_set, filters)
            
            # Fetch data with retry logic
            data = await self._fetch_with_retry(url)
            
            # Extract records from OData response
            records = self._extract_records(data)
            
            self.logger.info(f"Successfully fetched {len(records)} records from {entity_set}")
            return records
            
        except Exception as e:
            self.logger.error(f"Error fetching data from OData: {str(e)}")
            raise ApplicationException(
                message=f"Failed to fetch data from OData endpoint: {entity_set}",
                error_code=ErrorCode.EXTERNAL_SERVICE_ERROR,
                cause=e
            )
    
    async def get_data_count(self, entity_set: str, filters: Optional[Dict[str, Any]] = None) -> int:
        """Get count of data from OData endpoint."""
        try:
            # Build OData count URL
            count_url = self._build_odata_url(entity_set, filters, count_only=True)
            
            # Fetch count
            result = await self._fetch_with_retry(count_url)
            
            # OData count responses can be different formats
            if isinstance(result, int):
                return result
            elif isinstance(result, dict) and 'value' in result:
                return int(result['value'])
            elif isinstance(result, str) and result.isdigit():
                return int(result)
            else:
                self.logger.warning(f"Unexpected count response format: {type(result)}")
                return 0
                
        except Exception as e:
            self.logger.error(f"Error getting data count from OData: {str(e)}")
            return 0
    
    async def test_connection(self) -> bool:
        """Test connection to OData endpoint."""
        try:
            # Try to access the service root
            session = await self._get_session()
            
            test_url = f"{self.base_url}/$metadata"
            
            async with session.get(test_url) as response:
                if response.status in [200, 201]:
                    self.logger.info("OData connection test successful")
                    return True
                else:
                    self.logger.warning(f"OData connection test failed with status: {response.status}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"OData connection test failed: {str(e)}")
            return False
    
    def _build_odata_url(self, entity_set: str, filters: Optional[Dict[str, Any]] = None, count_only: bool = False) -> str:
        """Build OData URL with query parameters."""
        url = f"{self.base_url}/{entity_set}"
        
        if count_only:
            url += "/$count"
        
        # Build query parameters
        query_params = {}
        
        if not count_only:
            # Add $select for specified fields
            if self.dataset_config.odata_select_fields:
                query_params['$select'] = ','.join(self.dataset_config.odata_select_fields)
        
        # Add filters
        if filters:
            filter_expressions = []
            
            for field, value in filters.items():
                if isinstance(value, dict):
                    # Handle complex filters
                    for operator, filter_value in value.items():
                        expression = self._build_filter_expression(field, operator, filter_value)
                        if expression:
                            filter_expressions.append(expression)
                else:
                    # Simple equality filter
                    if isinstance(value, str):
                        filter_expressions.append(f"{field} eq '{value}'")
                    else:
                        filter_expressions.append(f"{field} eq {value}")
            
            if filter_expressions:
                query_params['$filter'] = ' and '.join(filter_expressions)
        
        # Add query parameters to URL
        if query_params:
            url += '?' + urlencode(query_params)
        
        return url
    
    def _build_filter_expression(self, field: str, operator: str, value: Any) -> str:
        """Build OData filter expression."""
        # Map internal operators to OData operators
        odata_operators = {
            'eq': 'eq',
            'ne': 'ne',
            'gt': 'gt',
            'gte': 'ge',
            'lt': 'lt',
            'lte': 'le',
            'like': 'contains',  # OData uses contains for like operations
        }
        
        odata_op = odata_operators.get(operator)
        if not odata_op:
            self.logger.warning(f"Unsupported filter operator: {operator}")
            return ""
        
        # Format value for OData
        if isinstance(value, str):
            if odata_op == 'contains':
                return f"contains({field}, '{value}')"
            else:
                return f"{field} {odata_op} '{value}'"
        else:
            return f"{field} {odata_op} {value}"
    
    async def _fetch_with_retry(self, url: str) -> Dict[str, Any]:
        """Fetch data with retry logic."""
        session = await self._get_session()
        
        for attempt in range(self.max_retries + 1):
            try:
                self.logger.debug(f"Fetching URL (attempt {attempt + 1}): {url}")
                
                async with session.get(url) as response:
                    if response.status in [200, 201]:
                        return await response.json()
                    elif response.status == 404:
                        raise ApplicationException(
                            message=f"OData endpoint not found: {url}",
                            error_code=ErrorCode.EXTERNAL_SERVICE_ERROR
                        )
                    elif response.status in [401, 403]:
                        raise ApplicationException(
                            message="OData authentication failed",
                            error_code=ErrorCode.AUTHENTICATION_ERROR
                        )
                    else:
                        error_text = await response.text()
                        raise ApplicationException(
                            message=f"OData request failed with status {response.status}: {error_text}",
                            error_code=ErrorCode.EXTERNAL_SERVICE_ERROR
                        )
                        
            except aiohttp.ClientError as e:
                if attempt < self.max_retries:
                    self.logger.warning(f"HTTP request failed (attempt {attempt + 1}), retrying: {str(e)}")
                    await asyncio.sleep(self.retry_delay_seconds * (attempt + 1))
                else:
                    raise ApplicationException(
                        message=f"HTTP request failed after {self.max_retries + 1} attempts",
                        error_code=ErrorCode.EXTERNAL_SERVICE_ERROR,
                        cause=e
                    )
            except Exception as e:
                if attempt < self.max_retries:
                    self.logger.warning(f"Request failed (attempt {attempt + 1}), retrying: {str(e)}")
                    await asyncio.sleep(self.retry_delay_seconds * (attempt + 1))
                else:
                    raise
        
        # This shouldn't be reached, but just in case
        raise ApplicationException(
            message="Unexpected error in retry logic",
            error_code=ErrorCode.EXTERNAL_SERVICE_ERROR
        )
    
    def _extract_records(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract records from OData response."""
        try:
            # OData responses typically have data in 'value' field
            if isinstance(data, dict) and 'value' in data:
                return data['value']
            elif isinstance(data, list):
                return data
            elif isinstance(data, dict):
                # Single record response
                return [data]
            else:
                self.logger.warning(f"Unexpected OData response format: {type(data)}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error extracting records from OData response: {str(e)}")
            return []
    
    async def close(self):
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self.logger.info("OData HTTP session closed")
    
    def __del__(self):
        """Cleanup when object is destroyed."""
        if self._session and not self._session.closed:
            # Note: This might not work in all cases due to event loop issues
            # It's better to explicitly call close() in application shutdown
            try:
                asyncio.create_task(self._session.close())
            except Exception:
                pass 