"""
Domain port for OData external API services.
Defines the contract for OData data fetching that returns Polars DataFrames for performance.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import polars as pl


class ODataExternalApiPort(ABC):
    """Port for OData external API data fetching that returns Polars DataFrames"""
    
    @abstractmethod
    async def fetch_well_production_data(
        self, 
        endpoint: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> pl.DataFrame:
        """
        Fetch well production data from OData external source.
        
        Args:
            endpoint: Optional specific endpoint to fetch from
            filters: Optional filters to apply
            
        Returns:
            Polars DataFrame with fetched data
            
        Raises:
            ExternalApiException: When external API call fails
        """
        pass
    
    @abstractmethod
    async def validate_connection(self) -> bool:
        """
        Validate connection to OData external API.
        
        Returns:
            True if connection is valid, False otherwise
        """
        pass
    
    @abstractmethod
    async def get_api_status(self) -> Dict[str, Any]:
        """
        Get status information from OData external API.
        
        Returns:
            Dictionary with status information
        """
        pass 