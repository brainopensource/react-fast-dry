"""
Domain port for external API services.
Defines the contract for external data fetching without coupling to specific implementations.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from ..entities.well_production import WellProduction


class ExternalApiPort(ABC):
    """Port for external API data fetching"""
    
    @abstractmethod
    async def fetch_well_production_data(
        self, 
        endpoint: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[WellProduction]:
        """
        Fetch well production data from external source.
        
        Args:
            endpoint: Optional specific endpoint to fetch from
            filters: Optional filters to apply
            
        Returns:
            List of WellProduction entities
            
        Raises:
            ExternalApiException: When external API call fails
        """
        pass
    
    @abstractmethod
    async def validate_connection(self) -> bool:
        """
        Validate connection to external API.
        
        Returns:
            True if connection is valid, False otherwise
        """
        pass
    
    @abstractmethod
    async def get_api_status(self) -> Dict[str, Any]:
        """
        Get status information from external API.
        
        Returns:
            Dictionary with status information
        """
        pass 