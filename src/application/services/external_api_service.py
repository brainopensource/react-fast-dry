import logging
from typing import Dict, Any, Optional, List
# Assuming WellProduction is the correct entity to be used.
# Adjust if WellProductionExternal or another model is specifically needed by fetchers.py
# For now, adapter returns List[WellProduction], so we use that.
from src.domain.entities.well_production import WellProduction
from src.shared.dependencies import get_container

logger = logging.getLogger(__name__)

class ExternalApiService:
    """
    Service for interacting with external API.
    Uses dependency injection for configuration and adapters.
    """
    
    def __init__(self):
        """Initialize the service with dependencies from the container."""
        self.adapter = get_container().get_external_api_adapter()

    async def fetch_well_production_data(self, endpoint: str = "/wells_production") -> Dict[str, Any]:
        """
        Fetches well production data using the configured ExternalApiAdapter.

        Args:
            endpoint: The API endpoint to fetch data from.

        Returns:
            A dictionary with "status_code" and "data" containing well productions.
        """
        result_list: List[WellProduction] = await self.adapter.fetch_well_production_data(
            endpoint=endpoint,
            filters=None
        )
        
        return {
            "status_code": 200,
            "data": {"value": [wp.model_dump() for wp in result_list]}
        }

