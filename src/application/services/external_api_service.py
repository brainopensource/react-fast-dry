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
    Compatibility wrapper for ExternalApiAdapter.
    This service now delegates calls to the ExternalApiAdapter obtained from the DI container.
    It transforms the adapter's output to the dictionary structure previously expected by fetchers.py.
    """
    
    def __init__(self, mock_mode: bool = True, mock_file_path: Optional[str] = None, 
                 base_url: Optional[str] = None, timeout: int = 30):
        """
        Initializes ExternalApiService.
        Note: Constructor arguments (mock_mode, mock_file_path, base_url, timeout) are
        largely ignored if the ExternalApiAdapter is already configured via DI.
        The adapter instance is fetched from the global dependency container.
        """
        self.adapter = get_container().get_external_api_adapter()

        if base_url:
            logger.warning(
                "ExternalApiService received 'base_url' in constructor. "
                "This is likely ignored as ExternalApiAdapter is configured via DI."
            )
        if mock_mode != self.adapter.mock_mode: # Compare with adapter's actual mock_mode
             logger.warning(
                f"ExternalApiService initialized with mock_mode={mock_mode}, "
                f"but the underlying adapter's mock_mode is {self.adapter.mock_mode}. "
                "The adapter's configuration will be used."
            )

    async def fetch_well_production_data(self, endpoint: str = "/wells_production") -> Dict[str, Any]:
        """
        Fetches well production data using the configured ExternalApiAdapter and
        transforms the result to the legacy dictionary format.

        Args:
            endpoint: The API endpoint to fetch data from.

        Returns:
            A dictionary with "status_code" and "data" (containing a "value" list of well productions).
        """
        # filters=None is passed as per instruction, adjust if filters are needed
        result_list: List[WellProduction] = await self.adapter.fetch_well_production_data(
            endpoint=endpoint,
            filters=None
        )
        
        # Transform List[WellProduction] to the expected OData-like dictionary structure
        # Pydantic's model_dump() is used here as WellProduction is a Pydantic model
        transformed_data = {"value": [wp.model_dump() for wp in result_list]}
        
        return {
            "status_code": 200, # Assuming success if adapter doesn't raise an exception
            "data": transformed_data
        }

    # The transform_external_to_internal method might no longer be needed here
    # as the adapter handles data fetching and parsing into WellProduction entities.
    # If fetchers.py or other callers still need this exact method signature or functionality,
    # it would need to be adapted or potentially removed if its role is now redundant.
    # For now, commenting it out as its direct utility is unclear with the new adapter flow.
    # def transform_external_to_internal(self, external_data: list[Any]) -> list[Any]:
    #     logger.warning("transform_external_to_internal called on compatibility wrapper, may be redundant.")
    #     return external_data
