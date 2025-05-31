# src/application/services/fetchers.py
from src.application.services.external_api_service import ExternalApiService
from pydantic import BaseModel
from typing import List, Dict, Any

# Define a simple ODataResponse model locally for parsing the 'value' field.
# This ensures fetchers.py can work with minimal changes as ExternalApiService
# now returns data in the format: {"value": [list_of_dictionaries]}
class ODataResponse(BaseModel):
    value: List[Dict[str, Any]] # Expects a list of dictionaries

async def fetch_well_production_data_then_parse() -> List[Dict[str, Any]]:
    """
    Fetches well production data using ExternalApiService and parses the "value" field.
    The ExternalApiService instance will use the dependency-injected ExternalApiAdapter.
    """
    # ExternalApiService() will use the DI-configured adapter.
    # Any arguments passed to ExternalApiService() constructor here (like mock_mode)
    # would be logged with a warning if they differ from the adapter's actual configuration.
    service = ExternalApiService()
    response = await service.fetch_well_production_data()

    if response["status_code"] != 200:
        # TODO: Consider more specific error handling or logging
        raise Exception(f"External API error: Status Code {response['status_code']}")

    # response["data"] is expected to be a dict like {"value": [...]}
    # ODataResponse will parse this and extract the list in the "value" field.
    # The elements in the list are dicts (from WellProduction.model_dump()).
    parsed_value: List[Dict[str, Any]] = ODataResponse(**response["data"]).value

    return parsed_value
