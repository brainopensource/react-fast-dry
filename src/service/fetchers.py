# src/service/fetchers.py
from src.service.external_api_service import ExternalApiService
from src.domain.models import ODataResponse

async def fetch_well_production_data_then_parse():
    response = await ExternalApiService(mock_mode=True).fetch_well_production_data()
    if response["status_code"] != 200:
        raise Exception("External API error")
    odata_response = ODataResponse(**response["data"]).value
    return odata_response
