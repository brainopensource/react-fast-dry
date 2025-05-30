### api/wells_router.py
from fastapi import APIRouter, Query, Body, HTTPException, Depends
from ..domain.models import WellProductionExternal, ODataResponse
from ..service.wells_service import WellService
from ..service.external_api_service import ExternalApiService
from ..infra.duckdb_repo import DuckDBWellRepo
from src.infra.pandas_csv_exporter import PandasCsvExporter
from src.domain.ports import CsvExporterPort
from ..config.settings import Settings, get_settings

router = APIRouter()

def get_repository(settings: Settings = Depends(get_settings)):
    return DuckDBWellRepo(
        str(settings.DB_PATH),
        str(settings.WELLS_SQL_PATH)
    )

def get_service(repo = Depends(get_repository)):
    return WellService(repo)

# Initialize non-dependency services at module level
external_api_service = ExternalApiService(mock_mode=True)
csv_exporter = PandasCsvExporter()

@router.post("/ingest")
def ingest(
    data: list[WellProductionExternal] = Body(...),
    service: WellService = Depends(get_service)
):
    service.insert(data)
    return {"inserted": len(data)}

@router.get("/search")
def search(
    name: str = Query(...),
    service: WellService = Depends(get_service)
):
    return service.search(name).to_dict(orient="records")

@router.get("/get")
def get(
    code: str, 
    period: str,
    service: WellService = Depends(get_service)
):
    return service.get(code, period).to_dict(orient="records")

@router.get("/fetch")
async def fetch_wells_production(settings: Settings = Depends(get_settings)):
    """
    Fetch well production data from external OData API and optionally store in database
    Returns the fetched data with status information
    """
    import json
    exporter = csv_exporter  # Use the module-level exporter

    try:
        # Fetch data from external API (mocked)
        response = await external_api_service.fetch_well_production_data()
        
        if response["status_code"] != 200:
            raise HTTPException(status_code=response["status_code"], detail="External API error")

        # Parse the OData response
        odata_response = ODataResponse(**response["data"]).value

        print('Print Odata response', odata_response)   # Debugging line to check the fetched data  

        response = {
            "status_code": 200,
            "data": [item.model_dump() for item in odata_response]
        }
        exporter.export(odata_response, str(settings.WELLS_EXPORT_PATH))
        return {
            "status": "success",
            "status_code": response["status_code"],
            "fetched_records": len(odata_response),
            "data": [item.model_dump() for item in odata_response]
            # "inserted_records": inserted_count  # Uncomment if auto-inserting
        }

    except Exception as e:
        raise HTTPException(status_code=500,
                             detail=f"Error fetching well production data: {str(e)}")
