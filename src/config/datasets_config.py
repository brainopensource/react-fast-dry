# src/config/datasets_config.py
from src.domain.models import WellProductionExternal
from src.infra.duckdb_repo import DuckDBWellRepo
from src.service.wells_service import WellService
from src.service.external_api_service import ExternalApiService
from src.infra.pandas_csv_exporter import PandasCsvExporter

async def fetch_well_production_data_then_parse():
    response = await ExternalApiService(mock_mode=True).fetch_well_production_data()
    if response["status_code"] != 200:
        raise Exception("External API error")
    from src.domain.models import ODataResponse
    odata_response = ODataResponse(**response["data"]).value
    return odata_response

DATASETS = {
    "wells_production": {
        "schema": WellProductionExternal,
        "table": "wells_production",
        "sql_path": "sql/wells.sql",
        "service": WellService,  # Optional: for custom logic
        "repo": DuckDBWellRepo,  # Optional: for custom repo
        "fetcher": fetch_well_production_data_then_parse,
        "exporter": PandasCsvExporter(),
        "export_path": "temp/wells_production.csv",
    },
    # Add more datasets here
}

def get_dataset_config(dataset_name: str):
    config = DATASETS.get(dataset_name)
    if not config:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Dataset not found")
    return config
