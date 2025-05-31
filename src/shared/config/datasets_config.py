# src/config/datasets_config.py
# Corrected import paths based on project structure
from src.domain.entities.well_production import WellProduction # Assuming WellProduction is the correct entity
# from src.domain.models import WellProductionExternal # Original, if WellProductionExternal is a distinct model
from src.infrastructure.db.duckdb_repo import DuckDBWellRepo # Corrected path
from src.application.services.wells_service import WellService # Corrected path
from src.infrastructure.external.pandas_csv_exporter import PandasCsvExporter # Corrected path
from src.application.services.fetchers import fetch_well_production_data_then_parse # Corrected path

DATASETS = {
    "wells_production": {
        "schema": WellProduction, # Changed to WellProduction, adjust if WellProductionExternal is intended and different
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
