# src/api/generic_dataset_router.py
from fastapi import APIRouter, Body, Query, HTTPException
from fastapi.responses import FileResponse
from src.config.datasets_config import get_dataset_config
from src.service.base import GenericDatasetService
from src.repo.base import DuckDBRepo
import os

router = APIRouter()

# Dependency to get the service for a dataset

def get_dataset_service(dataset_name: str):
    config = get_dataset_config(dataset_name)
    repo_cls = config.get("repo", DuckDBRepo)
    repo = repo_cls("data/db.duckdb", config["sql_path"])
    service_cls = config.get("service", GenericDatasetService)
    return service_cls(
        schema=config["schema"],
        repo=repo,
        table=config["table"]
    )

@router.post("/{dataset_name}/ingest")
def ingest(dataset_name: str, data: list[dict] = Body(...)):
    service = get_dataset_service(dataset_name)
    schema = service.schema
    parsed = [schema(**item) for item in data]
    inserted = service.insert_many(parsed)
    return {"inserted": inserted}

@router.get("/{dataset_name}/search")
def search(dataset_name: str, name: str = Query(...)):
    service = get_dataset_service(dataset_name)
    df = service.search_by_name(name)
    return df.to_dict(orient="records")

@router.get("/{dataset_name}/get")
def get(dataset_name: str, code: str, period: str):
    service = get_dataset_service(dataset_name)
    df = service.get_by_code_and_period(code, period)
    return df.to_dict(orient="records")

@router.get("/{dataset_name}/fetch")
async def fetch(dataset_name: str):
    config = get_dataset_config(dataset_name)
    fetcher = config.get("fetcher")
    exporter = config.get("exporter")
    export_path = config.get("export_path", f"temp/{dataset_name}.csv")

    if not fetcher or not exporter:
        raise HTTPException(status_code=400, detail="Fetch/export not supported for this dataset.")

    try:
        # Fetch data (should be async)
        data = await fetcher()
        # Export data to file
        exporter.export(data, export_path)
        # Return file for download
        if os.path.exists(export_path):
            return FileResponse(export_path, filename=os.path.basename(export_path))
        else:
            raise HTTPException(status_code=500, detail="Export failed: file not found.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching/exporting data: {str(e)}")
