# src/api/generic_dataset_router.py
from fastapi import APIRouter, Body, Query, HTTPException, Depends
from fastapi.responses import FileResponse
from src.config.datasets_config import get_dataset_config
from src.service.base import GenericDatasetService
from src.infra.duckdb_repo import DuckDBWellRepo
from src.domain.ports import WellRepoPort
from src.api.response_utils import APIResponse
import os
import duckdb
import pandas as pd


router = APIRouter()

# Dependency provider for service using Depends and ports
def get_service(dataset_name: str) -> WellRepoPort:
    config = get_dataset_config(dataset_name)
    repo_cls = config.get("repo", DuckDBWellRepo)
    repo = repo_cls("data/db.duckdb", config["sql_path"])
    service_cls = config.get("service", GenericDatasetService)
    # Service must implement WellRepoPort
    service = service_cls(schema=config["schema"],
                          repo=repo,
                          table=config["table"]
                          )
    return service

@router.post("/{dataset_name}/ingest")
def ingest(
    dataset_name: str,
    data: list[dict] = Body(...),
    service: WellRepoPort = Depends(get_service)
):
    # Use schema from config, not from service interface
    config = get_dataset_config(dataset_name)
    schema = config["schema"]
    parsed = [schema(**item) for item in data]
    inserted = service.insert_many(parsed)
    return {"inserted": inserted}

@router.get("/{dataset_name}/search")
def search(
    dataset_name: str,
    name: str = Query(...),
    service: WellRepoPort = Depends(get_service)
):
    df = service.search_by_name(name)
    return df.to_dict(orient="records")

@router.get("/{dataset_name}/get")
def get(
    dataset_name: str,
    code: str,
    period: str,
    service: WellRepoPort = Depends(get_service)
):
    df = service.get_by_code_and_period(code, period)
    return df.to_dict(orient="records")

@router.get("/{dataset_name}/fetch")
async def fetch(dataset_name: str):
    config = get_dataset_config(dataset_name)
    fetcher = config.get("fetcher")
    exporter = config.get("exporter")
    export_path = config.get("export_path", f"temp/{dataset_name}.csv")

    if not fetcher or not exporter:
        return APIResponse.bad_request("Fetch/export not supported for this dataset.")

    try:
        # Fetch data (should be async)
        data = await fetcher()
        # Export data to file
        exporter.export(data, export_path)

        # Save data directly to DuckDB from memory
        table_name = config.get("table", dataset_name)
        db_path = config.get("db_path", "data/db.duckdb")
        con = duckdb.connect(db_path)

        # Ensure data is a list of dicts (not list of tuples)
        if data and isinstance(data[0], tuple):
            data = [dict(item) for item in data]
        elif data and hasattr(data[0], '__dict__'):
            data = [vars(item) for item in data]
        df = pd.DataFrame(data)
        print('\n\n df', df.head())

        con.register('df_view', df)

        # Check if table exists, create if not
        result = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE lower(table_name) = lower('{table_name}')").fetchone()
        table_exists = result[0] if result else 0
        if not table_exists:
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_view")
        else:
            insert_sql = f'''
                INSERT INTO {table_name}
                SELECT * FROM df_view v
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} t
                    WHERE t.well_code = v.well_code
                      AND t.field_code = v.field_code
                      AND t.production_period = v.production_period
                )
            '''
            con.execute(insert_sql)
        con.unregister('df_view')
        con.close()

        # Return JSON response with download URL and message
        if os.path.exists(export_path):
            download_url = f"/api/{dataset_name}/download"  # You can adjust this path as needed
            return APIResponse.success(
                data={
                    "download_url": download_url,
                    "filename": os.path.basename(export_path)
                },
                message="Data fetched and stored successfully."
            )
        else:
            return APIResponse.error("Export failed: file not found.")
    except Exception as e:
        return APIResponse.error(f"Error fetching/exporting data: {str(e)}")

@router.get("/{dataset_name}/download")
def download(dataset_name: str):
    config = get_dataset_config(dataset_name)
    table_name = config.get("table", dataset_name)
    db_path = config.get("db_path", "data/db.duckdb")
    export_path = config.get("export_path", f"temp/{dataset_name}.csv")

    try:
        con = duckdb.connect(db_path)
        # Query all data from the table
        df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
        con.close()
        # Export to CSV
        df.to_csv(export_path, index=False)
        if os.path.exists(export_path):
            return FileResponse(export_path, filename=os.path.basename(export_path))
        else:
            return APIResponse.error("Export failed: file not found.")
    except Exception as e:
        return APIResponse.error(f"Error exporting data: {str(e)}")
