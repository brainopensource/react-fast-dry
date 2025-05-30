### api/wells_router.py
from fastapi import APIRouter, Query, Body
from ..domain.models import WellProduction
from ..service.wells_service import WellService
from ..infra.duckdb_repo import DuckDBWellRepo

router = APIRouter()
repo = DuckDBWellRepo("data/db.duckdb", "src/sql/wells.sql")
service = WellService(repo)

@router.post("/ingest")
def ingest(data: list[WellProduction] = Body(...)):
    service.insert(data)
    return {"inserted": len(data)}

@router.get("/search")
def search(name: str = Query(...)):
    return service.search(name).to_dict(orient="records")

@router.get("/get")
def get(code: str, period: str):
    return service.get(code, period).to_dict(orient="records")
