from src.application.services.well_production_import_service import WellProductionImportService
from src.infrastructure.adapters.external_api_adapter import ExternalApiAdapter
from src.infrastructure.repositories.duckdb_well_production_repository import DuckDBWellProductionRepository
from src.shared.job_manager import JobManager

# Create singleton instances
job_manager = JobManager()
external_api = ExternalApiAdapter()

def get_repository():
    """Get repository instance for dependency injection."""
    repository = DuckDBWellProductionRepository()
    return repository

def provide_well_production_import_service() -> WellProductionImportService:
    """Provide a WellProductionImportService instance"""
    return WellProductionImportService(
        external_api=external_api,
        repository=get_repository(),
        job_manager=job_manager
    ) 