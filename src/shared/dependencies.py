"""
Dependency injection container for managing service dependencies.
Eliminates tight coupling between layers and enables easy testing.
"""
from typing import Optional, Dict, Any
from functools import lru_cache
from fastapi import Depends
from pathlib import Path # Added for Path object usage

from ..domain.repositories.well_production_repository import WellProductionRepository as WellProductionRepositoryPort
# Renamed WellProductionRepository to WellProductionRepositoryPort for clarity if it's a port/interface
# from ..domain.repositories.ports import WellProductionRepository # As per suggestion, but using existing name
from ..domain.ports.external_api_port import ExternalApiPort

# Service imports
from ..application.services.well_production_service import WellProductionService # For Data Quality
from ..application.services.well_production_import_service import WellProductionImportService
from ..application.services.well_production_query_service import WellProductionQueryService
from ..application.services.odata_well_production_import_service import ODataWellProductionImportService

# Infrastructure imports
from ..infrastructure.repositories.duckdb_well_production_repository import DuckDBWellProductionRepository
from ..infrastructure.adapters.external_api_adapter import ExternalApiAdapter
from ..infrastructure.adapters.odata_external_api_adapter import ODataExternalApiAdapter
from ..domain.ports.odata_external_api_port import ODataExternalApiPort
# from ..infrastructure.db.duckdb_repo import DuckDBWellRepo # Example, not used yet

# Shared utilities
from ..shared.batch_processor import BatchProcessor, BatchConfig
from ..shared.job_manager import JobManager
from .config.settings import get_settings


class DependencyContainer:
    """
    Dependency injection container for managing service instances.
    Implements singleton pattern with lazy initialization.
    """
    
    def __init__(self):
        self._instances: Dict[str, Any] = {}
        self._config: Dict[str, Any] = {} # This will hold settings loaded from elsewhere
    
    def configure(self, config: Dict[str, Any]) -> None:
        """
        Configure the container with application settings.
        
        Args:
            config: Configuration dictionary (e.g., from a settings file)
        """
        self._config = config
        # Clear instances to force recreation with new config
        self._instances.clear()

    def get_batch_config_instance(self) -> BatchConfig:
        """Get the BatchConfig instance from container configuration."""
        if 'batch_config' not in self._instances:
            # Assuming batch_processing settings are under a 'batch_processing' key in the main config
            cfg = self._config.get('batch_processing', {})
            self._instances['batch_config'] = BatchConfig(
                batch_size=cfg.get('batch_size', 10000),
                max_memory_mb=cfg.get('max_memory_mb', 4096.0),
                max_concurrent_batches=cfg.get('max_concurrent_batches', 4),
                retry_attempts=cfg.get('retry_attempts', 3),
                retry_delay_seconds=cfg.get('retry_delay_seconds', 2.0),
                enable_memory_monitoring=cfg.get('enable_memory_monitoring', True),
                gc_threshold_mb=cfg.get('gc_threshold_mb', 2048.0)
            )
        return self._instances['batch_config']

    def get_repository(self) -> WellProductionRepositoryPort: # Renamed for clarity
        """Get the well production repository instance"""
        if 'repository' not in self._instances:
            repo_paths_config = self._config.get('repository_paths', {})
            data_dir = Path(repo_paths_config.get('data_dir', 'data')) # Default to 'data' if not configured
            downloads_dir = Path(repo_paths_config.get('downloads_dir', 'downloads')) # Add downloads_dir
            duckdb_filename = repo_paths_config.get('duckdb_filename', 'wells_production.duckdb')
            csv_filename = repo_paths_config.get('csv_filename', 'wells_prod.csv')

            # Use DuckDBWellProductionRepository instead of CompositeWellProductionRepository
            # This provides fast DuckDB operations for imports and on-demand CSV export
            self._instances['repository'] = DuckDBWellProductionRepository(
                db_path=data_dir / duckdb_filename,
                downloads_dir=downloads_dir,
                csv_filename=csv_filename
            )
        return self._instances['repository']
    
    def get_external_api_adapter(self) -> ExternalApiPort: # Renamed for clarity
        """Get the external API adapter instance"""
        if 'external_api_adapter' not in self._instances:
            api_config = self._config.get('external_api', {})
            # Ensuring mock_mode has a production-sensible default if not set by app_config
            # This requires settings to be accessible, or pass ENV through config
            # For now, relying on main.py to set it based on settings.ENV
            mock_mode_default = self._config.get('env', 'development') != "production"

            self._instances['external_api_adapter'] = ExternalApiAdapter(
                base_url=api_config.get('base_url'),
                api_key=api_config.get('api_key'),
                mock_mode=api_config.get('mock_mode', mock_mode_default), # Use resolved default
                mock_file_path=api_config.get('mock_file_path'),
                timeout_seconds=api_config.get('timeout_seconds', 30),
                max_retries=api_config.get('max_retries', 3),
                retry_delay_seconds=api_config.get('retry_delay_seconds', 1.0)
            )
        return self._instances['external_api_adapter']
    
    def get_batch_processor_instance(self) -> BatchProcessor: # Renamed for clarity
        """Get the batch processor instance"""
        if 'batch_processor_instance' not in self._instances:
            self._instances['batch_processor_instance'] = BatchProcessor(
                config=self.get_batch_config_instance()
            )
        return self._instances['batch_processor_instance']

    def get_job_manager(self) -> JobManager:
        """Get the job manager instance"""
        if 'job_manager' not in self._instances:
            self._instances['job_manager'] = JobManager()
        return self._instances['job_manager']

    # --- Service Getters ---
    def get_well_production_data_quality_service(self) -> WellProductionService:
        """Get the WellProductionService instance (for data quality)."""
        if 'well_production_data_quality_service' not in self._instances:
            self._instances['well_production_data_quality_service'] = WellProductionService(
                repository=self.get_repository()
            )
        return self._instances['well_production_data_quality_service']

    def get_well_production_import_service_instance(self) -> WellProductionImportService:
        """Get the WellProductionImportService instance."""
        if 'well_production_import_service_instance' not in self._instances:
            self._instances['well_production_import_service_instance'] = WellProductionImportService(
                repository=self.get_repository(),
                external_api=self.get_external_api_adapter(),
                batch_processor=self.get_batch_processor_instance(),
                job_manager=self.get_job_manager()
            )
        return self._instances['well_production_import_service_instance']

    def get_well_production_query_service_instance(self) -> WellProductionQueryService:
        """Get the WellProductionQueryService instance."""
        if 'well_production_query_service_instance' not in self._instances:
            self._instances['well_production_query_service_instance'] = WellProductionQueryService(
                repository=self.get_repository(),
                external_api=self.get_external_api_adapter()
            )
        return self._instances['well_production_query_service_instance']

    def get_odata_external_api_adapter(self) -> ODataExternalApiPort:
        """Get the OData external API adapter instance"""
        if 'odata_external_api_adapter' not in self._instances:
            # Use settings instead of hardcoded values
            settings = get_settings()
            self._instances['odata_external_api_adapter'] = ODataExternalApiAdapter(
                base_url=settings.ODATA_BASE_URL,
                username=settings.ODATA_USERNAME,
                password=settings.ODATA_PASSWORD,
                timeout_seconds=settings.ODATA_TIMEOUT_SECONDS,
                max_retries=settings.ODATA_MAX_RETRIES,
                retry_delay_seconds=settings.ODATA_RETRY_DELAY_SECONDS,
                max_records_per_request=settings.ODATA_MAX_RECORDS_PER_REQUEST
            )
        return self._instances['odata_external_api_adapter']

    def get_odata_well_production_import_service_instance(self) -> ODataWellProductionImportService:
        """Get the OData WellProductionImportService instance."""
        if 'odata_well_production_import_service_instance' not in self._instances:
            self._instances['odata_well_production_import_service_instance'] = ODataWellProductionImportService(
                odata_api_adapter=self.get_odata_external_api_adapter(),
                repository=self.get_repository(),
                job_manager=self.get_job_manager()
            )
        return self._instances['odata_well_production_import_service_instance']

    # --- Override methods for testing ---
    def override_repository(self, repository: WellProductionRepositoryPort) -> None:
        """
        Override the repository instance (useful for testing).
        
        Args:
            repository: Repository instance to use
        """
        self._instances['repository'] = repository
        # Clear dependent services to force recreation
        self._instances.pop('well_production_data_quality_service', None)
        self._instances.pop('well_production_import_service_instance', None)
        self._instances.pop('well_production_query_service_instance', None)
    
    def override_external_api_adapter(self, external_api: ExternalApiPort) -> None: # Renamed
        """
        Override the external API adapter instance (useful for testing).
        
        Args:
            external_api: External API instance to use
        """
        self._instances['external_api_adapter'] = external_api
        # Clear dependent services to force recreation
        self._instances.pop('well_production_import_service_instance', None)
        self._instances.pop('well_production_query_service_instance', None)

    def override_batch_config(self, batch_config: BatchConfig) -> None:
        """Override the batch config instance (useful for testing)."""
        self._instances['batch_config'] = batch_config
        self._instances.pop('batch_processor_instance', None) # batch_processor depends on batch_config
        self._instances.pop('well_production_import_service_instance', None)


    def override_batch_processor(self, batch_processor: BatchProcessor) -> None:
        """
        Override the batch processor instance (useful for testing).
        
        Args:
            batch_processor: Batch processor instance to use
        """
        self._instances['batch_processor_instance'] = batch_processor
        # Clear dependent services to force recreation
        self._instances.pop('well_production_import_service_instance', None)
    
    def clear(self) -> None:
        """Clear all instances (useful for testing)"""
        self._instances.clear()

    def override_odata_external_api_adapter(self, odata_api: ODataExternalApiPort) -> None:
        """
        Override the OData external API adapter instance (useful for testing).
        
        Args:
            odata_api: OData External API instance to use
        """
        self._instances['odata_external_api_adapter'] = odata_api
        # Clear dependent services to force recreation
        self._instances.pop('odata_well_production_import_service_instance', None)


# Global container instance
_container: Optional[DependencyContainer] = None


def get_container() -> DependencyContainer:
    """
    Get the global dependency container instance.
    
    Returns:
        DependencyContainer instance
    """
    global _container
    if _container is None:
        _container = DependencyContainer()
        # TODO: Configuration should be loaded here or explicitly passed to configure_dependencies
        # For now, expecting configure_dependencies to be called by application startup.
        # Example:
        # from .config.settings import load_app_settings
        # _container.configure(load_app_settings().model_dump())
    return _container


def configure_dependencies(config: Dict[str, Any]) -> None:
    """
    Configure the global dependency container.
    Call this at application startup.
    
    Args:
        config: Configuration dictionary
    """
    get_container().configure(config) # Ensures container is created before configuration


# FastAPI Dependency Providers using the global container

# Infrastructure Providers
def provide_well_production_repository() -> WellProductionRepositoryPort:
    """FastAPI dependency for WellProductionRepositoryPort."""
    return get_container().get_repository()

def provide_external_api_adapter() -> ExternalApiPort:
    """FastAPI dependency for ExternalApiPort."""
    return get_container().get_external_api_adapter()

def _get_batch_config_from_container() -> BatchConfig: # Underscore to indicate it's for internal use by provider
    """Helper to get BatchConfig from the container for the provider."""
    return get_container().get_batch_config_instance()

def provide_batch_config(config: BatchConfig = Depends(_get_batch_config_from_container)) -> BatchConfig:
    """FastAPI dependency for BatchConfig."""
    return config

def _get_batch_processor_from_container() -> BatchProcessor: # Underscore
    """Helper to get BatchProcessor from the container for the provider."""
    return get_container().get_batch_processor_instance()

def provide_batch_processor(processor: BatchProcessor = Depends(_get_batch_processor_from_container)) -> BatchProcessor:
    """FastAPI dependency for BatchProcessor."""
    return processor

# Service Providers
def _get_well_production_data_quality_service_from_container() -> WellProductionService: # Underscore
    return get_container().get_well_production_data_quality_service()

def provide_well_production_data_quality_service(
    service: WellProductionService = Depends(_get_well_production_data_quality_service_from_container)
) -> WellProductionService:
    """FastAPI dependency for WellProductionService (Data Quality)."""
    return service

def _get_well_production_import_service_from_container() -> WellProductionImportService: # Underscore
    return get_container().get_well_production_import_service_instance()

def provide_well_production_import_service(
    service: WellProductionImportService = Depends(_get_well_production_import_service_from_container)
) -> WellProductionImportService:
    """FastAPI dependency for WellProductionImportService."""
    return service

def _get_well_production_query_service_from_container() -> WellProductionQueryService: # Underscore
    return get_container().get_well_production_query_service_instance()

def provide_well_production_query_service(
    service: WellProductionQueryService = Depends(_get_well_production_query_service_from_container)
) -> WellProductionQueryService:
    """FastAPI dependency for WellProductionQueryService."""
    return service

# OData Service Providers
def _get_odata_external_api_adapter_from_container() -> ODataExternalApiPort:
    """Helper to get ODataExternalApiAdapter from the container for the provider."""
    return get_container().get_odata_external_api_adapter()

def provide_odata_external_api_adapter(
    adapter: ODataExternalApiPort = Depends(_get_odata_external_api_adapter_from_container)
) -> ODataExternalApiPort:
    """FastAPI dependency for ODataExternalApiAdapter."""
    return adapter

def _get_odata_well_production_import_service_from_container() -> ODataWellProductionImportService:
    """Helper to get ODataWellProductionImportService from the container for the provider."""
    return get_container().get_odata_well_production_import_service_instance()

def provide_odata_well_production_import_service(
    service: ODataWellProductionImportService = Depends(_get_odata_well_production_import_service_from_container)
) -> ODataWellProductionImportService:
    """FastAPI dependency for ODataWellProductionImportService."""
    return service
