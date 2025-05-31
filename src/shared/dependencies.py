"""
Dependency injection container for managing service dependencies.
Eliminates tight coupling between layers and enables easy testing.
"""
from typing import Optional, Dict, Any
from functools import lru_cache

from ..domain.repositories.well_production_repository import WellProductionRepository as WellProductionRepositoryPort
from ..domain.ports.external_api_port import ExternalApiPort
from ..application.services.well_production_service import WellProductionService
from ..infrastructure.repositories.composite_well_production_repository import CompositeWellProductionRepository
from ..infrastructure.adapters.external_api_adapter import ExternalApiAdapter
from ..shared.batch_processor import BatchProcessor, BatchConfig


class DependencyContainer:
    """
    Dependency injection container for managing service instances.
    Implements singleton pattern with lazy initialization.
    """
    
    def __init__(self):
        self._instances: Dict[str, Any] = {}
        self._config: Dict[str, Any] = {}
    
    def configure(self, config: Dict[str, Any]) -> None:
        """
        Configure the container with application settings.
        
        Args:
            config: Configuration dictionary
        """
        self._config = config
        # Clear instances to force recreation with new config
        self._instances.clear()
    
    def get_repository(self) -> WellProductionRepositoryPort:
        """Get the well production repository instance"""
        if 'repository' not in self._instances:
            self._instances['repository'] = CompositeWellProductionRepository()
        return self._instances['repository']
    
    def get_external_api(self) -> ExternalApiPort:
        """Get the external API adapter instance"""
        if 'external_api' not in self._instances:
            api_config = self._config.get('external_api', {})
            self._instances['external_api'] = ExternalApiAdapter(
                base_url=api_config.get('base_url'),
                api_key=api_config.get('api_key'),
                mock_mode=api_config.get('mock_mode', True),
                mock_file_path=api_config.get('mock_file_path'),
                timeout_seconds=api_config.get('timeout_seconds', 30),
                max_retries=api_config.get('max_retries', 3),
                retry_delay_seconds=api_config.get('retry_delay_seconds', 1.0)
            )
        return self._instances['external_api']
    
    def get_batch_processor(self) -> BatchProcessor:
        """Get the batch processor instance"""
        if 'batch_processor' not in self._instances:
            batch_config = self._config.get('batch_processing', {})
            config = BatchConfig(
                batch_size=batch_config.get('batch_size', 10000),
                max_memory_mb=batch_config.get('max_memory_mb', 4096.0),
                max_concurrent_batches=batch_config.get('max_concurrent_batches', 4),
                retry_attempts=batch_config.get('retry_attempts', 3),
                retry_delay_seconds=batch_config.get('retry_delay_seconds', 2.0),
                enable_memory_monitoring=batch_config.get('enable_memory_monitoring', True),
                gc_threshold_mb=batch_config.get('gc_threshold_mb', 2048.0)
            )
            self._instances['batch_processor'] = BatchProcessor(config)
        return self._instances['batch_processor']
    
    def get_well_production_service(self) -> WellProductionService:
        """Get the well production service instance"""
        if 'well_production_service' not in self._instances:
            self._instances['well_production_service'] = WellProductionService(
                repository=self.get_repository(),
                external_api=self.get_external_api(),
                batch_processor=self.get_batch_processor()
            )
        return self._instances['well_production_service']
    
    def override_repository(self, repository: WellProductionRepositoryPort) -> None:
        """
        Override the repository instance (useful for testing).
        
        Args:
            repository: Repository instance to use
        """
        self._instances['repository'] = repository
        # Clear dependent services to force recreation
        self._instances.pop('well_production_service', None)
    
    def override_external_api(self, external_api: ExternalApiPort) -> None:
        """
        Override the external API instance (useful for testing).
        
        Args:
            external_api: External API instance to use
        """
        self._instances['external_api'] = external_api
        # Clear dependent services to force recreation
        self._instances.pop('well_production_service', None)
    
    def override_batch_processor(self, batch_processor: BatchProcessor) -> None:
        """
        Override the batch processor instance (useful for testing).
        
        Args:
            batch_processor: Batch processor instance to use
        """
        self._instances['batch_processor'] = batch_processor
        # Clear dependent services to force recreation
        self._instances.pop('well_production_service', None)
    
    def clear(self) -> None:
        """Clear all instances (useful for testing)"""
        self._instances.clear()


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
    return _container


def configure_dependencies(config: Dict[str, Any]) -> None:
    """
    Configure the global dependency container.
    
    Args:
        config: Configuration dictionary
    """
    container = get_container()
    container.configure(config)


# Convenience functions for FastAPI dependency injection
def get_repository() -> WellProductionRepositoryPort:
    """FastAPI dependency for repository"""
    return get_container().get_repository()


def get_external_api() -> ExternalApiPort:
    """FastAPI dependency for external API"""
    return get_container().get_external_api()


def get_batch_processor() -> BatchProcessor:
    """FastAPI dependency for batch processor"""
    return get_container().get_batch_processor()


def get_well_production_service() -> WellProductionService:
    """FastAPI dependency for well production service"""
    return get_container().get_well_production_service() 