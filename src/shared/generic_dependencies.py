"""
Generic dependency injection system for any dataset.
Follows SOLID principles and implements the Factory pattern for DRY architecture.
"""

import logging
from typing import Dict, Any, Optional, Callable
from functools import lru_cache
from pathlib import Path

from .config.schemas import (
    get_dataset_config, get_available_datasets, DatasetSchemaConfig
)
from .config.settings import get_settings
from ..application.services.generic_data_service import GenericDataService
from ..infrastructure.repositories.generic_duckdb_repository import GenericDuckDBRepository
from ..domain.ports.repository import IFullRepository
from ..domain.ports.services import IDataManagementService, IExternalDataService

logger = logging.getLogger(__name__)

# Global configuration and registries
_config: Dict[str, Any] = {}
_repository_registry: Dict[str, IFullRepository] = {}
_service_registry: Dict[str, IDataManagementService] = {}
_external_service_registry: Dict[str, IExternalDataService] = {}

class DependencyContainer:
    """
    Dependency injection container for generic datasets.
    Follows the Container pattern for managing dependencies.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the container with configuration."""
        self.config = config
        self.settings = get_settings()
        self.logger = logging.getLogger(__name__)
        
        # Initialize component registries
        self._repositories: Dict[str, IFullRepository] = {}
        self._services: Dict[str, IDataManagementService] = {}
        self._external_services: Dict[str, IExternalDataService] = {}
        
        # Component factories
        self._repository_factories: Dict[str, Callable] = {}
        self._service_factories: Dict[str, Callable] = {}
        self._external_service_factories: Dict[str, Callable] = {}
        
        self._initialize_factories()
    
    def _initialize_factories(self):
        """Initialize component factories."""
        try:
            # Register default repository factory
            self._repository_factories['duckdb'] = self._create_duckdb_repository
            
            # Register default service factory
            self._service_factories['generic'] = self._create_generic_service
            
            # Register external service factories based on configuration
            if 'external_api' in self.config:
                external_config = self.config['external_api']
                if external_config.get('mock_mode', False):
                    self._external_service_factories['mock'] = self._create_mock_external_service
                else:
                    self._external_service_factories['odata'] = self._create_odata_external_service
            
            self.logger.info("Dependency factories initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize dependency factories: {str(e)}")
            raise
    
    def get_repository(self, dataset_name: str) -> IFullRepository:
        """Get repository for dataset."""
        if dataset_name not in self._repositories:
            self._repositories[dataset_name] = self._create_repository(dataset_name)
        return self._repositories[dataset_name]
    
    def get_service(self, dataset_name: str) -> IDataManagementService:
        """Get service for dataset."""
        if dataset_name not in self._services:
            self._services[dataset_name] = self._create_service(dataset_name)
        return self._services[dataset_name]
    
    def get_external_service(self, dataset_name: str) -> Optional[IExternalDataService]:
        """Get external service for dataset."""
        if dataset_name not in self._external_services:
            service = self._create_external_service(dataset_name)
            if service:
                self._external_services[dataset_name] = service
        return self._external_services.get(dataset_name)
    
    def _create_repository(self, dataset_name: str) -> IFullRepository:
        """Create repository for dataset."""
        try:
            config = get_dataset_config(dataset_name)
            
            # Use DuckDB as default repository
            repository_type = 'duckdb'
            factory = self._repository_factories.get(repository_type)
            
            if not factory:
                raise ValueError(f"No repository factory found for type: {repository_type}")
            
            repository = factory(dataset_name, config)
            self.logger.info(f"Created {repository_type} repository for {dataset_name}")
            
            return repository
            
        except Exception as e:
            self.logger.error(f"Failed to create repository for {dataset_name}: {str(e)}")
            raise
    
    def _create_service(self, dataset_name: str) -> IDataManagementService:
        """Create service for dataset."""
        try:
            config = get_dataset_config(dataset_name)
            
            # Get dependencies
            repository = self.get_repository(dataset_name)
            external_service = self.get_external_service(dataset_name)
            
            # Use generic service as default
            service_type = 'generic'
            factory = self._service_factories.get(service_type)
            
            if not factory:
                raise ValueError(f"No service factory found for type: {service_type}")
            
            service = factory(dataset_name, config, repository, external_service)
            self.logger.info(f"Created {service_type} service for {dataset_name}")
            
            return service
            
        except Exception as e:
            self.logger.error(f"Failed to create service for {dataset_name}: {str(e)}")
            raise
    
    def _create_external_service(self, dataset_name: str) -> Optional[IExternalDataService]:
        """Create external service for dataset."""
        try:
            if 'external_api' not in self.config:
                return None
            
            external_config = self.config['external_api']
            
            if external_config.get('mock_mode', False):
                service_type = 'mock'
            else:
                service_type = 'odata'
            
            factory = self._external_service_factories.get(service_type)
            if not factory:
                self.logger.warning(f"No external service factory found for type: {service_type}")
                return None
            
            service = factory(dataset_name, external_config)
            self.logger.info(f"Created {service_type} external service for {dataset_name}")
            
            return service
            
        except Exception as e:
            self.logger.error(f"Failed to create external service for {dataset_name}: {str(e)}")
            return None
    
    # Factory methods for different components
    def _create_duckdb_repository(self, dataset_name: str, config: DatasetSchemaConfig) -> IFullRepository:
        """Create DuckDB repository."""
        db_path = str(self.settings.DB_PATH)
        return GenericDuckDBRepository(dataset_name, db_path, config)
    
    def _create_generic_service(
        self, 
        dataset_name: str, 
        config: DatasetSchemaConfig, 
        repository: IFullRepository,
        external_service: Optional[IExternalDataService]
    ) -> IDataManagementService:
        """Create generic data service."""
        return GenericDataService(dataset_name, repository, external_service, config)
    
    def _create_mock_external_service(self, dataset_name: str, external_config: Dict[str, Any]) -> IExternalDataService:
        """Create mock external service."""
        # Import here to avoid circular dependencies
        from ..infrastructure.adapters.mock_external_service import MockExternalService
        return MockExternalService(dataset_name, external_config)
    
    def _create_odata_external_service(self, dataset_name: str, external_config: Dict[str, Any]) -> IExternalDataService:
        """Create OData external service."""
        # Import here to avoid circular dependencies
        from ..infrastructure.adapters.odata_external_service import ODataExternalService
        return ODataExternalService(dataset_name, external_config)
    
    def register_repository_factory(self, repo_type: str, factory: Callable):
        """Register a custom repository factory."""
        self._repository_factories[repo_type] = factory
        self.logger.info(f"Registered repository factory for type: {repo_type}")
    
    def register_service_factory(self, service_type: str, factory: Callable):
        """Register a custom service factory."""
        self._service_factories[service_type] = factory
        self.logger.info(f"Registered service factory for type: {service_type}")
    
    def register_external_service_factory(self, service_type: str, factory: Callable):
        """Register a custom external service factory."""
        self._external_service_factories[service_type] = factory
        self.logger.info(f"Registered external service factory for type: {service_type}")
    
    def close_all(self):
        """Close all managed resources."""
        # Close repositories
        for repo in self._repositories.values():
            if hasattr(repo, 'close'):
                try:
                    repo.close()
                except Exception as e:
                    self.logger.error(f"Error closing repository: {str(e)}")
        
        # Clear registries
        self._repositories.clear()
        self._services.clear()
        self._external_services.clear()
        
        self.logger.info("All dependencies closed")

# Global container instance
_container: Optional[DependencyContainer] = None

def configure_dependencies(config: Dict[str, Any]):
    """Configure the global dependency container."""
    global _container
    _container = DependencyContainer(config)
    logger.info("Dependencies configured")

def get_container() -> DependencyContainer:
    """Get the global dependency container."""
    if _container is None:
        raise RuntimeError("Dependencies not configured. Call configure_dependencies() first.")
    return _container

# Convenience functions for common dependency injection patterns
@lru_cache(maxsize=32)
def get_repository_for_dataset(dataset_name: str) -> IFullRepository:
    """Get repository for dataset with caching."""
    return get_container().get_repository(dataset_name)

@lru_cache(maxsize=32)
def get_service_for_dataset(dataset_name: str) -> IDataManagementService:
    """Get service for dataset with caching."""
    return get_container().get_service(dataset_name)

def get_external_service_for_dataset(dataset_name: str) -> Optional[IExternalDataService]:
    """Get external service for dataset."""
    return get_container().get_external_service(dataset_name)

# Service factory functions for FastAPI dependency injection
def create_service_factory(dataset_name: str) -> Callable[[], IDataManagementService]:
    """Create a service factory function for a specific dataset."""
    def service_factory() -> IDataManagementService:
        return get_service_for_dataset(dataset_name)
    
    return service_factory

# Registry for all service factories
def get_service_factory_registry() -> Dict[str, Callable[[], IDataManagementService]]:
    """Get registry of all service factories for available datasets."""
    registry = {}
    
    for dataset_name in get_available_datasets():
        registry[dataset_name] = create_service_factory(dataset_name)
    
    return registry

# Context manager for dependency lifecycle
class DependencyContext:
    """Context manager for managing dependency lifecycle."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    def __enter__(self) -> DependencyContainer:
        configure_dependencies(self.config)
        return get_container()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if _container:
            _container.close_all()

# Health check functions
async def check_dependencies_health() -> Dict[str, Any]:
    """Check health of all dependencies."""
    health_status = {
        "dependencies": "healthy",
        "datasets": {},
        "issues": []
    }
    
    try:
        container = get_container()
        
        # Check each dataset
        for dataset_name in get_available_datasets():
            dataset_health = {
                "status": "healthy",
                "repository": "healthy",
                "service": "healthy",
                "external_service": "not_configured"
            }
            
            try:
                # Test repository
                repo = container.get_repository(dataset_name)
                await repo.count()  # Simple health check
                
                # Test service
                service = container.get_service(dataset_name)
                await service.count()  # Simple health check
                
                # Test external service if available
                external_service = container.get_external_service(dataset_name)
                if external_service:
                    if hasattr(external_service, 'test_connection'):
                        if await external_service.test_connection():
                            dataset_health["external_service"] = "healthy"
                        else:
                            dataset_health["external_service"] = "unhealthy"
                    else:
                        dataset_health["external_service"] = "unknown"
                
            except Exception as e:
                dataset_health["status"] = "unhealthy"
                dataset_health["error"] = str(e)
                health_status["issues"].append(f"{dataset_name}: {str(e)}")
            
            health_status["datasets"][dataset_name] = dataset_health
        
        # Overall health
        unhealthy_datasets = [
            name for name, health in health_status["datasets"].items()
            if health["status"] != "healthy"
        ]
        
        if unhealthy_datasets:
            health_status["dependencies"] = "degraded"
            health_status["unhealthy_datasets"] = unhealthy_datasets
        
    except Exception as e:
        health_status["dependencies"] = "unhealthy"
        health_status["error"] = str(e)
        health_status["issues"].append(f"Global error: {str(e)}")
    
    return health_status

# Startup and shutdown hooks
async def startup_dependencies():
    """Initialize dependencies on application startup."""
    try:
        if not _container:
            logger.warning("Dependencies not configured during startup")
            return
        
        logger.info("Starting dependency health checks...")
        health = await check_dependencies_health()
        
        if health["dependencies"] == "healthy":
            logger.info("All dependencies are healthy")
        elif health["dependencies"] == "degraded":
            logger.warning(f"Some dependencies are unhealthy: {health.get('unhealthy_datasets', [])}")
        else:
            logger.error(f"Dependencies are unhealthy: {health.get('error', 'Unknown error')}")
        
    except Exception as e:
        logger.error(f"Error during dependency startup: {str(e)}")

async def shutdown_dependencies():
    """Clean up dependencies on application shutdown."""
    try:
        if _container:
            _container.close_all()
            logger.info("Dependencies shut down successfully")
    except Exception as e:
        logger.error(f"Error during dependency shutdown: {str(e)}")

# Export the main interface
__all__ = [
    'configure_dependencies',
    'get_container',
    'get_repository_for_dataset',
    'get_service_for_dataset',
    'get_external_service_for_dataset',
    'create_service_factory',
    'get_service_factory_registry',
    'DependencyContext',
    'check_dependencies_health',
    'startup_dependencies',
    'shutdown_dependencies'
] 