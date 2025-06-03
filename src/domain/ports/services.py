"""
Generic service ports following Service pattern and defining contracts for application services.
These define the contracts for business logic in the domain layer.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Generic, TypeVar, AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from .repository import QueryOptions, BulkOperationResult

# Generic types
EntityType = TypeVar('EntityType')
DTOType = TypeVar('DTOType')

# Value objects for service operations
@dataclass(frozen=True)
class ImportOptions:
    """Value object for import operations."""
    batch_size: int = 1000
    skip_duplicates: bool = True
    validate_data: bool = True
    overwrite_existing: bool = False
    filters: Optional[Dict[str, Any]] = None

@dataclass(frozen=True)
class ExportOptions:
    """Value object for export operations."""
    format: str = "csv"  # "csv", "json", "xlsx"
    include_headers: bool = True
    filters: Optional[Dict[str, Any]] = None
    columns: Optional[List[str]] = None
    batch_size: int = 10000

# Service result types
@dataclass
class ServiceResult(Generic[DTOType]):
    """Generic result container for service operations."""
    data: DTOType
    success: bool = True
    message: str = ""
    errors: List[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.errors is None:
            object.__setattr__(self, 'errors', [])

@dataclass
class PaginatedResult(Generic[DTOType]):
    """Result container for paginated operations."""
    items: List[DTOType]
    total_count: int
    page: int
    page_size: int
    has_next: bool = False
    has_previous: bool = False
    
    @property
    def total_pages(self) -> int:
        """Calculate total pages."""
        return (self.total_count + self.page_size - 1) // self.page_size

@dataclass
class ImportResult:
    """Result of import operations."""
    total_items: int
    processed_items: int
    successful_items: int
    failed_items: int
    skipped_items: int = 0
    errors: List[str] = None
    processing_time_seconds: float = 0.0
    memory_usage_mb: float = 0.0
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.errors is None:
            object.__setattr__(self, 'errors', [])
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.processed_items == 0:
            return 0.0
        return (self.successful_items / self.processed_items) * 100

@dataclass
class ExportResult:
    """Result of export operations."""
    file_path: str
    total_records: int
    file_size_bytes: int
    processing_time_seconds: float = 0.0
    format: str = "csv"
    
    @property
    def file_size_mb(self) -> float:
        """Get file size in MB."""
        return self.file_size_bytes / (1024 * 1024)

@dataclass
class StatisticsResult:
    """Result of statistics operations."""
    dataset_name: str
    total_records: int
    active_records: int
    last_updated: Optional[datetime] = None
    aggregates: Dict[str, Any] = None
    field_statistics: Dict[str, Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.aggregates is None:
            object.__setattr__(self, 'aggregates', {})
        if self.field_statistics is None:
            object.__setattr__(self, 'field_statistics', {})

# Core service interfaces following Single Responsibility Principle
class IQueryService(ABC, Generic[DTOType]):
    """Interface for query operations."""
    
    @abstractmethod
    async def get_by_id(self, entity_id: str) -> Optional[DTOType]:
        """Get entity by ID."""
        pass
    
    @abstractmethod
    async def get_all(self, options: Optional[QueryOptions] = None) -> PaginatedResult[DTOType]:
        """Get all entities with pagination and filtering."""
        pass
    
    @abstractmethod
    async def search(self, query: str, options: Optional[QueryOptions] = None) -> PaginatedResult[DTOType]:
        """Search entities by query string."""
        pass
    
    @abstractmethod
    async def count(self, filters: Optional[Dict[str, Any]] = None) -> int:
        """Count entities matching filters."""
        pass

class ICommandService(ABC, Generic[DTOType]):
    """Interface for command operations (writes)."""
    
    @abstractmethod
    async def create(self, dto: DTOType) -> ServiceResult[DTOType]:
        """Create a new entity."""
        pass
    
    @abstractmethod
    async def update(self, entity_id: str, dto: DTOType) -> ServiceResult[DTOType]:
        """Update an existing entity."""
        pass
    
    @abstractmethod
    async def delete(self, entity_id: str) -> ServiceResult[bool]:
        """Delete an entity."""
        pass
    
    @abstractmethod
    async def bulk_create(self, dtos: List[DTOType]) -> ServiceResult[BulkOperationResult]:
        """Create multiple entities."""
        pass

class IImportService(ABC):
    """Interface for import operations."""
    
    @abstractmethod
    async def import_from_external(self, options: Optional[ImportOptions] = None) -> ImportResult:
        """Import data from external source."""
        pass
    
    @abstractmethod
    async def import_from_file(self, file_path: str, options: Optional[ImportOptions] = None) -> ImportResult:
        """Import data from file."""
        pass
    
    @abstractmethod
    async def get_import_status(self, import_id: str) -> Optional[Dict[str, Any]]:
        """Get status of an import operation."""
        pass

class IExportService(ABC):
    """Interface for export operations."""
    
    @abstractmethod
    async def export_to_file(self, options: Optional[ExportOptions] = None) -> ExportResult:
        """Export data to file."""
        pass
    
    @abstractmethod
    async def stream_export(self, options: Optional[ExportOptions] = None) -> AsyncIterator[bytes]:
        """Stream export data."""
        pass

class IAnalyticsService(ABC):
    """Interface for analytics and statistics operations."""
    
    @abstractmethod
    async def get_statistics(self) -> StatisticsResult:
        """Get comprehensive statistics."""
        pass
    
    @abstractmethod
    async def get_aggregates(self, aggregations: Dict[str, str], filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get aggregated data."""
        pass
    
    @abstractmethod
    async def get_trends(self, date_field: str, group_by: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Get trend data over time."""
        pass

class IValidationService(ABC, Generic[DTOType]):
    """Interface for validation operations."""
    
    @abstractmethod
    async def validate(self, dto: DTOType) -> List[str]:
        """Validate a single DTO."""
        pass
    
    @abstractmethod
    async def validate_many(self, dtos: List[DTOType]) -> Dict[int, List[str]]:
        """Validate multiple DTOs."""
        pass
    
    @abstractmethod
    async def validate_business_rules(self, dto: DTOType) -> List[str]:
        """Validate business rules."""
        pass

# Composite service interfaces combining multiple capabilities
class IReadService(IQueryService[DTOType], IAnalyticsService):
    """Composite interface for read operations."""
    pass

class IWriteService(ICommandService[DTOType], IValidationService[DTOType]):
    """Composite interface for write operations."""
    pass

class IDataManagementService(
    IQueryService[DTOType],
    ICommandService[DTOType],
    IImportService,
    IExportService,
    IAnalyticsService,
    IValidationService[DTOType]
):
    """Full-featured data management service interface."""
    pass

# Factory interface for creating services
class IServiceFactory(ABC):
    """Factory interface for creating services."""
    
    @abstractmethod
    def create_query_service(self, dataset_name: str) -> IQueryService:
        """Create query service for dataset."""
        pass
    
    @abstractmethod
    def create_command_service(self, dataset_name: str) -> ICommandService:
        """Create command service for dataset."""
        pass
    
    @abstractmethod
    def create_import_service(self, dataset_name: str) -> IImportService:
        """Create import service for dataset."""
        pass
    
    @abstractmethod
    def create_export_service(self, dataset_name: str) -> IExportService:
        """Create export service for dataset."""
        pass
    
    @abstractmethod
    def create_analytics_service(self, dataset_name: str) -> IAnalyticsService:
        """Create analytics service for dataset."""
        pass
    
    @abstractmethod
    def create_data_management_service(self, dataset_name: str) -> IDataManagementService:
        """Create full data management service for dataset."""
        pass

# External service interfaces (for adapters)
class IExternalDataService(ABC):
    """Interface for external data services."""
    
    @abstractmethod
    async def fetch_data(self, entity_set: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Fetch data from external source."""
        pass
    
    @abstractmethod
    async def get_data_count(self, entity_set: str, filters: Optional[Dict[str, Any]] = None) -> int:
        """Get count of data from external source."""
        pass
    
    @abstractmethod
    async def test_connection(self) -> bool:
        """Test connection to external source."""
        pass 