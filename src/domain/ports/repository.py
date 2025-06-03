"""
Generic repository ports following Repository pattern and Interface Segregation Principle.
These define the contracts for data access in the domain layer.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Generic, TypeVar, AsyncIterator
from dataclasses import dataclass
from enum import Enum

# Generic type for entities
EntityType = TypeVar('EntityType')

# Value objects for query parameters
@dataclass(frozen=True)
class QueryFilter:
    """Value object for query filters."""
    field: str
    operator: str  # "eq", "ne", "gt", "gte", "lt", "lte", "like", "in"
    value: Any

@dataclass(frozen=True)
class QuerySort:
    """Value object for query sorting."""
    field: str
    direction: str = "ASC"  # "ASC" or "DESC"

@dataclass(frozen=True)
class QueryPagination:
    """Value object for pagination."""
    offset: int = 0
    limit: int = 100
    
    def __post_init__(self):
        if self.offset < 0:
            raise ValueError("Offset must be non-negative")
        if self.limit <= 0:
            raise ValueError("Limit must be positive")

@dataclass(frozen=True)
class QueryOptions:
    """Comprehensive query options."""
    filters: List[QueryFilter] = None
    sorts: List[QuerySort] = None
    pagination: Optional[QueryPagination] = None
    
    def __post_init__(self):
        if self.filters is None:
            object.__setattr__(self, 'filters', [])
        if self.sorts is None:
            object.__setattr__(self, 'sorts', [])

# Repository result types
@dataclass
class RepositoryResult(Generic[EntityType]):
    """Result container for repository operations."""
    items: List[EntityType]
    total_count: int
    has_more: bool = False
    
    @property
    def count(self) -> int:
        """Get count of returned items."""
        return len(self.items)

@dataclass
class BulkOperationResult:
    """Result of bulk operations."""
    successful_count: int
    failed_count: int
    total_count: int
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            object.__setattr__(self, 'errors', [])
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_count == 0:
            return 0.0
        return (self.successful_count / self.total_count) * 100

# Base repository interface
class IRepository(ABC, Generic[EntityType]):
    """Base repository interface following Repository pattern."""
    
    @abstractmethod
    async def find_by_id(self, entity_id: str) -> Optional[EntityType]:
        """Find entity by ID."""
        pass
    
    @abstractmethod
    async def find_all(self, options: Optional[QueryOptions] = None) -> RepositoryResult[EntityType]:
        """Find all entities matching criteria."""
        pass
    
    @abstractmethod
    async def save(self, entity: EntityType) -> EntityType:
        """Save a single entity."""
        pass
    
    @abstractmethod
    async def save_many(self, entities: List[EntityType]) -> BulkOperationResult:
        """Save multiple entities."""
        pass
    
    @abstractmethod
    async def update(self, entity_id: str, updates: Dict[str, Any]) -> Optional[EntityType]:
        """Update entity by ID."""
        pass
    
    @abstractmethod
    async def delete(self, entity_id: str) -> bool:
        """Delete entity by ID."""
        pass
    
    @abstractmethod
    async def delete_many(self, filters: List[QueryFilter]) -> int:
        """Delete multiple entities matching filters."""
        pass
    
    @abstractmethod
    async def count(self, filters: Optional[List[QueryFilter]] = None) -> int:
        """Count entities matching filters."""
        pass
    
    @abstractmethod
    async def exists(self, entity_id: str) -> bool:
        """Check if entity exists."""
        pass

# Specialized interfaces following Interface Segregation Principle
class IReadOnlyRepository(ABC, Generic[EntityType]):
    """Read-only repository interface."""
    
    @abstractmethod
    async def find_by_id(self, entity_id: str) -> Optional[EntityType]:
        """Find entity by ID."""
        pass
    
    @abstractmethod
    async def find_all(self, options: Optional[QueryOptions] = None) -> RepositoryResult[EntityType]:
        """Find all entities matching criteria."""
        pass
    
    @abstractmethod
    async def count(self, filters: Optional[List[QueryFilter]] = None) -> int:
        """Count entities matching filters."""
        pass
    
    @abstractmethod
    async def exists(self, entity_id: str) -> bool:
        """Check if entity exists."""
        pass

class IBulkWriteRepository(ABC, Generic[EntityType]):
    """Interface for bulk write operations."""
    
    @abstractmethod
    async def bulk_insert(self, entities: List[EntityType], batch_size: int = 1000) -> BulkOperationResult:
        """Bulk insert entities."""
        pass
    
    @abstractmethod
    async def bulk_update(self, updates: List[Dict[str, Any]], batch_size: int = 1000) -> BulkOperationResult:
        """Bulk update entities."""
        pass
    
    @abstractmethod
    async def bulk_upsert(self, entities: List[EntityType], batch_size: int = 1000) -> BulkOperationResult:
        """Bulk upsert (insert or update) entities."""
        pass

class IStreamingRepository(ABC, Generic[EntityType]):
    """Interface for streaming operations."""
    
    @abstractmethod
    async def stream_all(self, options: Optional[QueryOptions] = None) -> AsyncIterator[EntityType]:
        """Stream all entities matching criteria."""
        pass
    
    @abstractmethod
    async def stream_by_batch(self, batch_size: int = 1000, options: Optional[QueryOptions] = None) -> AsyncIterator[List[EntityType]]:
        """Stream entities in batches."""
        pass

class IAnalyticsRepository(ABC, Generic[EntityType]):
    """Interface for analytics and aggregation operations."""
    
    @abstractmethod
    async def aggregate(self, aggregations: Dict[str, str], filters: Optional[List[QueryFilter]] = None) -> Dict[str, Any]:
        """Perform aggregations (sum, avg, count, etc.)."""
        pass
    
    @abstractmethod
    async def group_by(self, group_fields: List[str], aggregations: Dict[str, str], filters: Optional[List[QueryFilter]] = None) -> List[Dict[str, Any]]:
        """Group by fields with aggregations."""
        pass
    
    @abstractmethod
    async def get_distinct_values(self, field: str, filters: Optional[List[QueryFilter]] = None) -> List[Any]:
        """Get distinct values for a field."""
        pass

# Full-featured repository interface combining all capabilities
class IFullRepository(
    IRepository[EntityType],
    IBulkWriteRepository[EntityType],
    IStreamingRepository[EntityType],
    IAnalyticsRepository[EntityType]
):
    """Full-featured repository interface with all capabilities."""
    pass

# Factory interface for creating repositories
class IRepositoryFactory(ABC):
    """Factory interface for creating repositories."""
    
    @abstractmethod
    def create_repository(self, dataset_name: str) -> IFullRepository:
        """Create repository for dataset."""
        pass
    
    @abstractmethod
    def create_read_only_repository(self, dataset_name: str) -> IReadOnlyRepository:
        """Create read-only repository for dataset."""
        pass 