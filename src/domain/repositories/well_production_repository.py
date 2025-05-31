from abc import ABC, abstractmethod
from typing import List, Optional, Tuple
from ..entities.well_production import WellProduction

class WellProductionRepository(ABC):
    """Repository interface for well production data."""
    
    @abstractmethod
    async def get_by_well_code(self, well_code: int) -> List[WellProduction]:
        """Get well production data by well code."""
        pass
    
    @abstractmethod
    async def get_by_field_code(self, field_code: int) -> List[WellProduction]:
        """Get all well production data for a field."""
        pass
    
    @abstractmethod
    async def save(self, well_production: WellProduction) -> WellProduction:
        """Save well production data."""
        pass
    
    @abstractmethod
    async def update(self, well_production: WellProduction) -> WellProduction:
        """Update well production data."""
        pass
    
    @abstractmethod
    async def bulk_insert(self, well_productions: List[WellProduction]) -> Tuple[List[WellProduction], int, int]:
        """
        Bulk insert well production data with duplicate detection.
        
        Returns:
            Tuple of (inserted_records, new_records_count, duplicate_records_count)
        """
        pass
    
    @abstractmethod
    async def get_all(self) -> List[WellProduction]:
        """Get all well production data."""
        pass
    
    @abstractmethod
    async def count(self) -> int:
        """Get the total count of well production records."""
        pass 