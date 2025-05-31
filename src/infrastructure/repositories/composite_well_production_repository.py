import csv
from pathlib import Path
from typing import List, Optional
from datetime import datetime
import asyncio

from ...domain.entities.well_production import WellProduction
from ...domain.repositories.well_production_repository import WellProductionRepository
from .duckdb_well_production_repository import DuckDBWellProductionRepository

class CompositeWellProductionRepository(WellProductionRepository):
    """Composite repository that handles both CSV and DuckDB storage."""
    
    def __init__(self, data_dir: Path = Path("data")):
        self.data_dir = data_dir
        self.data_dir.mkdir(exist_ok=True)
        self.csv_path = self.data_dir / "wells_prod.csv"
        self.duckdb_repo = DuckDBWellProductionRepository(data_dir / "wells_production.duckdb")
    
    async def get_by_well_code(self, well_code: int) -> Optional[WellProduction]:
        """Get well production data by well code from DuckDB (faster for queries)."""
        return await self.duckdb_repo.get_by_well_code(well_code)
    
    async def get_by_field_code(self, field_code: int) -> List[WellProduction]:
        """Get all well production data for a field from DuckDB."""
        return await self.duckdb_repo.get_by_field_code(field_code)
    
    async def save(self, well_production: WellProduction) -> WellProduction:
        """Save well production data to both CSV and DuckDB."""
        # Save to both storages concurrently
        tasks = [
            self._save_to_csv(well_production),
            self.duckdb_repo.save(well_production)
        ]
        await asyncio.gather(*tasks)
        return well_production
    
    async def update(self, well_production: WellProduction) -> WellProduction:
        """Update well production data in both storages."""
        return await self.save(well_production)  # Both handle upsert
    
    async def bulk_insert(self, well_productions: List[WellProduction]) -> List[WellProduction]:
        """Bulk insert data to both storages for optimal performance."""
        if not well_productions:
            return well_productions
            
        # Perform bulk operations concurrently
        tasks = [
            self._bulk_save_to_csv(well_productions),
            self.duckdb_repo.bulk_insert(well_productions)
        ]
        await asyncio.gather(*tasks)
        return well_productions
    
    async def get_all(self) -> List[WellProduction]:
        """Get all well production data from DuckDB (faster for large datasets)."""
        return await self.duckdb_repo.get_all()
    
    async def count(self) -> int:
        """Get the total count of well production records from DuckDB."""
        return await self.duckdb_repo.count()
    
    async def export_to_csv(self) -> Path:
        """Export all data from DuckDB to CSV for download."""
        well_productions = await self.duckdb_repo.get_all()
        await self._bulk_save_to_csv(well_productions, overwrite=True)
        return self.csv_path
    
    async def _save_to_csv(self, well_production: WellProduction) -> None:
        """Save a single well production record to CSV."""
        file_exists = self.csv_path.exists()
        
        with open(self.csv_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self._get_fieldnames())
            if not file_exists:
                writer.writeheader()
            writer.writerow(self._entity_to_row(well_production))
    
    async def _bulk_save_to_csv(self, well_productions: List[WellProduction], overwrite: bool = False) -> None:
        """Bulk save well production records to CSV."""
        mode = 'w' if overwrite else 'a'
        file_exists = self.csv_path.exists() and not overwrite
        
        with open(self.csv_path, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self._get_fieldnames())
            if not file_exists:
                writer.writeheader()
            
            for well_production in well_productions:
                writer.writerow(self._entity_to_row(well_production))
    
    def _get_fieldnames(self) -> List[str]:
        """Get the fieldnames for the CSV file."""
        return [
            'field_code', 'field_name', 'well_code', 'well_reference',
            'well_name', 'production_period', 'days_on_production',
            'oil_production_kbd', 'gas_production_mmcfd',
            'liquids_production_kbd', 'water_production_kbd',
            'data_source', 'source_data', 'partition_0',
            'created_at', 'updated_at'
        ]
    
    def _entity_to_row(self, entity: WellProduction) -> dict:
        """Convert a WellProduction entity to a CSV row."""
        return {
            'field_code': str(entity.field_code),
            'field_name': entity.field_name,
            'well_code': str(entity.well_code),
            'well_reference': entity.well_reference,
            'well_name': entity.well_name,
            'production_period': entity.production_period,
            'days_on_production': str(entity.days_on_production),
            'oil_production_kbd': str(entity.oil_production_kbd),
            'gas_production_mmcfd': str(entity.gas_production_mmcfd),
            'liquids_production_kbd': str(entity.liquids_production_kbd),
            'water_production_kbd': str(entity.water_production_kbd),
            'data_source': entity.data_source,
            'source_data': entity.source_data,
            'partition_0': entity.partition_0,
            'created_at': entity.created_at.isoformat() if entity.created_at else '',
            'updated_at': entity.updated_at.isoformat() if entity.updated_at else ''
        } 