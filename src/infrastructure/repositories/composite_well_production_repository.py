import csv
from pathlib import Path
from typing import List, Optional, Set, Tuple
from datetime import datetime
import asyncio

from ...domain.entities.well_production import WellProduction
from ...domain.repositories.well_production_repository import WellProductionRepository
from .duckdb_well_production_repository import DuckDBWellProductionRepository

class CompositeWellProductionRepository(WellProductionRepository):
    """Composite repository that handles both CSV and DuckDB storage."""
    
    def __init__(self, data_dir: Path = Path("data"), duckdb_filename: str = "wells_production.duckdb", csv_filename: str = "wells_prod.csv"):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True) # Ensure parent dirs are created
        self.csv_path = self.data_dir / csv_filename
        # Ensure DuckDBWellProductionRepository receives the full path to the database file
        self.duckdb_repo = DuckDBWellProductionRepository(db_path = self.data_dir / duckdb_filename)
    
    async def get_by_well_code(self, well_code: int) -> List[WellProduction]:
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
    
    async def bulk_insert(self, well_productions: List[WellProduction]) -> Tuple[List[WellProduction], int, int]:
        """
        Bulk insert data to both storages for optimal performance, avoiding duplicates.
        
        Returns:
            Tuple of (inserted_records, new_records_count, duplicate_records_count)
        """
        if not well_productions:
            return well_productions, 0, 0
        
        # Filter out duplicates based on composite key (well_code, field_code, production_period)
        unique_records = await self._filter_duplicates(well_productions)
        duplicate_count = len(well_productions) - len(unique_records)
        
        if not unique_records:
            return [], 0, duplicate_count  # All records were duplicates
            
        # Perform bulk operations concurrently
        tasks = [
            self._bulk_save_to_csv(unique_records),
            self.duckdb_repo.bulk_insert(unique_records)
        ]
        await asyncio.gather(*tasks)
        return unique_records, len(unique_records), duplicate_count
    
    async def _filter_duplicates(self, well_productions: List[WellProduction]) -> List[WellProduction]:
        """Filter out records that already exist based on composite key."""
        if not well_productions:
            return well_productions
        
        # Get existing records from DuckDB
        existing_keys = await self.duckdb_repo.get_existing_composite_keys(
            [(wp.well_code, wp.field_code, wp.production_period) for wp in well_productions]
        )
        
        # Filter out duplicates
        unique_records = []
        for wp in well_productions:
            composite_key = (wp.well_code, wp.field_code, wp.production_period)
            if composite_key not in existing_keys:
                unique_records.append(wp)
        
        return unique_records
    
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
        """Save a single well production record to CSV, checking for duplicates."""
        # Check if record already exists in CSV
        if await self._csv_record_exists(well_production):
            return  # Skip duplicate
        
        file_exists = self.csv_path.exists()
        
        with open(self.csv_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self._get_fieldnames())
            if not file_exists:
                writer.writeheader()
            writer.writerow(self._entity_to_row(well_production))
    
    async def _csv_record_exists(self, well_production: WellProduction) -> bool:
        """Check if a record already exists in CSV based on composite key."""
        if not self.csv_path.exists():
            return False
        
        composite_key = (well_production.well_code, well_production.field_code, well_production.production_period)
        
        with open(self.csv_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    row_key = (int(row['well_code']), int(row['field_code']), row['production_period'])
                    if row_key == composite_key:
                        return True
                except (ValueError, KeyError):
                    continue  # Skip malformed rows
        
        return False
    
    async def _bulk_save_to_csv(self, well_productions: List[WellProduction], overwrite: bool = False) -> None:
        """Bulk save well production records to CSV, avoiding duplicates unless overwriting."""
        if overwrite:
            # When overwriting, just write all records
            mode = 'w'
            file_exists = False
        else:
            # When appending, filter out existing records
            well_productions = await self._filter_csv_duplicates(well_productions)
            if not well_productions:
                return  # All records were duplicates
            
            mode = 'a'
            file_exists = self.csv_path.exists()
        
        with open(self.csv_path, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self._get_fieldnames())
            if not file_exists:
                writer.writeheader()
            
            for well_production in well_productions:
                writer.writerow(self._entity_to_row(well_production))
    
    async def _filter_csv_duplicates(self, well_productions: List[WellProduction]) -> List[WellProduction]:
        """Filter out records that already exist in CSV."""
        if not self.csv_path.exists():
            return well_productions
        
        # Get existing composite keys from CSV
        existing_keys: Set[Tuple[int, int, str]] = set()
        
        with open(self.csv_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    key = (int(row['well_code']), int(row['field_code']), row['production_period'])
                    existing_keys.add(key)
                except (ValueError, KeyError):
                    continue  # Skip malformed rows
        
        # Filter out duplicates
        unique_records = []
        for wp in well_productions:
            composite_key = (wp.well_code, wp.field_code, wp.production_period)
            if composite_key not in existing_keys:
                unique_records.append(wp)
        
        return unique_records
    
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