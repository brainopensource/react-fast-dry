import csv
import json
from pathlib import Path
from typing import List, Optional, Tuple, Set
from datetime import datetime

from ...domain.entities.well_production import WellProduction
from ...domain.repositories.well_production_repository import WellProductionRepository
from ...domain.value_objects.source_data import SourceData

class WellProductionRepositoryImpl(WellProductionRepository):
    """Implementation of the well production repository."""
    
    def __init__(self, data_dir: Path = Path("data")):
        self.data_dir = data_dir
        self.data_dir.mkdir(exist_ok=True)
        self.csv_path = self.data_dir / "wells_prod.csv"
    
    async def get_by_well_code(self, well_code: int) -> Optional[WellProduction]:
        """Get well production data by well code."""
        if not self.csv_path.exists():
            return None
            
        with open(self.csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if int(row['well_code']) == well_code:
                    return self._row_to_entity(row)
        return None
    
    async def get_by_field_code(self, field_code: int) -> List[WellProduction]:
        """Get all well production data for a field."""
        if not self.csv_path.exists():
            return []
            
        results = []
        with open(self.csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if int(row['field_code']) == field_code:
                    results.append(self._row_to_entity(row))
        return results
    
    async def save(self, well_production: WellProduction) -> WellProduction:
        """Save well production data."""
        # For this implementation, we'll just append to the CSV
        file_exists = self.csv_path.exists()
        
        with open(self.csv_path, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self._get_fieldnames())
            if not file_exists:
                writer.writeheader()
            writer.writerow(self._entity_to_row(well_production))
        
        return well_production
    
    async def update(self, well_production: WellProduction) -> WellProduction:
        """Update well production data."""
        # For this implementation, we'll read all rows, update the matching one,
        # and write everything back
        if not self.csv_path.exists():
            return await self.save(well_production)
            
        rows = []
        with open(self.csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if int(row['well_code']) == well_production.well_code:
                    rows.append(self._entity_to_row(well_production))
                else:
                    rows.append(row)
        
        with open(self.csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self._get_fieldnames())
            writer.writeheader()
            writer.writerows(rows)
        
        return well_production
    
    async def get_existing_record_keys(self) -> Set[str]:
        """Get a set of existing record keys for duplicate detection."""
        if not self.csv_path.exists():
            return set()
            
        existing_keys = set()
        with open(self.csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Create a unique key based on well_code and production_period
                key = f"{row['well_code']}_{row['production_period']}"
                existing_keys.add(key)
        return existing_keys
    
    async def bulk_insert(self, well_productions: List[WellProduction]) -> Tuple[List[WellProduction], int, int]:
        """
        Bulk insert well production data with duplicate detection.
        
        Returns:
            Tuple of (inserted_records, new_records_count, duplicate_records_count)
        """
        if not well_productions:
            return well_productions, 0, 0
            
        # Get existing record keys for duplicate detection
        existing_keys = await self.get_existing_record_keys()
        
        # Separate new records from duplicates
        new_records = []
        duplicate_count = 0
        
        for well_production in well_productions:
            record_key = f"{well_production.well_code}_{well_production.production_period}"
            if record_key not in existing_keys:
                new_records.append(well_production)
                existing_keys.add(record_key)  # Add to set to avoid duplicates within the batch
            else:
                duplicate_count += 1
        
        # Only insert new records
        if new_records:
            file_exists = self.csv_path.exists()
            
            with open(self.csv_path, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self._get_fieldnames())
                if not file_exists:
                    writer.writeheader()
                
                for well_production in new_records:
                    writer.writerow(self._entity_to_row(well_production))
        
        return new_records, len(new_records), duplicate_count

    async def get_all(self) -> List[WellProduction]:
        """Get all well production data."""
        if not self.csv_path.exists():
            return []
            
        results = []
        with open(self.csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                results.append(self._row_to_entity(row))
        return results

    async def count(self) -> int:
        """Get the total count of well production records."""
        if not self.csv_path.exists():
            return 0
            
        with open(self.csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return sum(1 for _ in reader)
    
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
    
    def _row_to_entity(self, row: dict) -> WellProduction:
        """Convert a CSV row to a WellProduction entity."""
        return WellProduction(
            field_code=int(row['field_code']),
            field_name=row['field_name'],
            well_code=int(row['well_code']),
            well_reference=row['well_reference'],
            well_name=row['well_name'],
            production_period=row['production_period'],
            days_on_production=int(row['days_on_production']),
            oil_production_kbd=float(row['oil_production_kbd']),
            gas_production_mmcfd=float(row['gas_production_mmcfd']),
            liquids_production_kbd=float(row['liquids_production_kbd']),
            water_production_kbd=float(row['water_production_kbd']),
            data_source=row['data_source'],
            source_data=row['source_data'],
            partition_0=row['partition_0'],
            created_at=datetime.fromisoformat(row['created_at']) if row.get('created_at') else None,
            updated_at=datetime.fromisoformat(row['updated_at']) if row.get('updated_at') else None
        )
    
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