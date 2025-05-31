import json
from datetime import datetime
from pathlib import Path
from typing import List

from ...domain.entities.well_production import WellProduction
from ...domain.repositories.well_production_repository import WellProductionRepository

class ImportWellProductionUseCase:
    """Use case for importing well production data from JSON with optimized bulk operations."""
    
    def __init__(self, repository: WellProductionRepository):
        self.repository = repository
    
    async def execute(self, json_path: Path) -> dict:
        """Execute the import use case with bulk operations for optimal performance.
        
        Args:
            json_path: Path to the JSON file containing well production data
            
        Returns:
            Dictionary with import statistics
        """
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Convert all JSON data to domain entities in memory first
        well_productions = []
        current_time = datetime.now()
        
        for well_data in data['value']:
            well = WellProduction(
                field_code=well_data['field_code'],
                field_name=well_data['_field_name'],
                well_code=well_data['well_code'],
                well_reference=well_data['_well_reference'],
                well_name=well_data['well_name'],
                production_period=well_data['production_period'],
                days_on_production=well_data['days_on_production'],
                oil_production_kbd=well_data['oil_production_kbd'],
                gas_production_mmcfd=well_data['gas_production_mmcfd'],
                liquids_production_kbd=well_data['liquids_production_kbd'],
                water_production_kbd=well_data['water_production_kbd'],
                data_source=well_data['data_source'],
                source_data=well_data['source_data'],
                partition_0=well_data['partition_0'],
                created_at=current_time,
                updated_at=current_time
            )
            well_productions.append(well)
        
        # Use bulk insert for better performance
        imported_wells = await self.repository.bulk_insert(well_productions)
        
        # Get final count for statistics
        total_count = await self.repository.count()
        
        return {
            "imported_count": len(imported_wells),
            "total_records": total_count,
            "message": f"Successfully imported {len(imported_wells)} wells. Total records: {total_count}"
        } 