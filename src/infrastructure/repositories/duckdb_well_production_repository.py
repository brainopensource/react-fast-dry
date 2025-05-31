import duckdb
from pathlib import Path
from typing import List, Optional
from datetime import datetime

from ...domain.entities.well_production import WellProduction
from ...domain.repositories.well_production_repository import WellProductionRepository
from ...shared.utils.sql_loader import load_sql

class DuckDBWellProductionRepository(WellProductionRepository):
    """DuckDB implementation of the well production repository."""
    
    def __init__(self, db_path: Path = Path("data/wells_production.duckdb"), sql_path: Path = None):
        self.db_path = db_path
        self.db_path.parent.mkdir(exist_ok=True)
        
        # Load SQL queries from file
        if sql_path is None:
            sql_path = Path(__file__).parent.parent / "operations" / "wells.sql"
        self.queries = load_sql(str(sql_path))
        
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize the DuckDB database and create the table if it doesn't exist."""
        with duckdb.connect(str(self.db_path)) as conn:
            # Create table
            conn.execute(self.queries['create_table'])
            
            # Create indexes
            conn.execute(self.queries['create_indexes'])
    
    async def get_by_well_code(self, well_code: int) -> Optional[WellProduction]:
        """Get well production data by well code."""
        with duckdb.connect(str(self.db_path)) as conn:
            result = conn.execute(
                self.queries['get_by_well_code'],
                [well_code]
            ).fetchone()
            
            return self._row_to_entity(result) if result else None
    
    async def get_by_field_code(self, field_code: int) -> List[WellProduction]:
        """Get all well production data for a field."""
        with duckdb.connect(str(self.db_path)) as conn:
            results = conn.execute(
                self.queries['get_by_field_code'],
                [field_code]
            ).fetchall()
            
            return [self._row_to_entity(row) for row in results]
    
    async def save(self, well_production: WellProduction) -> WellProduction:
        """Save well production data."""
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute(
                self.queries['insert_single'], 
                self._entity_to_params(well_production)
            )
        
        return well_production
    
    async def update(self, well_production: WellProduction) -> WellProduction:
        """Update well production data."""
        return await self.save(well_production)  # DuckDB handles upsert with INSERT OR REPLACE
    
    async def bulk_insert(self, well_productions: List[WellProduction]) -> List[WellProduction]:
        """Bulk insert well production data for better performance."""
        if not well_productions:
            return well_productions
            
        with duckdb.connect(str(self.db_path)) as conn:
            data = [
                self._entity_to_params(wp)
                for wp in well_productions
            ]
            
            conn.executemany(self.queries['insert_bulk'], data)
        
        return well_productions
    
    async def get_all(self) -> List[WellProduction]:
        """Get all well production data."""
        with duckdb.connect(str(self.db_path)) as conn:
            results = conn.execute(self.queries['get_all']).fetchall()
            return [self._row_to_entity(row) for row in results]
    
    async def count(self) -> int:
        """Get the total count of well production records."""
        with duckdb.connect(str(self.db_path)) as conn:
            result = conn.execute(self.queries['count_all']).fetchone()
            return result[0] if result else 0
    
    def _entity_to_params(self, well_production: WellProduction) -> list:
        """Convert a WellProduction entity to a list of parameters for SQL queries."""
        return [
            well_production.field_code,
            well_production.field_name,
            well_production.well_code,
            well_production.well_reference,
            well_production.well_name,
            well_production.production_period,
            well_production.days_on_production,
            well_production.oil_production_kbd,
            well_production.gas_production_mmcfd,
            well_production.liquids_production_kbd,
            well_production.water_production_kbd,
            well_production.data_source,
            well_production.source_data,
            well_production.partition_0,
            well_production.created_at,
            well_production.updated_at
        ]
    
    def _row_to_entity(self, row: tuple) -> WellProduction:
        """Convert a database row to a WellProduction entity."""
        return WellProduction(
            field_code=row[0],
            field_name=row[1],
            well_code=row[2],
            well_reference=row[3],
            well_name=row[4],
            production_period=row[5],
            days_on_production=row[6],
            oil_production_kbd=row[7],
            gas_production_mmcfd=row[8],
            liquids_production_kbd=row[9],
            water_production_kbd=row[10],
            data_source=row[11],
            source_data=row[12],
            partition_0=row[13],
            created_at=row[14],
            updated_at=row[15]
        ) 