import duckdb
import csv
import asyncio
from pathlib import Path
from typing import List, Optional, Set, Tuple, Union
from datetime import datetime
import polars as pl

from ...domain.entities.well_production import WellProduction
from ...domain.repositories.well_production_repository import WellProductionRepository
from ...shared.utils.sql_loader import load_sql
from ...shared.utils.timing_decorator import async_timed, timed
from ...shared.config.settings import get_settings
from ...shared.schema import WellProductionSchema

class DuckDBWellProductionRepository(WellProductionRepository):
    """DuckDB implementation of the well production repository with on-demand CSV export."""
    
    def __init__(
        self, 
        db_path: Optional[Path] = None, 
        sql_path: Optional[Path] = None,
        downloads_dir: Optional[Path] = None,
        csv_filename: Optional[str] = None
    ):
        # Get settings for default values
        settings = get_settings()
        
        self.db_path = db_path or settings.DATABASE_PATH
        self.downloads_dir = downloads_dir or settings.DOWNLOADS_DIR
        self.csv_filename = csv_filename or settings.DEFAULT_CSV_FILENAME
        
        # DuckDB export configuration from settings
        self.BATCH_SIZE = settings.DUCKDB_EXPORT_BATCH_SIZE
        self.MEMORY_LIMIT = settings.DUCKDB_EXPORT_MEMORY_LIMIT
        self.THREADS = settings.DUCKDB_EXPORT_THREADS
        self.TEMP_DIR = Path(settings.TEMP_DIR_NAME)
        self.TEMP_DIR.mkdir(parents=True, exist_ok=True)
        
        # Load SQL queries from file
        if sql_path is None:
            sql_path = Path(__file__).parent.parent / "operations" / "wells.sql"
        self.queries = load_sql(str(sql_path))
        
        # Initialize database
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize the DuckDB database and create the table if it doesn't exist."""
        with duckdb.connect(str(self.db_path)) as conn:
            # Create table using schema
            conn.execute(WellProductionSchema.get_sql_create_table())
            
            # Create indexes using schema
            for index_sql in WellProductionSchema.get_sql_indexes():
                conn.execute(index_sql)
    
    async def get_by_well_code(self, well_code: int) -> List[WellProduction]:
        """Get well production data by well code."""
        def _get_by_well_code_sync():
            with duckdb.connect(str(self.db_path)) as conn:
                query = f"""
                SELECT * FROM well_production 
                WHERE well_code = ? 
                ORDER BY production_period DESC
                """
                results = conn.execute(query, [well_code]).fetchall()
                return [self._row_to_entity(row) for row in results]
        
        return await asyncio.to_thread(_get_by_well_code_sync)
    
    async def get_by_field_code(self, field_code: int) -> List[WellProduction]:
        """Get all well production data for a field."""
        def _get_by_field_code_sync():
            with duckdb.connect(str(self.db_path)) as conn:
                query = f"""
                SELECT * FROM well_production 
                WHERE field_code = ?
                """
                results = conn.execute(query, [field_code]).fetchall()
                return [self._row_to_entity(row) for row in results]
        
        return await asyncio.to_thread(_get_by_field_code_sync)
    
    async def save(self, well_production: WellProduction) -> WellProduction:
        """Save well production data."""
        def _save_sync():
            with duckdb.connect(str(self.db_path)) as conn:
                columns = WellProductionSchema.get_column_names()
                placeholders = ", ".join(["?" for _ in columns])
                query = f"""
                INSERT OR REPLACE INTO well_production 
                ({', '.join(columns)}) 
                VALUES ({placeholders})
                """
                conn.execute(query, self._entity_to_params(well_production))
            return well_production
        
        return await asyncio.to_thread(_save_sync)
    
    async def update(self, well_production: WellProduction) -> WellProduction:
        """Update well production data."""
        return await self.save(well_production)  # DuckDB handles upsert with INSERT OR REPLACE
    
    def _get_existing_composite_keys_sync(self, composite_keys: List[Tuple]) -> Set[Tuple]:
        """Synchronous helper to get existing composite keys."""
        if not composite_keys:
            return set()
        
        with duckdb.connect(str(self.db_path)) as conn:
            # Create a temporary table with the keys to check
            placeholders = ','.join(['(?, ?, ?)'] * len(composite_keys))
            query = f"""
            WITH input_keys AS (
                VALUES {placeholders}
            )
            SELECT DISTINCT well_code, field_code, production_period 
            FROM well_production 
            WHERE (well_code, field_code, production_period) IN (
                SELECT * FROM input_keys
            )
            """
            
            # Flatten the composite keys for the query
            flat_keys = [item for sublist in composite_keys for item in sublist]
            results = conn.execute(query, flat_keys).fetchall()
            return set(results)
    
    @async_timed
    async def get_existing_composite_keys(self, composite_keys: List[Tuple]) -> Set[Tuple]:
        """Get existing composite keys for duplicate detection."""
        return await asyncio.to_thread(self._get_existing_composite_keys_sync, composite_keys)
    
    @timed
    def _bulk_insert_sync(self, incoming_df: pl.DataFrame) -> Tuple[List[WellProduction], int, int]:
        """Synchronous helper for bulk insert operations using Polars and ON CONFLICT DO NOTHING."""
        if incoming_df.is_empty():
            return [], 0, 0

        with duckdb.connect(str(self.db_path)) as conn:
            # Register the incoming DataFrame
            conn.register('incoming_productions_df', incoming_df)

            # Get count before insert
            count_before = conn.execute("SELECT COUNT(*) FROM well_production").fetchone()[0]

            # Insert with ON CONFLICT DO NOTHING
            insert_query = """
            INSERT INTO well_production 
            SELECT * FROM incoming_productions_df
            ON CONFLICT (well_code, field_code, production_period) DO NOTHING;
            """
            conn.execute(insert_query)
            
            # Get count after insert
            count_after = conn.execute("SELECT COUNT(*) FROM well_production").fetchone()[0]
            
            # Calculate metrics
            new_records_count = count_after - count_before
            total_incoming = incoming_df.height
            duplicate_count = total_incoming - new_records_count

            # Cleanup
            conn.unregister('incoming_productions_df')

            return [], new_records_count, duplicate_count
    
    @async_timed
    async def bulk_insert(self, well_productions: List[WellProduction]) -> Tuple[List[WellProduction], int, int]:
        """Bulk insert well production data with duplicate detection."""
        def _bulk_insert_sync():
            with duckdb.connect(str(self.db_path)) as conn:
                # Get count before insert
                count_before = conn.execute("SELECT COUNT(*) FROM well_production").fetchone()[0]
                
                # Convert list of WellProduction objects to a DataFrame
                data = []
                for wp in well_productions:
                    data.append({
                        'field_code': wp.field_code,
                        '_field_name': wp.field_name,  # Use alias for DB column
                        'well_code': wp.well_code,
                        '_well_reference': wp.well_reference,  # Use alias for DB column
                        'well_name': wp.well_name,
                        'production_period': wp.production_period,
                        'days_on_production': wp.days_on_production,
                        'oil_production_kbd': wp.oil_production_kbd,
                        'gas_production_mmcfd': wp.gas_production_mmcfd,
                        'liquids_production_kbd': wp.liquids_production_kbd,
                        'water_production_kbd': wp.water_production_kbd,
                        'data_source': wp.data_source,
                        'source_data': wp.source_data,
                        'partition_0': wp.partition_0,
                        'created_at': wp.created_at,
                        'updated_at': wp.updated_at
                    })
                
                # Create DataFrame and register it with DuckDB
                df = pl.DataFrame(data)
                conn.register('incoming_productions_df', df)
                
                # Bulk insert with ON CONFLICT DO NOTHING
                insert_query = """
                INSERT INTO well_production 
                SELECT * FROM incoming_productions_df
                ON CONFLICT (well_code, field_code, production_period) DO NOTHING;
                """
                conn.execute(insert_query)
                
                # Get count after insert
                count_after = conn.execute("SELECT COUNT(*) FROM well_production").fetchone()[0]
                
                # Calculate metrics
                new_records_count = count_after - count_before
                total_incoming = len(well_productions)
                duplicate_count = total_incoming - new_records_count
                
                # Cleanup
                conn.unregister('incoming_productions_df')
                
                return [], new_records_count, duplicate_count
        
        return await asyncio.to_thread(_bulk_insert_sync)
    
    async def get_all(self) -> List[WellProduction]:
        """Get all well production data."""
        def _get_all_sync():
            with duckdb.connect(str(self.db_path)) as conn:
                results = conn.execute(self.queries['get_all']).fetchall()
                return [self._row_to_entity(row) for row in results]
        
        return await asyncio.to_thread(_get_all_sync)
    
    async def count(self) -> int:
        """Get the total count of well production records."""
        def _count_sync():
            with duckdb.connect(str(self.db_path)) as conn:
                result = conn.execute(self.queries['count_all']).fetchone()
                return result[0] if result else 0
        
        return await asyncio.to_thread(_count_sync)
    
    def _entity_to_params(self, well_production: WellProduction) -> list:
        """Convert a WellProduction entity to a list of parameters for SQL queries."""
        return [getattr(well_production, col) for col in WellProductionSchema.get_column_names()]
    
    def _row_to_entity(self, row: tuple) -> WellProduction:
        """Convert a database row to a WellProduction entity."""
        # Get column names in the correct order
        columns = WellProductionSchema.get_column_names()
        # Create a dictionary mapping column names to values
        row_dict = dict(zip(columns, row))
        # Create the entity
        return WellProduction(**row_dict)

    @timed
    def _export_to_csv_sync(self) -> Path:
        """Synchronous helper for CSV export operations."""
        try:
            # Connect to the DuckDB database with optimized settings
            conn = duckdb.connect(str(self.db_path))
            
            # Configure DuckDB for optimal performance
            conn.execute(f"PRAGMA memory_limit='{self.MEMORY_LIMIT}GB'")
            conn.execute(f"PRAGMA threads={self.THREADS}")
            
            # Get total count for progress tracking
            total_count = conn.execute("SELECT COUNT(*) FROM well_production").fetchone()[0]
            
            if total_count <= self.BATCH_SIZE:
                # For smaller datasets, export directly with optimized settings
                conn.execute(f"""
                    COPY (
                        SELECT 
                            field_code::VARCHAR as field_code,
                            _field_name as field_name,
                            well_code::VARCHAR as well_code,
                            _well_reference as well_reference,
                            well_name,
                            production_period,
                            days_on_production::VARCHAR as days_on_production,
                            oil_production_kbd::VARCHAR as oil_production_kbd,
                            gas_production_mmcfd::VARCHAR as gas_production_mmcfd,
                            liquids_production_kbd::VARCHAR as liquids_production_kbd,
                            water_production_kbd::VARCHAR as water_production_kbd,
                            data_source,
                            source_data,
                            partition_0,
                            created_at::VARCHAR as created_at,
                            updated_at::VARCHAR as updated_at
                        FROM well_production
                        ORDER BY well_code, field_code, production_period
                    ) TO '{self.csv_filename}' (
                        HEADER, 
                        DELIMITER ',',
                        QUOTE '"',
                        ESCAPE '"',
                        NULL 'NULL',
                        FORCE_QUOTE (field_name, well_reference, well_name, production_period, data_source, source_data, partition_0)
                    );
                """)
            else:
                # For larger datasets, use parallel export with temporary files
                # First, write headers
                with open(self.csv_filename, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=self._get_fieldnames())
                    writer.writeheader()
                
                # Calculate number of chunks for parallel processing
                num_chunks = (total_count + self.BATCH_SIZE - 1) // self.BATCH_SIZE
                temp_files = []
                
                # Create temporary files for parallel export
                for i in range(num_chunks):
                    temp_file = self.TEMP_DIR / f"temp_export_{i}.csv"
                    temp_files.append(temp_file)
                    
                    # Export chunk to temporary file
                    conn.execute(f"""
                        COPY (
                            SELECT 
                                field_code::VARCHAR as field_code,
                                _field_name as field_name,
                                well_code::VARCHAR as well_code,
                                _well_reference as well_reference,
                                well_name,
                                production_period,
                                days_on_production::VARCHAR as days_on_production,
                                oil_production_kbd::VARCHAR as oil_production_kbd,
                                gas_production_mmcfd::VARCHAR as gas_production_mmcfd,
                                liquids_production_kbd::VARCHAR as liquids_production_kbd,
                                water_production_kbd::VARCHAR as water_production_kbd,
                                data_source,
                                source_data,
                                partition_0,
                                created_at::VARCHAR as created_at,
                                updated_at::VARCHAR as updated_at
                            FROM well_production
                            ORDER BY well_code, field_code, production_period
                            LIMIT {self.BATCH_SIZE} OFFSET {i * self.BATCH_SIZE}
                        ) TO '{temp_file}' (
                            HEADER FALSE, 
                            DELIMITER ',',
                            QUOTE '"',
                            ESCAPE '"',
                            NULL 'NULL',
                            FORCE_QUOTE (field_name, well_reference, well_name, production_period, data_source, source_data, partition_0)
                        );
                    """)
                
                # Combine temporary files into final CSV
                with open(self.csv_filename, 'ab') as outfile:
                    for temp_file in temp_files:
                        with open(temp_file, 'rb') as infile:
                            outfile.write(infile.read())
                        # Clean up temporary file
                        temp_file.unlink()
            
            conn.close()
            return self.csv_filename
            
        except Exception as e:
            # Clean up any temporary files in case of error
            for temp_file in self.TEMP_DIR.glob("temp_export_*.csv"):
                try:
                    temp_file.unlink()
                except:
                    pass
            raise e

    @async_timed
    async def export_to_csv(self) -> Path:
        """Export all data from DuckDB to CSV for download using DuckDB's native export."""
        try:
            csv_path = await asyncio.to_thread(self._export_to_csv_sync)
            return Path(csv_path)
        except Exception as e:
            # Fallback to the old method if DuckDB export fails
            well_productions = await self.get_all()
            await self._bulk_save_to_csv(well_productions, overwrite=True)
            return Path(self.csv_filename)

    def _get_fieldnames(self) -> List[str]:
        """Get CSV fieldnames in the correct order."""
        return [
            'field_code', 'field_name', 'well_code', 'well_reference', 'well_name',
            'production_period', 'days_on_production', 'oil_production_kbd',
            'gas_production_mmcfd', 'liquids_production_kbd', 'water_production_kbd',
            'data_source', 'source_data', 'partition_0', 'created_at', 'updated_at'
        ]

    def _entity_to_row(self, entity: WellProduction) -> dict:
        """Convert WellProduction entity to CSV row dictionary."""
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

    async def _bulk_save_to_csv(self, well_productions: List[WellProduction], overwrite: bool = False) -> None:
        """Fallback method to save data to CSV using Python (slower but more reliable)."""
        mode = 'w' if overwrite else 'a'
        file_exists = self.csv_filename.exists()
        
        with open(self.csv_filename, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self._get_fieldnames())
            
            # Write header if file is new or we're overwriting
            if overwrite or not file_exists:
                writer.writeheader()
            
            for well_production in well_productions:
                writer.writerow(self._entity_to_row(well_production)) 