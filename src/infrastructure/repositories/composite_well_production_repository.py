import csv
from pathlib import Path
from typing import List, Optional, Set, Tuple
from datetime import datetime
import asyncio
import duckdb
import os

from ...domain.entities.well_production import WellProduction
from ...domain.repositories.well_production_repository import WellProductionRepository
from .duckdb_well_production_repository import DuckDBWellProductionRepository

class CompositeWellProductionRepository(WellProductionRepository):
    """Composite repository that handles both CSV and DuckDB storage."""
    
    # Bulk processing configuration
    BATCH_SIZE = 100_000  # Number of records per batch
    MEMORY_LIMIT = "6GB"  # Leave 2GB for system and other processes
    THREADS = 4  # Number of threads for parallel processing
    TEMP_DIR = Path("temp")  # Directory for temporary files during export
    
    def __init__(
        self, 
        data_dir: Path = Path("data"), 
        downloads_dir: Path = Path("downloads"),
        duckdb_filename: str = "wells_production.duckdb", 
        csv_filename: str = "wells_prod.csv"
    ):
        self.data_dir = data_dir
        self.downloads_dir = downloads_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        self.TEMP_DIR.mkdir(parents=True, exist_ok=True)
        
        # Database file in data directory
        self.duckdb_repo = DuckDBWellProductionRepository(db_path=self.data_dir / duckdb_filename)
        
        # CSV files in downloads directory
        self.csv_path = self.downloads_dir / csv_filename
        self.duckdb_path = self.data_dir / duckdb_filename
    
    async def _ensure_csv_initialized(self) -> None:
        """Ensure CSV file exists and has proper headers."""
        if not self.csv_path.exists():
            # Create empty CSV with headers
            with open(self.csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self._get_fieldnames())
                writer.writeheader()
    
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
        
        try:
            # First try to get existing records to check for duplicates
            existing_keys = await self.duckdb_repo.get_existing_composite_keys(
                [(wp.well_code, wp.field_code, wp.production_period) for wp in well_productions]
            )
            
            # Filter out duplicates
            unique_records = []
            duplicate_count = 0
            for wp in well_productions:
                composite_key = (wp.well_code, wp.field_code, wp.production_period)
                if composite_key not in existing_keys:
                    unique_records.append(wp)
                else:
                    duplicate_count += 1
            
            # Save unique records to DuckDB if any
            if unique_records:
                await self.duckdb_repo.bulk_insert(unique_records)
            
            # Always recreate CSV with all data from DuckDB, even if no new records
            await self.export_to_csv()
            
            return unique_records, len(unique_records), duplicate_count
            
        except Exception as e:
            # If there's an error (like table doesn't exist), initialize and try again
            await self.duckdb_repo._initialize_database()
            
            # Since table was just created, all records are new
            await self.duckdb_repo.bulk_insert(well_productions)
            await self.export_to_csv()
            return well_productions, len(well_productions), 0
    
    async def get_all(self) -> List[WellProduction]:
        """Get all well production data from DuckDB (faster for large datasets)."""
        return await self.duckdb_repo.get_all()
    
    async def count(self) -> int:
        """Get the total count of well production records from DuckDB."""
        return await self.duckdb_repo.count()
    
    async def export_to_csv(self) -> Path:
        """Export all data from DuckDB to CSV for download using DuckDB's native export."""
        try:
            # Connect to the DuckDB database with optimized settings
            conn = duckdb.connect(str(self.duckdb_path))
            
            # Configure DuckDB for optimal performance
            conn.execute(f"PRAGMA memory_limit='{self.MEMORY_LIMIT}'")
            conn.execute(f"PRAGMA threads={self.THREADS}")
            
            # Get total count for progress tracking
            total_count = conn.execute("SELECT COUNT(*) FROM well_production").fetchone()[0]
            
            if total_count <= self.BATCH_SIZE:
                # For smaller datasets, export directly with optimized settings
                conn.execute(f"""
                    COPY (
                        SELECT 
                            field_code::VARCHAR as field_code,
                            field_name,
                            well_code::VARCHAR as well_code,
                            well_reference,
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
                    ) TO '{self.csv_path}' (
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
                with open(self.csv_path, 'w', newline='', encoding='utf-8') as f:
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
                                field_name,
                                well_code::VARCHAR as well_code,
                                well_reference,
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
                with open(self.csv_path, 'ab') as outfile:
                    for temp_file in temp_files:
                        with open(temp_file, 'rb') as infile:
                            outfile.write(infile.read())
                        # Clean up temporary file
                        temp_file.unlink()
            
            conn.close()
            return self.csv_path
            
        except Exception as e:
            # Clean up any temporary files in case of error
            for temp_file in self.TEMP_DIR.glob("temp_export_*.csv"):
                try:
                    temp_file.unlink()
                except:
                    pass
                    
            # Fallback to the old method if DuckDB export fails
            well_productions = await self.duckdb_repo.get_all()
            await self._bulk_save_to_csv(well_productions, overwrite=True)
            return self.csv_path
    
    async def _save_to_csv(self, well_production: WellProduction) -> None:
        """Save a single well production record to CSV, checking for duplicates."""
        await self._ensure_csv_initialized()  # Ensure CSV exists
        
        # Check if record already exists in CSV
        if await self._csv_record_exists(well_production):
            return  # Skip duplicate
        
        with open(self.csv_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self._get_fieldnames())
            writer.writerow(self._entity_to_row(well_production))
    
    async def _csv_record_exists(self, well_production: WellProduction) -> bool:
        """Check if a record already exists in CSV based on composite key."""
        await self._ensure_csv_initialized()  # Ensure CSV exists
        
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
        """Save multiple well production records to CSV efficiently."""
        if not well_productions:
            return

        mode = 'w' if overwrite else 'a'
        with open(self.csv_path, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self._get_fieldnames())
            if overwrite:
                writer.writeheader()
            
            # Write all rows at once using writerows
            writer.writerows([self._entity_to_row(wp) for wp in well_productions])
    
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