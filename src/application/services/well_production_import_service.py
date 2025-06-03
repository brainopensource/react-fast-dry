import logging
# asyncio and concurrent.futures removed
from typing import List, Optional, Dict, Any, Tuple, AsyncGenerator
import polars as pl # Added for type hinting and direct use

from ...domain.entities.well_production import WellProduction
from ...domain.repositories.well_production_repository import WellProductionRepository
from ...domain.ports.external_api_port import ExternalApiPort
from ...shared.batch_processor import BatchProcessor, BatchResult
from ...shared.exceptions import (
    ValidationException,
    ApplicationException,
    ExternalApiException,
    BatchProcessingException
)
from ...shared.job_manager import JobManager
from ...infrastructure.adapters.external_api_adapter import ExternalApiAdapter
from ...shared.utils.timing_decorator import async_timed, timed # Added import

logger = logging.getLogger(__name__)

class WellProductionImportService:
    """
    Service for importing well production data from external sources.
    Handles data validation, transformation, and storage.
    """

    def __init__(
        self,
        repository: WellProductionRepository,
        external_api: ExternalApiPort,
        batch_processor: BatchProcessor,
        job_manager: JobManager
    ):
        self.repository = repository
        self.external_api = external_api
        self.batch_processor = batch_processor
        self.job_manager = job_manager
        # Track import statistics across batches
        # These stats are reset per call to import_production_data, which is correct.
        # self._import_stats can be removed if stats are handled locally within import_production_data

    @async_timed
    async def import_production_data(
        self,
        filters: Optional[Dict[str, Any]] = None,
        batch_id: str = None
    ) -> BatchResult:
        """
        Import well production data from external source.
        
        Args:
            filters: Optional filters to apply
            batch_id: Optional batch ID for tracking
            
        Returns:
            BatchResult with import statistics
        """
        try:
            logger.info(f"Starting well production data import (batch ID: {batch_id})")
            
            # Initialize counters
            total_records_from_source = 0
            total_new_records_inserted = 0
            total_duplicate_records_skipped = 0
            total_failed_validation_records = 0
            all_validation_errors = []
            
            # Fetch data from external API
            well_productions = await self.external_api.fetch_well_production_data(
                endpoint=None,
                filters=filters
            )
            
            total_records_from_source = len(well_productions)
            logger.info(f"Received {total_records_from_source} records from external API (batch ID: {batch_id})")
            
            if batch_id:
                await self.job_manager.update_job(
                    batch_id,
                    total_records=total_records_from_source,
                    progress=0
                )
            
            if not well_productions:
                logger.info(f"No records returned from external API for batch ID: {batch_id}")
                if batch_id:
                    await self.job_manager.update_job(
                        batch_id, progress=100, 
                        new_records=0, duplicate_records=0
                    )
                return BatchResult(
                    batch_id=batch_id,
                    total_items=0,
                    processed_items=0,
                    failed_items=0,
                    success_rate=100,
                    errors=[],
                    execution_time_ms=0,
                    memory_usage_mb=0,
                    metadata={
                        'new_records': 0,
                        'duplicate_records': 0,
                        'failed_validation_records': 0,
                        'data_status': 'no_data_from_source'
                    }
                )
            
            # Insert data into repository
            _, inserted_count, duplicate_count = await self.repository.bulk_insert(well_productions)
            
            total_new_records_inserted = inserted_count
            total_duplicate_records_skipped = duplicate_count
            
            if batch_id:
                await self.job_manager.update_job(
                    batch_id, progress=100,
                    new_records=total_new_records_inserted,
                    duplicate_records=total_duplicate_records_skipped
                )
            
            # Determine overall data status
            data_status = 'no_new_data'
            if total_new_records_inserted > 0:
                data_status = 'updated'
            elif total_records_from_source > 0 and total_duplicate_records_skipped == total_records_from_source:
                data_status = 'no_new_data_all_duplicates'
            elif total_records_from_source == 0:
                data_status = 'no_data_from_source'
            
            # Calculate success rate
            success_rate = ((total_new_records_inserted / total_records_from_source) * 100) if total_records_from_source > 0 else 100
            
            logger.info(f"Import completed for batch ID {batch_id}: "
                       f"{total_new_records_inserted} new, "
                       f"{total_duplicate_records_skipped} duplicates "
                       f"out of {total_records_from_source} total records from source.")
            
            return BatchResult(
                batch_id=batch_id,
                total_items=total_records_from_source,
                processed_items=total_new_records_inserted,
                failed_items=total_duplicate_records_skipped,
                success_rate=success_rate,
                errors=[str(e) for e in all_validation_errors],
                execution_time_ms=0,
                memory_usage_mb=0,
                metadata={
                    'new_records': total_new_records_inserted,
                    'duplicate_records': total_duplicate_records_skipped,
                    'failed_validation_records': total_failed_validation_records,
                    'data_status': data_status
                }
            )
            
        except ApplicationException as e:
            logger.error(f"Application error during import (batch ID: {batch_id}): {str(e)}", exc_info=True)
            if batch_id:
                await self.job_manager.update_job(batch_id, status='failed', error=str(e))
            raise
        except Exception as e:
            logger.error(f"Unexpected error importing data (batch ID: {batch_id}): {str(e)}", exc_info=True)
            if batch_id:
                await self.job_manager.update_job(batch_id, status='failed', error=str(e))
            raise ApplicationException(
                message=f"Import failed due to unexpected error: {str(e)}",
                cause=e
            )

    @timed
    def _validate_production_data_df(
        self,
        production_df: pl.DataFrame 
    ) -> Tuple[pl.DataFrame, List[Dict[str, Any]]]: 
        """
        Validates production data DataFrame and returns cleaned data with validation errors.
        """
        if production_df.is_empty():
            return production_df, []

        errors: List[Dict[str, Any]] = []
        df_to_validate = production_df.clone() # Work on a clone to avoid modifying the original

        # --- Define Target Schema (Matches DuckDB well_production table) ---
        # This ensures correct types and column order for DB insertion.
        target_schema_with_types = {
            "field_code": pl.Int64, "_field_name": pl.Utf8, "well_code": pl.Int64,
            "_well_reference": pl.Utf8, "well_name": pl.Utf8, "production_period": pl.Utf8,
            "days_on_production": pl.Int64, "oil_production_kbd": pl.Float64,
            "gas_production_mmcfd": pl.Float64, "liquids_production_kbd": pl.Float64,
            "water_production_kbd": pl.Float64, "data_source": pl.Utf8,
            "source_data": pl.Utf8, "partition_0": pl.Utf8,
            "created_at": pl.Datetime, "updated_at": pl.Datetime
        }
        target_columns_ordered = list(target_schema_with_types.keys())

        # --- Type Casting and Column Preparation ---
        expressions_for_casting = []
        
        # Handle type casting
        for col, target_type in target_schema_with_types.items():
            if col in production_df.columns:
                expressions_for_casting.append(pl.col(col).cast(target_type))

        if expressions_for_casting:
            df_to_validate = df_to_validate.with_columns(expressions_for_casting)
        
        # Ensure all target columns exist, filling with null if any were missed (e.g. not in expressions_for_casting)
        for col_name, target_type in target_schema_with_types.items():
            if col_name not in df_to_validate.columns:
                df_to_validate = df_to_validate.with_columns(pl.lit(None, dtype=target_type).alias(col_name))

        # Select columns in the target order, effectively dropping any unexpected ones
        df_validated = df_to_validate.select(target_columns_ordered)

        # --- Validation Rules ---
        # Rule 1: Primary Key components must not be null
        # PK: (well_code, field_code, production_period)
        pk_cols = ["well_code", "field_code", "production_period"]
        pk_null_condition = None
        for pk_col in pk_cols:
            condition = pl.col(pk_col).is_null()
            if pk_null_condition is None:
                pk_null_condition = condition
            else:
                pk_null_condition = pk_null_condition | condition
        
        invalid_pk_rows = df_validated.filter(pk_null_condition)
        if not invalid_pk_rows.is_empty():
            for row_dict in invalid_pk_rows.to_dicts(): # Convert failing rows to dicts for error reporting
                errors.append({
                    "error_type": "NullPrimaryKeyComponent",
                    "message": f"Primary key component is null for data: { {k: row_dict.get(k) for k in pk_cols} }",
                    "data": {k: row_dict.get(k) for k in pk_cols} # Include only PK cols for brevity
                })
            df_validated = df_validated.filter(pk_null_condition.is_not()) # Keep only non-null PKs        # Rule 2: days_on_production >= configured minimum
        if "days_on_production" in df_validated.columns and df_validated["days_on_production"].dtype == pl.Int64:
            from ...shared.config.settings import get_settings
            settings = get_settings()
            dop_invalid_condition = pl.col("days_on_production") < settings.VALIDATION_MIN_DAYS_ON_PRODUCTION
            invalid_dop_rows = df_validated.filter(dop_invalid_condition)
            if not invalid_dop_rows.is_empty():
                for row_dict in invalid_dop_rows.to_dicts():
                    errors.append({
                        "error_type": "InvalidDaysOnProduction",
                        "message": f"days_on_production ({row_dict.get('days_on_production')}) is below minimum threshold ({settings.VALIDATION_MIN_DAYS_ON_PRODUCTION}) for PK: ({row_dict.get('well_code')}, {row_dict.get('field_code')}, {row_dict.get('production_period')})",
                        "data": { "well_code": row_dict.get('well_code'), "field_code": row_dict.get('field_code'), "production_period": row_dict.get('production_period'), "days_on_production": row_dict.get('days_on_production')}
                    })
                df_validated = df_validated.filter(dop_invalid_condition.is_not())
        
        # Add more validation rules as needed...
        # Example: Check string lengths, specific value constraints, etc.
        # For each rule, filter out invalid rows, add to `errors`, and update `df_validated`

        logger.info(f"Polars validation: Input rows: {production_df.height}, Valid rows: {df_validated.height}, Errors: {len(errors)}")
        return df_validated, errors
