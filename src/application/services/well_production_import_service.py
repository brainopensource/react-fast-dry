import logging
# asyncio and concurrent.futures removed
from typing import List, Optional, Dict, Any, Tuple, AsyncGenerator
import pandas as pd # Will be removed if validation fully moves to Polars
from dataclasses import asdict # Will be removed if validation fully moves to Polars
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
    Service for importing well production data.
    Handles fetching, validation, and batch insertion of data.
    """

    def __init__(
        self,
        external_api: ExternalApiAdapter,
        repository: WellProductionRepository,
        job_manager: JobManager
    ):
        self.external_api = external_api
        self.repository = repository
        self.job_manager = job_manager
        # Track import statistics across batches
        # These stats are reset per call to import_production_data, which is correct.
        # self._import_stats can be removed if stats are handled locally within import_production_data

    @async_timed # Added decorator
    async def import_production_data(
        self,
        filters: Optional[Dict[str, Any]] = None,
        batch_id: str = None
    ) -> BatchResult:
        """
        Import well production data from external source with batch processing.
        Assumes external_api.fetch_well_production_data now returns a Polars DataFrame.
        Handles validation and batch insertion of data using Polars DataFrames.
        """
        try:
            logger.info(f"Starting well production data import (batch ID: {batch_id})")

            total_new_records_inserted = 0
            total_duplicate_records_skipped = 0
            total_failed_validation_records = 0
            all_validation_errors = [] 

            # fetch_well_production_data now returns a Polars DataFrame from ExternalApiAdapter
            incoming_df = await self.external_api.fetch_well_production_data(filters)

            if incoming_df is None or incoming_df.is_empty():
                logger.info(f"No data returned from external API for batch ID: {batch_id}")
                return BatchResult(
                    batch_id=batch_id, total_items=0, processed_items=0, failed_items=0,
                    success_rate=100, errors=[], execution_time_ms=0, memory_usage_mb=0,
                    metadata={'data_status': 'no_data_from_source'}
                )

            total_records_from_source = incoming_df.height
            logger.info(f"Received {total_records_from_source} records from external API (batch ID: {batch_id}) as Polars DataFrame.")

            if batch_id:
                await self.job_manager.update_job(
                    batch_id,
                    total_records=total_records_from_source,
                    progress=0 # Initial progress
                )
            
            # Validate the entire DataFrame. 
            # _validate_production_data_df will be refactored to accept Polars DF 
            # and return a tuple: (validated_polars_df, list_of_error_dicts)
            valid_df, validation_errors_for_df = self._validate_production_data_df(incoming_df)
            
            if validation_errors_for_df:
                all_validation_errors.extend(validation_errors_for_df)
            
            total_failed_validation_records = total_records_from_source - valid_df.height
            if total_failed_validation_records > 0:
                 logger.warning(f"{total_failed_validation_records} records failed validation for batch ID {batch_id}.")

            if valid_df.is_empty():
                logger.info(f"No valid records after validation for batch ID: {batch_id}. Total from source: {total_records_from_source}")
                if batch_id:
                    await self.job_manager.update_job(
                        batch_id, progress=100, 
                        new_records=0, duplicate_records=0,
                        # We can add 'failed_validation_records': total_failed_validation_records to job metadata here
                    )
                return BatchResult(
                    batch_id=batch_id, total_items=total_records_from_source, 
                    processed_items=0, failed_items=total_failed_validation_records,
                    success_rate=0, errors=[str(e) for e in all_validation_errors], 
                    execution_time_ms=0, memory_usage_mb=0,
                    metadata={
                        'new_records': 0, 'duplicate_records': 0,
                        'failed_validation_records': total_failed_validation_records,
                        'data_status': 'all_failed_validation' if total_records_from_source > 0 else 'no_data_from_source'
                    }
                )

            # At this point, valid_df contains only rows that passed schema and rule validation.
            # Now, pass this Polars DataFrame to the repository's bulk_insert method.
            # The repository's bulk_insert is already refactored to accept a Polars DataFrame.
            
            # The first element of the tuple (list of WellProduction entities) is [] due to performance reasons.
            _, inserted_count, duplicate_count = await self.repository.bulk_insert(valid_df)
            
            total_new_records_inserted = inserted_count
            total_duplicate_records_skipped = duplicate_count

            if batch_id:
                await self.job_manager.update_job(
                    batch_id, progress=100, # Mark as 100% processed
                    new_records=total_new_records_inserted,
                    duplicate_records=total_duplicate_records_skipped
                )

            # Determine overall data status
            data_status = 'no_new_data'
            if total_new_records_inserted > 0:
                data_status = 'updated'
            # If all valid records were duplicates
            elif valid_df.height > 0 and total_duplicate_records_skipped == valid_df.height:
                 data_status = 'no_new_data_all_duplicates'
            elif total_records_from_source > 0 and total_failed_validation_records == total_records_from_source:
                data_status = 'all_failed_validation'
            elif total_records_from_source == 0:
                data_status = 'no_data_from_source'
                
            # Calculate success rate based on valid items that were intended for insertion
            potential_inserts = valid_df.height
            success_rate = ((total_new_records_inserted / potential_inserts) * 100) if potential_inserts > 0 else 100
            if total_records_from_source == 0: success_rate = 100 
            if potential_inserts == 0 and total_records_from_source > 0 : success_rate = 0

            logger.info(f"Import completed for batch ID {batch_id}: "
                        f"{total_new_records_inserted} new, "
                        f"{total_duplicate_records_skipped} duplicates (from {potential_inserts} valid records), "
                        f"{total_failed_validation_records} failed validation "
                        f"out of {total_records_from_source} total records from source.")

            return BatchResult(
                batch_id=batch_id,
                total_items=total_records_from_source,
                processed_items=total_new_records_inserted,
                failed_items=total_failed_validation_records + total_duplicate_records_skipped, 
                success_rate=success_rate,
                errors=[str(e) for e in all_validation_errors],
                execution_time_ms=0, # To be filled by caller or job manager
                memory_usage_mb=0,   # To be filled by caller or job manager
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
        Validate production data using Polars DataFrame for efficiency.
        Performs schema mapping, type casting, and rule-based validation.
        Returns a DataFrame with valid rows and a list of dictionaries detailing errors.
        """
        if production_df.is_empty():
            return production_df, []

        errors: List[Dict[str, Any]] = []
        df_to_validate = production_df.clone() # Work on a clone to avoid modifying the original

        # --- Field Name Mapping (Align with WellProduction entity and DB schema) ---
        # Source JSON field names (keys) to target DataFrame/DB column names (values)
        rename_map = {
            # Direct mappings for fields that already match
            "field_code": "field_code",
            "_field_name": "field_name",  # Fix: underscore prefix in source
            "well_code": "well_code", 
            "_well_reference": "well_reference",  # Fix: underscore prefix in source
            "well_name": "well_name",
            "production_period": "production_period",
            "days_on_production": "days_on_production",
            "oil_production_kbd": "oil_production_kbd",
            "gas_production_mmcfd": "gas_production_mmcfd",
            "liquids_production_kbd": "liquids_production_kbd",
            "water_production_kbd": "water_production_kbd",
            "data_source": "data_source",
            "source_data": "source_data", 
            "partition_0": "partition_0",
            # Legacy Pascal case mappings (in case data format changes)
            "FieldCode": "field_code",
            "FieldName": "field_name",
            "WellCode": "well_code",
            "WellReference": "well_reference",
            "WellName": "well_name",
            "ProductionPeriod": "production_period",
            "DaysOnProduction": "days_on_production",
            "OilProductionKBD": "oil_production_kbd",
            "GasProductionMMCFD": "gas_production_mmcfd",
            "LiquidsProductionKBD": "liquids_production_kbd",
            "WaterProductionKBD": "water_production_kbd",
            "DataSource": "data_source",
            "SourceData": "source_data", 
            "Partition0": "partition_0",
            "createdAt": "created_at", 
            "updatedAt": "updated_at"
        }
        actual_rename_map = {k: v for k, v in rename_map.items() if k in df_to_validate.columns}
        if actual_rename_map:
            df_to_validate = df_to_validate.rename(actual_rename_map)

        # --- Define Target Schema (Matches DuckDB well_production table) ---
        # This ensures correct types and column order for DB insertion.
        target_schema_with_types = {
            "field_code": pl.Int64, "field_name": pl.Utf8, "well_code": pl.Int64,
            "well_reference": pl.Utf8, "well_name": pl.Utf8, "production_period": pl.Utf8,
            "days_on_production": pl.Int64, "oil_production_kbd": pl.Float64,
            "gas_production_mmcfd": pl.Float64, "liquids_production_kbd": pl.Float64,
            "water_production_kbd": pl.Float64, "data_source": pl.Utf8,
            "source_data": pl.Utf8, "partition_0": pl.Utf8,
            "created_at": pl.Datetime, "updated_at": pl.Datetime
        }
        target_columns_ordered = list(target_schema_with_types.keys())

        # --- Type Casting and Column Preparation ---
        expressions_for_casting = []
        for col_name, target_type in target_schema_with_types.items():
            if col_name in df_to_validate.columns:
                current_type = df_to_validate[col_name].dtype
                if target_type == pl.Datetime and current_type == pl.Utf8:
                    # Attempt to parse ISO 8601 format, Coalesce errors to null
                    # Example: "2023-01-15T10:00:00Z" or "2023-01-15T10:00:00.123456Z"
                    # Polars' strptime is quite flexible. Adjust format string if needed.
                    expressions_for_casting.append(
                        pl.col(col_name).str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f%Z", strict=False, exact=False).alias(col_name)
                    )
                elif target_type == pl.Int64 and current_type == pl.Utf8:
                    expressions_for_casting.append(pl.col(col_name).cast(pl.Int64, strict=False).alias(col_name))
                elif target_type == pl.Float64 and current_type == pl.Utf8:
                     expressions_for_casting.append(pl.col(col_name).cast(pl.Float64, strict=False).alias(col_name))
                elif current_type != target_type:
                    expressions_for_casting.append(pl.col(col_name).cast(target_type, strict=False).alias(col_name))
            else:
                # Add missing columns as null literals of the target type
                expressions_for_casting.append(pl.lit(None, dtype=target_type).alias(col_name))
        
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
            df_validated = df_validated.filter(pk_null_condition.is_not()) # Keep only non-null PKs

        # Rule 2: days_on_production >= 0
        if "days_on_production" in df_validated.columns and df_validated["days_on_production"].dtype == pl.Int64:
            dop_invalid_condition = pl.col("days_on_production") < 0
            invalid_dop_rows = df_validated.filter(dop_invalid_condition)
            if not invalid_dop_rows.is_empty():
                for row_dict in invalid_dop_rows.to_dicts():
                    errors.append({
                        "error_type": "InvalidDaysOnProduction",
                        "message": f"days_on_production is negative ({row_dict.get('days_on_production')}) for PK: ({row_dict.get('well_code')}, {row_dict.get('field_code')}, {row_dict.get('production_period')})",
                        "data": { "well_code": row_dict.get('well_code'), "field_code": row_dict.get('field_code'), "production_period": row_dict.get('production_period'), "days_on_production": row_dict.get('days_on_production')}
                    })
                df_validated = df_validated.filter(dop_invalid_condition.is_not())
        
        # Add more validation rules as needed...
        # Example: Check string lengths, specific value constraints, etc.
        # For each rule, filter out invalid rows, add to `errors`, and update `df_validated`

        logger.info(f"Polars validation: Input rows: {production_df.height}, Valid rows: {df_validated.height}, Errors: {len(errors)}")
        return df_validated, errors

    # Removed: _create_well_production_entities_from_df - this is now part of validation/transformation to Polars DF

    # Removed: _get_default_batch_config - BatchProcessor is not used in this simplified flow

    # Removed: _run_batch_processor - BatchProcessor is not used in this simplified flow

# Removing the unused _import_stats attribute from __init__ if it's fully managed within import_production_data
# Also removing BatchProcessor from __init__ if unused.

# The old _validate_production_data and _insert_batch methods are effectively replaced or unused.
# The edit tool will handle removal if the new content fully replaces the old methods' lines.
# If the old methods are not fully covered by the diff, they might need explicit deletion.
# The provided diff implies replacement of import_production_data and addition of _validate_production_data_df.
# The unused methods _validate_production_data (original) and _insert_batch should be manually checked for removal if not overwritten by the tool.

# For the purpose of this edit, I am focusing on replacing `import_production_data` and adding the new validation method.
# The original `_validate_production_data` and `_insert_batch` are implicitly removed by not being included in the new version of the file content that this edit would generate if it replaced the whole file.
# If the edit tool does line-by-line patching, those methods would remain unless explicitly targeted for deletion.
# Assuming this edit is comprehensive for the service logic.
