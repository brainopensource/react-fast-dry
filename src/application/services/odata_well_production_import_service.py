"""
OData Well Production Import Service for importing data from external OData APIs.
Implements hexagonal architecture and DDD principles with object calisthenics.
"""
import logging
from typing import Optional, Dict, Any, Tuple, List
import polars as pl
import requests

from ...domain.entities.well_production import WellProduction
from ...domain.repositories.well_production_repository import WellProductionRepository
from ...domain.ports.odata_external_api_port import ODataExternalApiPort
from ...shared.batch_processor import BatchResult
from ...shared.exceptions import (
    ValidationException,
    ApplicationException,
    ExternalApiException
)
from ...shared.job_manager import JobManager
from ...shared.utils.timing_decorator import async_timed, timed
from ...shared.schema import WellProductionSchema

logger = logging.getLogger(__name__)


class ODataWellProductionImportService:
    """
    Service for importing well production data from external OData APIs.
    Follows DDD principles and object calisthenics for clean, maintainable code.
    """

    def __init__(
        self,
        odata_api_adapter: ODataExternalApiPort,
        repository: WellProductionRepository,
        job_manager: JobManager
    ):
        self._odata_api_adapter = odata_api_adapter
        self._repository = repository
        self._job_manager = job_manager

    @async_timed
    async def import_production_data_from_odata(
        self,
        batch_id: Optional[str] = None
    ) -> BatchResult:
        """
        Import well production data from external OData API with comprehensive error handling.
        
        Args:
            batch_id: Optional batch identifier for tracking
            
        Returns:
            BatchResult with import statistics and metadata
            
        Raises:
            ApplicationException: When import process fails
            ExternalApiException: When OData API call fails
        """
        try:
            logger.info(f"Starting OData well production data import (batch ID: {batch_id})")

            # Initialize counters following object calisthenics (no primitive obsession)
            import_metrics = ImportMetrics()

            # Fetch data from OData API
            incoming_dataframe = await self._fetch_data_from_odata_api()

            if self._is_dataframe_empty(incoming_dataframe):
                return self._create_empty_batch_result(batch_id, import_metrics)

            import_metrics.set_total_records_from_source(incoming_dataframe.height)
            logger.info(f"Received {import_metrics.total_records_from_source} records from OData API")

            await self._update_job_progress_if_exists(batch_id, import_metrics.total_records_from_source, 0)

            # Validate and clean the data
            validated_dataframe, validation_errors = self._validate_production_dataframe(incoming_dataframe)
            import_metrics.set_failed_validation_records(import_metrics.total_records_from_source - validated_dataframe.height)

            if self._is_dataframe_empty(validated_dataframe):
                return self._create_validation_failed_batch_result(batch_id, import_metrics, validation_errors)

            # Insert validated data into repository
            insertion_result = await self._insert_validated_data(validated_dataframe)
            import_metrics.set_insertion_results(insertion_result.new_records, insertion_result.duplicate_records)

            await self._update_job_completion_if_exists(batch_id, import_metrics)

            # Determine final data status
            data_status = self._determine_data_status(import_metrics)

            logger.info(f"OData import completed for batch ID {batch_id}: "
                       f"{import_metrics.new_records} new, "
                       f"{import_metrics.duplicate_records} duplicates, "
                       f"{import_metrics.failed_validation_records} failed validation "
                       f"out of {import_metrics.total_records_from_source} total records")

            return self._create_successful_batch_result(batch_id, import_metrics, data_status, validation_errors)

        except ApplicationException:
            await self._handle_job_failure_if_exists(batch_id)
            raise
        except Exception as e:
            await self._handle_job_failure_if_exists(batch_id)
            logger.error(f"Unexpected error importing OData data (batch ID: {batch_id}): {str(e)}", exc_info=True)
            raise ApplicationException(
                message=f"OData import failed due to unexpected error: {str(e)}",
                cause=e
            )

    async def _fetch_data_from_odata_api(self) -> pl.DataFrame:
        """Fetch data from OData API with proper error handling."""
        try:
            return await self._odata_api_adapter.fetch_well_production_data()
        except ExternalApiException as e:
            logger.error(f"Failed to fetch data from OData API: {e.message}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching from OData API: {str(e)}", exc_info=True)
            raise ExternalApiException(
                message=f"Unexpected error during OData API fetch: {str(e)}",
                endpoint=self._odata_api_adapter.base_url,
                cause=e
            )

    @timed
    def _validate_production_dataframe(
        self,
        production_dataframe: pl.DataFrame
    ) -> Tuple[pl.DataFrame, List[Dict[str, Any]]]:
        """
        Validate production data using Polars DataFrame for efficiency.
        Follows object calisthenics by avoiding primitive obsession and using intention-revealing names.
        """
        if self._is_dataframe_empty(production_dataframe):
            return production_dataframe, []

        validation_errors = []
        dataframe_to_validate = production_dataframe.clone()

        # Apply field name mapping
        dataframe_to_validate = self._apply_field_name_mapping(dataframe_to_validate)

        # Apply type casting and schema validation
        dataframe_to_validate = self._apply_type_casting_and_schema(dataframe_to_validate)        # Apply business rule validations
        business_rule_validator = BusinessRuleValidator()
        validated_dataframe, business_rule_errors = business_rule_validator.validate_dataframe(dataframe_to_validate)
        validation_errors.extend(business_rule_errors)

        logger.info(f"Polars validation: Input rows: {production_dataframe.height}, "
                   f"Valid rows: {validated_dataframe.height}, Errors: {len(validation_errors)}")

        return validated_dataframe, validation_errors

    def _apply_field_name_mapping(self, dataframe: pl.DataFrame) -> pl.DataFrame:
        """Apply field name mapping to align with domain entity structure."""
        field_mapping = WellProductionSchema.get_field_mapping()
        actual_rename_map = {k: v for k, v in field_mapping.items() if k in dataframe.columns}
        
        if actual_rename_map:
            return dataframe.rename(actual_rename_map)
        return dataframe

    def _apply_type_casting_and_schema(self, dataframe: pl.DataFrame) -> pl.DataFrame:
        """Apply type casting and ensure schema compliance."""
        target_schema = TargetSchema()
        casting_expressions = target_schema.create_casting_expressions(dataframe)
        
        if casting_expressions:
            dataframe = dataframe.with_columns(casting_expressions)
        
        # Ensure all target columns exist
        dataframe = target_schema.ensure_all_columns_exist(dataframe)
        
        # Select columns in target order
        return dataframe.select(target_schema.get_ordered_columns())

    def _apply_business_rule_validations(
        self, 
        dataframe: pl.DataFrame
    ) -> Tuple[pl.DataFrame, List[Dict[str, Any]]]:
        """Apply business rule validations and return valid data with errors."""
        validator = BusinessRuleValidator()
        return validator.validate_dataframe(dataframe)

    async def _insert_validated_data(self, validated_dataframe: pl.DataFrame) -> 'InsertionResult':
        """Insert validated data into repository."""
        try:
            _, inserted_count, duplicate_count = await self._repository.bulk_insert(validated_dataframe)
            return InsertionResult(inserted_count, duplicate_count)
        except Exception as e:
            logger.error(f"Error inserting validated data: {str(e)}", exc_info=True)
            raise ApplicationException(
                message=f"Failed to insert validated data: {str(e)}",
                cause=e
            )

    def _is_dataframe_empty(self, dataframe: pl.DataFrame) -> bool:
        """Check if dataframe is empty following object calisthenics."""
        return dataframe is None or dataframe.is_empty()

    def _determine_data_status(self, metrics: 'ImportMetrics') -> str:
        """Determine the final data status based on import metrics."""
        if metrics.new_records > 0:
            return 'updated'
        elif metrics.total_records_from_source > 0 and metrics.failed_validation_records == metrics.total_records_from_source:
            return 'all_failed_validation'
        elif metrics.duplicate_records > 0:
            return 'no_new_data_all_duplicates'
        elif metrics.total_records_from_source == 0:
            return 'no_data_from_source'
        else:
            return 'no_new_data'

    def _create_empty_batch_result(self, batch_id: str, metrics: 'ImportMetrics') -> BatchResult:
        """Create batch result for empty data scenario."""
        logger.info(f"No data returned from OData API for batch ID: {batch_id}")
        return BatchResult(
            batch_id=batch_id,
            total_items=0,
            processed_items=0,
            failed_items=0,
            success_rate=100,
            errors=[],
            execution_time_ms=0,
            memory_usage_mb=0,
            metadata={'data_status': 'no_data_from_source'}
        )

    def _create_validation_failed_batch_result(
        self, 
        batch_id: str, 
        metrics: 'ImportMetrics', 
        validation_errors: List[Dict[str, Any]]
    ) -> BatchResult:
        """Create batch result for validation failure scenario."""
        logger.info(f"No valid records after validation for batch ID: {batch_id}")
        return BatchResult(
            batch_id=batch_id,
            total_items=metrics.total_records_from_source,
            processed_items=0,
            failed_items=metrics.failed_validation_records,
            success_rate=0,
            errors=[str(e) for e in validation_errors],
            execution_time_ms=0,
            memory_usage_mb=0,
            metadata={
                'new_records': 0,
                'duplicate_records': 0,
                'failed_validation_records': metrics.failed_validation_records,
                'data_status': 'all_failed_validation' if metrics.total_records_from_source > 0 else 'no_data_from_source'
            }
        )

    def _create_successful_batch_result(
        self,
        batch_id: str,
        metrics: 'ImportMetrics',
        data_status: str,
        validation_errors: List[Dict[str, Any]]
    ) -> BatchResult:
        """Create batch result for successful import scenario."""
        potential_inserts = metrics.total_records_from_source - metrics.failed_validation_records
        success_rate = ((metrics.new_records / potential_inserts) * 100) if potential_inserts > 0 else 100
        
        if metrics.total_records_from_source == 0:
            success_rate = 100
        if potential_inserts == 0 and metrics.total_records_from_source > 0:
            success_rate = 0

        return BatchResult(
            batch_id=batch_id,
            total_items=metrics.total_records_from_source,
            processed_items=metrics.new_records,
            failed_items=metrics.failed_validation_records + metrics.duplicate_records,
            success_rate=success_rate,
            errors=[str(e) for e in validation_errors],
            execution_time_ms=0,
            memory_usage_mb=0,
            metadata={
                'new_records': metrics.new_records,
                'duplicate_records': metrics.duplicate_records,
                'failed_validation_records': metrics.failed_validation_records,
                'data_status': data_status
            }
        )

    async def _update_job_progress_if_exists(self, batch_id: str, total_records: int, progress: int) -> None:
        """Update job progress if batch_id exists."""
        if batch_id:
            await self._job_manager.update_job(
                batch_id,
                total_records=total_records,
                progress=progress
            )

    async def _update_job_completion_if_exists(self, batch_id: str, metrics: 'ImportMetrics') -> None:
        """Update job completion if batch_id exists."""
        if batch_id:
            await self._job_manager.update_job(
                batch_id,
                progress=100,
                new_records=metrics.new_records,
                duplicate_records=metrics.duplicate_records
            )

    async def _handle_job_failure_if_exists(self, batch_id: str) -> None:
        """Handle job failure if batch_id exists."""
        if batch_id:
            await self._job_manager.update_job(batch_id, status='failed', error="Import failed")


# Value objects following object calisthenics principles
class ImportMetrics:
    """Encapsulates import metrics to avoid primitive obsession."""
    
    def __init__(self):
        self.total_records_from_source = 0
        self.new_records = 0
        self.duplicate_records = 0
        self.failed_validation_records = 0

    def set_total_records_from_source(self, count: int) -> None:
        self.total_records_from_source = count

    def set_insertion_results(self, new_records: int, duplicate_records: int) -> None:
        self.new_records = new_records
        self.duplicate_records = duplicate_records

    def set_failed_validation_records(self, count: int) -> None:
        self.failed_validation_records = count


class InsertionResult:
    """Encapsulates insertion results."""
    
    def __init__(self, new_records: int, duplicate_records: int):
        self.new_records = new_records
        self.duplicate_records = duplicate_records


class TargetSchema:
    """Defines and manages the target schema for well production data."""
    
    def __init__(self):
        self._schema_with_types = {
            "field_code": pl.Int64,
            "field_name": pl.Utf8,
            "well_code": pl.Int64,
            "well_reference": pl.Utf8,
            "well_name": pl.Utf8,
            "production_period": pl.Utf8,
            "days_on_production": pl.Int64,
            "oil_production_kbd": pl.Float64,
            "gas_production_mmcfd": pl.Float64,
            "liquids_production_kbd": pl.Float64,
            "water_production_kbd": pl.Float64,
            "data_source": pl.Utf8,
            "source_data": pl.Utf8,
            "partition_0": pl.Utf8,
            "created_at": pl.Datetime,
            "updated_at": pl.Datetime
        }

    def create_casting_expressions(self, dataframe: pl.DataFrame) -> List[pl.Expr]:
        """Create casting expressions for type conversion."""
        expressions = []
        
        for column_name, target_type in self._schema_with_types.items():
            if column_name in dataframe.columns:
                current_type = dataframe[column_name].dtype
                expression = self._create_type_casting_expression(column_name, current_type, target_type)
                if expression is not None:
                    expressions.append(expression)
            else:
                # Add missing columns as null literals
                expressions.append(pl.lit(None, dtype=target_type).alias(column_name))
        
        return expressions

    def _create_type_casting_expression(self, column_name: str, current_type: pl.DataType, target_type: pl.DataType) -> Optional[pl.Expr]:
        """Create a type casting expression for a specific column."""
        if target_type == pl.Datetime and current_type == pl.Utf8:
            return pl.col(column_name).str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f%Z", strict=False, exact=False).alias(column_name)
        elif target_type == pl.Int64 and current_type == pl.Utf8:
            return pl.col(column_name).cast(pl.Int64, strict=False).alias(column_name)
        elif target_type == pl.Float64 and current_type == pl.Utf8:
            return pl.col(column_name).cast(pl.Float64, strict=False).alias(column_name)
        elif current_type != target_type:
            return pl.col(column_name).cast(target_type, strict=False).alias(column_name)
        
        return None

    def ensure_all_columns_exist(self, dataframe: pl.DataFrame) -> pl.DataFrame:
        """Ensure all target columns exist in the dataframe."""
        for column_name, target_type in self._schema_with_types.items():
            if column_name not in dataframe.columns:
                dataframe = dataframe.with_columns(pl.lit(None, dtype=target_type).alias(column_name))
        return dataframe

    def get_ordered_columns(self) -> List[str]:
        """Get columns in the target order."""
        return list(self._schema_with_types.keys())


class BusinessRuleValidator:
    """Validates business rules for well production data."""
    
    def __init__(self):
        """Initialize validator with settings from configuration."""
        from ...shared.config.settings import get_settings
        self.settings = get_settings()
    
    def validate_dataframe(self, dataframe: pl.DataFrame) -> Tuple[pl.DataFrame, List[Dict[str, Any]]]:
        """Validate dataframe against business rules."""
        errors = []
        validated_dataframe = dataframe

        # Rule 1: Primary key components must not be null
        validated_dataframe, pk_errors = self._validate_primary_key_components(validated_dataframe)
        errors.extend(pk_errors)

        # Rule 2: Days on production must be non-negative
        validated_dataframe, dop_errors = self._validate_days_on_production(validated_dataframe)
        errors.extend(dop_errors)

        return validated_dataframe, errors

    def _validate_primary_key_components(self, dataframe: pl.DataFrame) -> Tuple[pl.DataFrame, List[Dict[str, Any]]]:
        """Validate that primary key components are not null."""
        errors = []
        primary_key_columns = ["well_code", "field_code", "production_period"]
        
        null_condition = None
        for pk_column in primary_key_columns:
            condition = pl.col(pk_column).is_null()
            null_condition = condition if null_condition is None else null_condition | condition
        
        invalid_rows = dataframe.filter(null_condition)
        if not invalid_rows.is_empty():
            for row_dict in invalid_rows.to_dicts():
                errors.append({
                    "error_type": "NullPrimaryKeyComponent",
                    "message": f"Primary key component is null for data: {row_dict}",
                    "data": {k: row_dict.get(k) for k in primary_key_columns}
                })
            dataframe = dataframe.filter(null_condition.is_not())

        return dataframe, errors

    def _validate_days_on_production(self, dataframe: pl.DataFrame) -> Tuple[pl.DataFrame, List[Dict[str, Any]]]:
        """Validate that days on production is non-negative."""
        errors = []
        if "days_on_production" in dataframe.columns and dataframe["days_on_production"].dtype == pl.Int64:
            invalid_condition = pl.col("days_on_production") < self.settings.VALIDATION_MIN_DAYS_ON_PRODUCTION
            invalid_rows = dataframe.filter(invalid_condition)
            
            if not invalid_rows.is_empty():
                for row_dict in invalid_rows.to_dicts():
                    errors.append({
                        "error_type": "InvalidDaysOnProduction",
                        "message": f"days_on_production ({row_dict.get('days_on_production')}) is below minimum threshold ({self.settings.VALIDATION_MIN_DAYS_ON_PRODUCTION})",
                        "data": {
                            "well_code": row_dict.get('well_code'),
                            "field_code": row_dict.get('field_code'),
                            "production_period": row_dict.get('production_period'),                            "days_on_production": row_dict.get('days_on_production')
                        }
                    })
                dataframe = dataframe.filter(invalid_condition.is_not())

        return dataframe, errors