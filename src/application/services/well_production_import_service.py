import logging
# asyncio and concurrent.futures removed
from typing import List, Optional, Dict, Any

from ...domain.entities.well_production import WellProduction
from ...domain.repositories.well_production_repository import WellProductionRepository as WellProductionRepositoryPort
from ...domain.ports.external_api_port import ExternalApiPort
from ...shared.batch_processor import BatchProcessor # BatchConfig removed
from ...shared.exceptions import (
    ValidationException,
    ApplicationException
)
from ...shared.responses import BatchResult

logger = logging.getLogger(__name__)

class WellProductionImportService:
    """
    Service for importing well production data.
    Handles fetching, validation, and batch insertion of data.
    """

    def __init__(
        self,
        repository: WellProductionRepositoryPort,
        external_api: ExternalApiPort,
        batch_processor: BatchProcessor # Changed: Optional removed, no default
    ):
        self.repository = repository
        self.external_api = external_api
        self.batch_processor = batch_processor # Changed: Direct assignment
        # Track import statistics across batches
        self._import_stats = {
            'new_records': 0,
            'duplicate_records': 0,
            'total_processed': 0
        }

    async def import_production_data(
        self,
        filters: Optional[Dict[str, Any]] = None,
        batch_id: Optional[str] = None
    ) -> BatchResult:
        """
        Import well production data from external source with batch processing.

        Args:
            filters: Optional filters for data import
            batch_id: Optional batch identifier for tracking

        Returns:
            BatchResult with import statistics including duplicate detection

        Raises:
            ValidationException: When data validation fails
            ExternalApiException: When external API fails
            BatchProcessingException: When batch processing fails
        """
        try:
            logger.info("Starting well production data import")

            # Reset import statistics
            self._import_stats = {
                'new_records': 0,
                'duplicate_records': 0,
                'total_processed': 0
            }

            # Fetch data from external API
            external_data = await self.external_api.fetch_well_production_data(
                filters=filters
            )

            if not external_data:
                raise ValidationException(
                    message="No data received from external API",
                    field="external_data"
                )

            # Validate data before processing
            validated_data = await self._validate_production_data(external_data)

            # Process data in batches
            result = await self.batch_processor.process_async(
                items=validated_data,
                processor=self._insert_batch,
                batch_id=batch_id
            )

            # Create enhanced result with duplicate detection info
            enhanced_result = BatchResult(
                batch_id=result.batch_id,
                total_items=result.total_items,
                processed_items=self._import_stats['new_records'],  # Only count new records as "processed"
                failed_items=result.failed_items,
                success_rate=(self._import_stats['new_records'] / result.total_items * 100) if result.total_items > 0 else 0,
                errors=result.errors,
                execution_time_ms=result.execution_time_ms,
                memory_usage_mb=result.memory_usage_mb,
                # Add custom metadata for duplicate tracking
                metadata={
                    'new_records': self._import_stats['new_records'],
                    'duplicate_records': self._import_stats['duplicate_records'],
                    'total_from_source': result.total_items,
                    'data_status': 'new_data_imported' if self._import_stats['new_records'] > 0 else 'no_new_data'
                }
            )

            logger.info(f"Import completed: {self._import_stats['new_records']} new, {self._import_stats['duplicate_records']} duplicates out of {result.total_items} total records")
            return enhanced_result

        except ApplicationException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error during import: {str(e)}")
            raise ApplicationException(
                message=f"Import failed due to unexpected error: {str(e)}",
                cause=e
            )

    async def _validate_production_data(
        self,
        production_data: List[WellProduction]
    ) -> List[WellProduction]:
        """
        Validate production data before processing.

        Args:
            production_data: Raw production data

        Returns:
            Validated production data

        Raises:
            ValidationException: When validation fails
        """
        if not production_data:
            raise ValidationException(
                message="Production data cannot be empty",
                field="production_data"
            )

        validated_data = []
        validation_errors = []

        for i, production in enumerate(production_data):
            try:
                # Validate required fields
                if not production.well_code:
                    raise ValidationException(
                        message=f"Missing well_code at record {i}",
                        field="well_code"
                    )

                # Additional validations
                if production.oil_production_kbd < 0:
                    raise ValidationException(
                        message=f"Negative oil production at record {i}",
                        field="oil_production_kbd",
                        value=production.oil_production_kbd
                    )

                validated_data.append(production)

            except ValidationException as e:
                validation_errors.append(e)

        if validation_errors and len(validation_errors) > len(production_data) * 0.1:
            # Fail if more than 10% of records are invalid
            raise ValidationException(
                message=f"Too many validation errors: {len(validation_errors)} out of {len(production_data)} records",
                field="validation_errors"
            )

        return validated_data

    async def _insert_batch(self, batch: List[WellProduction]) -> int:
        """
        Insert a batch of well production records with duplicate detection.

        Args:
            batch: Batch of production records

        Returns:
            Number of inserted records
        """
        try:
            inserted_records, new_count, duplicate_count = await self.repository.bulk_insert(batch)

            self._import_stats['new_records'] += new_count
            self._import_stats['duplicate_records'] += duplicate_count
            self._import_stats['total_processed'] += len(batch)

            return new_count
        except Exception as e:
            logger.error(f"Error inserting batch: {str(e)}")
            raise
