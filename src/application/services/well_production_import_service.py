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
from ...infrastructure.adapters.external_api_adapter import ExternalApiAdapter
from ...infrastructure.repositories.composite_well_production_repository import CompositeWellProductionRepository
from ...shared.job_manager import JobManager

logger = logging.getLogger(__name__)

class WellProductionImportService:
    """
    Service for importing well production data.
    Handles fetching, validation, and batch insertion of data.
    """

    def __init__(
        self,
        external_api: ExternalApiAdapter,
        repository: CompositeWellProductionRepository,
        job_manager: JobManager
    ):
        self.external_api = external_api
        self.repository = repository
        self.job_manager = job_manager
        self.batch_processor = BatchProcessor()
        # Track import statistics across batches
        self._import_stats = {
            'new_records': 0,
            'duplicate_records': 0,
            'total_processed': 0
        }

    async def import_production_data(
        self,
        filters: Optional[Dict[str, Any]] = None,
        batch_id: str = None
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
            data = await self.external_api.fetch_well_production_data(filters)
            
            if not data:
                return BatchResult(
                    batch_id=batch_id,
                    total_items=0,
                    processed_items=0,
                    failed_items=0,
                    success_rate=100,
                    errors=[],
                    execution_time_ms=0,
                    memory_usage_mb=0
                )

            # Process data in batches
            total_records = len(data)
            processed_records = 0
            new_records = 0
            duplicate_records = 0

            # Update job with total records
            if batch_id:
                await self.job_manager.update_job(
                    batch_id,
                    total_records=total_records,
                    progress=0
                )

            # Process in batches
            batch_size = 100000
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                
                # Save batch to repository
                result = await self.repository.bulk_insert(batch)
                new_records += result[1]  # Number of new records
                duplicate_records += result[2]  # Number of duplicates
                processed_records += len(batch)

                # Update progress
                if batch_id:
                    progress = int((processed_records / total_records) * 100)
                    await self.job_manager.update_job(
                        batch_id,
                        progress=progress,
                        new_records=new_records,
                        duplicate_records=duplicate_records
                    )

            # Create enhanced result with duplicate detection info
            enhanced_result = BatchResult(
                batch_id=batch_id,
                total_items=total_records,
                processed_items=new_records,
                failed_items=0,
                success_rate=100,
                errors=[],
                execution_time_ms=0,
                memory_usage_mb=0,
                metadata={
                    'new_records': new_records,
                    'duplicate_records': duplicate_records,
                    'data_status': 'updated' if new_records > 0 else 'no_new_data'
                }
            )

            logger.info(f"Import completed: {new_records} new, {duplicate_records} duplicates out of {total_records} total records")
            return enhanced_result

        except ApplicationException:
            raise
        except Exception as e:
            logger.error(f"Error importing data: {str(e)}", exc_info=True)
            if batch_id:
                await self.job_manager.update_job(
                    batch_id,
                    status='failed',
                    error=str(e)
                )
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
