"""
Improved well production service with decoupled dependencies and batch processing.
Implements business logic without tight coupling to external services.
"""
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from dataclasses import asdict

from ...domain.entities.well_production import WellProduction
from ...domain.repositories.well_production_repository import WellProductionRepository as WellProductionRepositoryPort
from ...domain.ports.external_api_port import ExternalApiPort
from ...shared.batch_processor import BatchProcessor, BatchConfig
from ...shared.exceptions import (
    ValidationException, 
    BusinessRuleViolationException,
    ApplicationException
)
from ...shared.responses import ResponseBuilder, BatchResult

logger = logging.getLogger(__name__)


class WellProductionService:
    """
    Service for well production business operations.
    Uses dependency injection for external dependencies.
    """
    
    def __init__(
        self,
        repository: WellProductionRepositoryPort,
        external_api: ExternalApiPort,
        batch_processor: Optional[BatchProcessor] = None
    ):
        self.repository = repository
        self.external_api = external_api
        self.batch_processor = batch_processor or BatchProcessor(
            BatchConfig(
                batch_size=10000,
                max_memory_mb=4096.0,
                max_concurrent_batches=4,
                retry_attempts=3,
                retry_delay_seconds=2.0,
                enable_memory_monitoring=True,
                gc_threshold_mb=2048.0
            )
        )
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
    
    async def get_production_by_well(
        self,
        well_code: int,
        period_start: Optional[datetime] = None,
        period_end: Optional[datetime] = None
    ) -> List[WellProduction]:
        """
        Get production data for a specific well.
        
        Args:
            well_code: Well identification code
            period_start: Optional start date for filtering
            period_end: Optional end date for filtering
            
        Returns:
            List of WellProduction entities
            
        Raises:
            ValidationException: When parameters are invalid
        """
        try:
            if well_code <= 0:
                raise ValidationException(
                    message="Well code must be positive",
                    field="well_code",
                    value=well_code
                )
            
            # Get all records for the well
            wells = await self.repository.get_by_well_code(well_code)
            
            # Apply date filtering if provided
            if period_start or period_end:
                filtered_wells = []
                for well in wells:
                    # Parse production_period (assuming format like "2024-01" or "2024-01-01")
                    try:
                        if len(well.production_period) == 7:  # "2024-01" format
                            well_date = datetime.strptime(well.production_period + "-01", "%Y-%m-%d")
                        else:  # Assume full date format
                            well_date = datetime.strptime(well.production_period, "%Y-%m-%d")
                        
                        # Check if within date range
                        if period_start and well_date < period_start:
                            continue
                        if period_end and well_date > period_end:
                            continue
                        
                        filtered_wells.append(well)
                    except ValueError:
                        # If date parsing fails, include the record
                        filtered_wells.append(well)
                
                wells = filtered_wells
            
            return wells
            
        except ApplicationException:
            raise
        except Exception as e:
            logger.error(f"Error retrieving well production for {well_code}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to retrieve well production: {str(e)}",
                cause=e
            )
    
    async def get_production_by_field(
        self,
        field_code: int,
        limit: Optional[int] = None
    ) -> List[WellProduction]:
        """
        Get production data for all wells in a field.
        
        Args:
            field_code: Field identification code
            limit: Optional limit for number of records
            
        Returns:
            List of WellProduction entities
        """
        try:
            if field_code <= 0:
                raise ValidationException(
                    message="Field code must be positive",
                    field="field_code",
                    value=field_code
                )
            
            # Get all records for the field
            wells = await self.repository.get_by_field_code(field_code)
            
            # Apply limit if provided
            if limit and limit > 0:
                wells = wells[:limit]
            
            return wells
            
        except ApplicationException:
            raise
        except Exception as e:
            logger.error(f"Error retrieving field production for {field_code}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to retrieve field production: {str(e)}",
                cause=e
            )
    
    async def get_production_statistics(self) -> Dict[str, Any]:
        """
        Get overall production statistics.
        
        Returns:
            Dictionary with production statistics
        """
        try:
            total_count = await self.repository.count()
            
            # Get external API status
            api_status = await self.external_api.get_api_status()
            
            return {
                "total_records": total_count,
                "external_api_status": api_status,
                "batch_processor_status": self.batch_processor.get_memory_status(),
                "last_updated": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting production statistics: {str(e)}")
            raise ApplicationException(
                message=f"Failed to get statistics: {str(e)}",
                cause=e
            )
    
    async def validate_data_quality(
        self,
        well_productions: List[WellProduction]
    ) -> Dict[str, Any]:
        """
        Validate data quality for well production records.
        
        Args:
            well_productions: List of production records to validate
            
        Returns:
            Dictionary with validation results
        """
        validation_results = {
            "total_records": len(well_productions),
            "valid_records": 0,
            "invalid_records": 0,
            "validation_errors": []
        }
        
        for production in well_productions:
            try:
                # Business rule validations
                if not production.is_producing() and production.calculate_total_production() > 0:
                    raise BusinessRuleViolationException(
                        message="Non-producing well has production data",
                        rule="production_consistency",
                        context={"well_code": production.well_code}
                    )
                
                # Additional validations can be added here
                validation_results["valid_records"] += 1
                
            except (ValidationException, BusinessRuleViolationException) as e:
                validation_results["invalid_records"] += 1
                validation_results["validation_errors"].append(e.to_dict())
        
        validation_results["quality_score"] = (
            validation_results["valid_records"] / validation_results["total_records"] * 100
            if validation_results["total_records"] > 0 else 0
        )
        
        return validation_results
    
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
    
    def _insert_batch(self, batch: List[WellProduction]) -> int:
        """
        Insert a batch of well production records with duplicate detection.
        
        Args:
            batch: Batch of production records
            
        Returns:
            Number of inserted records
        """
        try:
            # Use asyncio.run to handle the async repository call in a sync context
            import asyncio
            
            async def async_insert():
                inserted_records, new_count, duplicate_count = await self.repository.bulk_insert(batch)
                
                # Update import statistics
                self._import_stats['new_records'] += new_count
                self._import_stats['duplicate_records'] += duplicate_count
                self._import_stats['total_processed'] += len(batch)
                
                return new_count
            
            # Check if there's an existing event loop
            try:
                loop = asyncio.get_running_loop()
                # If we're in an async context, we can't use asyncio.run
                # We'll need to create a task instead
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, async_insert())
                    return future.result()
            except RuntimeError:
                # No event loop running, safe to use asyncio.run
                return asyncio.run(async_insert())
            
        except Exception as e:
            logger.error(f"Error inserting batch: {str(e)}")
            raise 