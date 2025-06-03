"""
Generic data management service implementation following SOLID principles and DRY approach.
This service can handle any dataset based on configuration.
"""

import logging
import asyncio
from typing import List, Optional, Dict, Any, Type, AsyncIterator
from datetime import datetime
import time

from ...domain.ports.repository import (
    IFullRepository, QueryOptions, QueryFilter, QuerySort, QueryPagination
)
from ...domain.ports.services import (
    IDataManagementService, ServiceResult, PaginatedResult, ImportResult,
    ExportResult, StatisticsResult, ImportOptions, ExportOptions, 
    BulkOperationResult
)
from ...shared.config.schemas import (
    DatasetSchemaConfig, DatasetFactory, BaseEntity, get_dataset_config
)
from ...shared.exceptions import (
    ApplicationException, ValidationException, BusinessRuleViolationException,
    ErrorCode
)

logger = logging.getLogger(__name__)

class GenericDataService(IDataManagementService):
    """
    Generic data management service that can handle any dataset.
    Implements all service interfaces following SOLID principles.
    """
    
    def __init__(
        self,
        dataset_name: str,
        repository: IFullRepository,
        external_service = None,  # IExternalDataService
        config: Optional[DatasetSchemaConfig] = None
    ):
        """Initialize generic data service."""
        self.dataset_name = dataset_name
        self.repository = repository
        self.external_service = external_service
        self.config = config or get_dataset_config(dataset_name)
        self.entity_class = DatasetFactory.get_entity_class(dataset_name)
        self.schema_class = DatasetFactory.get_schema_class(dataset_name)
        self.logger = logging.getLogger(f"{__name__}.{dataset_name}")
    
    # Query Service Implementation
    async def get_by_id(self, entity_id: str) -> Optional[Any]:
        """Get entity by ID."""
        try:
            entity = await self.repository.find_by_id(entity_id)
            if not entity:
                return None
            
            # Convert entity to DTO
            return self._entity_to_dto(entity)
            
        except Exception as e:
            self.logger.error(f"Error getting {self.dataset_name} by ID {entity_id}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to retrieve {self.dataset_name}",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    async def get_all(self, options: Optional[QueryOptions] = None) -> PaginatedResult:
        """Get all entities with pagination and filtering."""
        try:
            result = await self.repository.find_all(options)
            
            # Convert entities to DTOs
            dtos = [self._entity_to_dto(entity) for entity in result.items]
            
            # Calculate pagination info
            page_size = options.pagination.limit if options and options.pagination else 100
            page = (options.pagination.offset // page_size) + 1 if options and options.pagination else 1
            
            return PaginatedResult(
                items=dtos,
                total_count=result.total_count,
                page=page,
                page_size=page_size,
                has_next=result.has_more,
                has_previous=page > 1
            )
            
        except Exception as e:
            self.logger.error(f"Error getting all {self.dataset_name}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to retrieve {self.dataset_name} data",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    async def search(self, query: str, options: Optional[QueryOptions] = None) -> PaginatedResult:
        """Search entities by query string."""
        try:
            # Create search filters based on string fields in the configuration
            search_filters = []
            for field_def in self.config.fields:
                if field_def.field_type == str:
                    search_filters.append(
                        QueryFilter(field=field_def.name, operator="like", value=f"%{query}%")
                    )
            
            # Combine with existing filters
            if options and options.filters:
                search_filters.extend(options.filters)
            
            # Create new options with search filters
            search_options = QueryOptions(
                filters=search_filters,
                sorts=options.sorts if options else [],
                pagination=options.pagination if options else None
            )
            
            return await self.get_all(search_options)
            
        except Exception as e:
            self.logger.error(f"Error searching {self.dataset_name} with query '{query}': {str(e)}")
            raise ApplicationException(
                message=f"Failed to search {self.dataset_name}",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    async def count(self, filters: Optional[Dict[str, Any]] = None) -> int:
        """Count entities matching filters."""
        try:
            query_filters = self._dict_to_query_filters(filters) if filters else []
            return await self.repository.count(query_filters)
            
        except Exception as e:
            self.logger.error(f"Error counting {self.dataset_name}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to count {self.dataset_name}",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    # Command Service Implementation
    async def create(self, dto: Any) -> ServiceResult:
        """Create a new entity."""
        try:
            # Validate DTO
            validation_errors = await self.validate(dto)
            if validation_errors:
                return ServiceResult(
                    data=None,
                    success=False,
                    message="Validation failed",
                    errors=validation_errors
                )
            
            # Convert DTO to entity
            entity = self._dto_to_entity(dto)
            
            # Save entity
            saved_entity = await self.repository.save(entity)
            
            # Convert back to DTO
            result_dto = self._entity_to_dto(saved_entity)
            
            return ServiceResult(
                data=result_dto,
                success=True,
                message=f"{self.config.display_name} created successfully"
            )
            
        except Exception as e:
            self.logger.error(f"Error creating {self.dataset_name}: {str(e)}")
            return ServiceResult(
                data=None,
                success=False,
                message=f"Failed to create {self.dataset_name}",
                errors=[str(e)]
            )
    
    async def update(self, entity_id: str, dto: Any) -> ServiceResult:
        """Update an existing entity."""
        try:
            # Check if entity exists
            existing = await self.repository.find_by_id(entity_id)
            if not existing:
                return ServiceResult(
                    data=None,
                    success=False,
                    message=f"{self.config.display_name} not found"
                )
            
            # Validate DTO
            validation_errors = await self.validate(dto)
            if validation_errors:
                return ServiceResult(
                    data=None,
                    success=False,
                    message="Validation failed",
                    errors=validation_errors
                )
            
            # Convert DTO to update dictionary
            updates = dto.model_dump(exclude_unset=True) if hasattr(dto, 'model_dump') else dto.__dict__
            updates['updated_at'] = datetime.now()
            
            # Update entity
            updated_entity = await self.repository.update(entity_id, updates)
            
            if updated_entity:
                result_dto = self._entity_to_dto(updated_entity)
                return ServiceResult(
                    data=result_dto,
                    success=True,
                    message=f"{self.config.display_name} updated successfully"
                )
            else:
                return ServiceResult(
                    data=None,
                    success=False,
                    message=f"Failed to update {self.config.display_name}"
                )
            
        except Exception as e:
            self.logger.error(f"Error updating {self.dataset_name} {entity_id}: {str(e)}")
            return ServiceResult(
                data=None,
                success=False,
                message=f"Failed to update {self.dataset_name}",
                errors=[str(e)]
            )
    
    async def delete(self, entity_id: str) -> ServiceResult[bool]:
        """Delete an entity."""
        try:
            success = await self.repository.delete(entity_id)
            
            return ServiceResult(
                data=success,
                success=success,
                message=f"{self.config.display_name} deleted successfully" if success else f"{self.config.display_name} not found"
            )
            
        except Exception as e:
            self.logger.error(f"Error deleting {self.dataset_name} {entity_id}: {str(e)}")
            return ServiceResult(
                data=False,
                success=False,
                message=f"Failed to delete {self.dataset_name}",
                errors=[str(e)]
            )
    
    async def bulk_create(self, dtos: List[Any]) -> ServiceResult[BulkOperationResult]:
        """Create multiple entities."""
        try:
            # Validate all DTOs
            validation_results = await self.validate_many(dtos)
            valid_dtos = [dto for i, dto in enumerate(dtos) if i not in validation_results]
            
            if not valid_dtos:
                return ServiceResult(
                    data=BulkOperationResult(0, len(dtos), len(dtos)),
                    success=False,
                    message="All records failed validation",
                    errors=[f"Record {i}: {', '.join(errors)}" for i, errors in validation_results.items()]
                )
            
            # Convert DTOs to entities
            entities = [self._dto_to_entity(dto) for dto in valid_dtos]
            
            # Bulk save
            bulk_result = await self.repository.save_many(entities)
            
            return ServiceResult(
                data=bulk_result,
                success=bulk_result.successful_count > 0,
                message=f"Bulk create completed: {bulk_result.successful_count}/{bulk_result.total_count} successful"
            )
            
        except Exception as e:
            self.logger.error(f"Error in bulk create for {self.dataset_name}: {str(e)}")
            return ServiceResult(
                data=BulkOperationResult(0, len(dtos), len(dtos)),
                success=False,
                message=f"Bulk create failed for {self.dataset_name}",
                errors=[str(e)]
            )
    
    # Import Service Implementation
    async def import_from_external(self, options: Optional[ImportOptions] = None) -> ImportResult:
        """Import data from external source."""
        if not self.external_service:
            raise ApplicationException(
                message="External service not configured",
                error_code=ErrorCode.CONFIGURATION_ERROR
            )
        
        start_time = time.time()
        options = options or ImportOptions()
        
        try:
            self.logger.info(f"Starting import for {self.dataset_name}")
            
            # Fetch data from external source
            external_data = await self.external_service.fetch_data(
                self.config.odata_entity_set,
                options.filters
            )
            
            if not external_data:
                return ImportResult(
                    total_items=0,
                    processed_items=0,
                    successful_items=0,
                    failed_items=0,
                    processing_time_seconds=time.time() - start_time
                )
            
            # Convert external data to DTOs
            dtos = []
            conversion_errors = []
            
            for i, data in enumerate(external_data):
                try:
                    dto = self._external_data_to_dto(data)
                    if options.validate_data:
                        validation_errors = await self.validate(dto)
                        if validation_errors:
                            conversion_errors.append(f"Record {i}: {', '.join(validation_errors)}")
                            continue
                    dtos.append(dto)
                except Exception as e:
                    conversion_errors.append(f"Record {i}: {str(e)}")
            
            # Bulk create DTOs
            if dtos:
                result = await self.bulk_create(dtos)
                bulk_result = result.data
                
                return ImportResult(
                    total_items=len(external_data),
                    processed_items=len(dtos),
                    successful_items=bulk_result.successful_count,
                    failed_items=bulk_result.failed_count + len(conversion_errors),
                    errors=conversion_errors + bulk_result.errors,
                    processing_time_seconds=time.time() - start_time,
                    metadata={
                        "dataset": self.dataset_name,
                        "import_strategy": options.__dict__
                    }
                )
            else:
                return ImportResult(
                    total_items=len(external_data),
                    processed_items=0,
                    successful_items=0,
                    failed_items=len(external_data),
                    errors=conversion_errors,
                    processing_time_seconds=time.time() - start_time
                )
                
        except Exception as e:
            self.logger.error(f"Error importing {self.dataset_name}: {str(e)}")
            return ImportResult(
                total_items=0,
                processed_items=0,
                successful_items=0,
                failed_items=0,
                errors=[str(e)],
                processing_time_seconds=time.time() - start_time
            )
    
    async def import_from_file(self, file_path: str, options: Optional[ImportOptions] = None) -> ImportResult:
        """Import data from file."""
        # Implementation for file import would go here
        # This is a placeholder for the interface
        raise NotImplementedError("File import not yet implemented")
    
    async def get_import_status(self, import_id: str) -> Optional[Dict[str, Any]]:
        """Get status of an import operation."""
        # Implementation for import status tracking would go here
        # This is a placeholder for the interface
        raise NotImplementedError("Import status tracking not yet implemented")
    
    # Export Service Implementation
    async def export_to_file(self, options: Optional[ExportOptions] = None) -> ExportResult:
        """Export data to file."""
        options = options or ExportOptions()
        start_time = time.time()
        
        try:
            # Create query options from export options
            query_options = None
            if options.filters:
                filters = self._dict_to_query_filters(options.filters)
                query_options = QueryOptions(filters=filters)
            
            # Get all data
            result = await self.repository.find_all(query_options)
            
            # Export implementation would depend on the format
            # This is a simplified version
            file_path = f"exports/{self.dataset_name}_{int(time.time())}.{options.format}"
            
            # Calculate file size (placeholder)
            file_size = len(result.items) * 100  # Rough estimate
            
            return ExportResult(
                file_path=file_path,
                total_records=len(result.items),
                file_size_bytes=file_size,
                processing_time_seconds=time.time() - start_time,
                format=options.format
            )
            
        except Exception as e:
            self.logger.error(f"Error exporting {self.dataset_name}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to export {self.dataset_name}",
                error_code=ErrorCode.EXPORT_ERROR,
                cause=e
            )
    
    async def stream_export(self, options: Optional[ExportOptions] = None) -> AsyncIterator[bytes]:
        """Stream export data."""
        # Implementation for streaming export would go here
        # This is a placeholder for the interface
        raise NotImplementedError("Streaming export not yet implemented")
    
    # Analytics Service Implementation
    async def get_statistics(self) -> StatisticsResult:
        """Get comprehensive statistics."""
        try:
            total_records = await self.repository.count()
            
            # Count active records based on status rules
            active_records = total_records
            if self.config.status_rules:
                # This is simplified - would need proper filtering based on status rules
                active_records = total_records  # Placeholder
            
            # Get basic aggregates
            aggregates = {}
            if hasattr(self.repository, 'aggregate'):
                # Calculate basic aggregates for numeric fields
                numeric_fields = [
                    field.name for field in self.config.fields 
                    if field.field_type in [int, float]
                ]
                
                if numeric_fields:
                    for field in numeric_fields[:5]:  # Limit to first 5 numeric fields
                        try:
                            field_aggregates = await self.repository.aggregate(
                                {f"{field}_sum": f"SUM({field})", f"{field}_avg": f"AVG({field})"}
                            )
                            aggregates.update(field_aggregates)
                        except:
                            pass  # Skip if aggregation fails
            
            return StatisticsResult(
                dataset_name=self.dataset_name,
                total_records=total_records,
                active_records=active_records,
                last_updated=datetime.now(),
                aggregates=aggregates
            )
            
        except Exception as e:
            self.logger.error(f"Error getting statistics for {self.dataset_name}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to get statistics for {self.dataset_name}",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    async def get_aggregates(self, aggregations: Dict[str, str], filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get aggregated data."""
        try:
            query_filters = self._dict_to_query_filters(filters) if filters else None
            
            if hasattr(self.repository, 'aggregate'):
                return await self.repository.aggregate(aggregations, query_filters)
            else:
                return {}
                
        except Exception as e:
            self.logger.error(f"Error getting aggregates for {self.dataset_name}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to get aggregates for {self.dataset_name}",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    async def get_trends(self, date_field: str, group_by: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Get trend data over time."""
        try:
            query_filters = self._dict_to_query_filters(filters) if filters else None
            
            if hasattr(self.repository, 'group_by'):
                return await self.repository.group_by(
                    [date_field, group_by],
                    {"count": "COUNT(*)"},
                    query_filters
                )
            else:
                return []
                
        except Exception as e:
            self.logger.error(f"Error getting trends for {self.dataset_name}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to get trends for {self.dataset_name}",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    # Validation Service Implementation
    async def validate(self, dto: Any) -> List[str]:
        """Validate a single DTO."""
        try:
            errors = []
            
            # Pydantic validation
            if hasattr(dto, 'model_validate'):
                try:
                    dto.model_validate(dto.model_dump())
                except Exception as e:
                    errors.append(f"Schema validation: {str(e)}")
            
            # Convert to entity for business validation
            try:
                entity = self._dto_to_entity(dto)
                business_errors = await self.validate_business_rules(dto)
                errors.extend(business_errors)
            except Exception as e:
                errors.append(f"Entity conversion: {str(e)}")
            
            return errors
            
        except Exception as e:
            return [f"Validation error: {str(e)}"]
    
    async def validate_many(self, dtos: List[Any]) -> Dict[int, List[str]]:
        """Validate multiple DTOs."""
        results = {}
        
        for i, dto in enumerate(dtos):
            errors = await self.validate(dto)
            if errors:
                results[i] = errors
        
        return results
    
    async def validate_business_rules(self, dto: Any) -> List[str]:
        """Validate business rules."""
        try:
            entity = self._dto_to_entity(dto)
            if hasattr(entity, 'validate'):
                return entity.validate()
            return []
        except Exception as e:
            return [f"Business rule validation error: {str(e)}"]
    
    # Helper methods
    def _entity_to_dto(self, entity: BaseEntity) -> Any:
        """Convert entity to DTO."""
        try:
            entity_dict = entity.to_dict()
            return self.schema_class(**entity_dict)
        except Exception as e:
            self.logger.error(f"Error converting entity to DTO: {str(e)}")
            raise
    
    def _dto_to_entity(self, dto: Any) -> BaseEntity:
        """Convert DTO to entity."""
        try:
            if hasattr(dto, 'model_dump'):
                data = dto.model_dump()
            else:
                data = dto.__dict__
            
            return DatasetFactory.create_entity(self.dataset_name, **data)
        except Exception as e:
            self.logger.error(f"Error converting DTO to entity: {str(e)}")
            raise
    
    def _external_data_to_dto(self, data: Dict[str, Any]) -> Any:
        """Convert external data to DTO."""
        try:
            # Map external field names to internal field names
            mapped_data = {}
            field_mapping = dict(zip(self.config.odata_select_fields, [f.name for f in self.config.fields]))
            
            for external_field, internal_field in field_mapping.items():
                if external_field in data:
                    mapped_data[internal_field] = data[external_field]
            
            return self.schema_class(**mapped_data)
        except Exception as e:
            self.logger.error(f"Error converting external data to DTO: {str(e)}")
            raise
    
    def _dict_to_query_filters(self, filters: Dict[str, Any]) -> List[QueryFilter]:
        """Convert dictionary filters to QueryFilter objects."""
        query_filters = []
        
        for field, value in filters.items():
            if isinstance(value, dict):
                # Handle complex filters like {"gte": 100}
                for operator, filter_value in value.items():
                    query_filters.append(QueryFilter(field=field, operator=operator, value=filter_value))
            else:
                # Simple equality filter
                query_filters.append(QueryFilter(field=field, operator="eq", value=value))
        
        return query_filters 