"""
Generic dataset router that can handle any dataset based on configuration.
Implements DRY principles and follows REST conventions.
"""

import logging
import time
import uuid
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status, Request, Query, Path, Body
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from ...domain.ports.services import (
    IDataManagementService, ImportOptions, ExportOptions
)
from ...domain.ports.repository import (
    QueryOptions, QueryFilter, QuerySort, QueryPagination
)
from ...shared.config.schemas import (
    DatasetSchemaConfig, DatasetFactory, get_dataset_config, get_available_datasets
)
from ...shared.exceptions import (
    ApplicationException, ValidationException, BusinessRuleViolationException,
    ErrorCode
)
from ...shared.responses import ResponseBuilder, SuccessResponse, ErrorResponse
from ...shared.job_manager import JobManager, JobStatus
from ...shared.utils.timing_decorator import async_timed

logger = logging.getLogger(__name__)

# Generic request/response models
class GenericFilter(BaseModel):
    """Generic filter model for API requests."""
    field: str
    operator: str = "eq"  # eq, ne, gt, gte, lt, lte, like, in
    value: Any

class GenericSort(BaseModel):
    """Generic sort model for API requests."""
    field: str
    direction: str = "ASC"  # ASC, DESC

class GenericQueryParams(BaseModel):
    """Generic query parameters for API requests."""
    filters: Optional[List[GenericFilter]] = None
    sorts: Optional[List[GenericSort]] = None
    page: int = 1
    page_size: int = 100
    search: Optional[str] = None

class GenericImportRequest(BaseModel):
    """Generic import request model."""
    filters: Optional[Dict[str, Any]] = None
    batch_size: int = 1000
    skip_duplicates: bool = True
    validate_data: bool = True

class GenericExportRequest(BaseModel):
    """Generic export request model."""
    format: str = "csv"
    filters: Optional[Dict[str, Any]] = None
    columns: Optional[List[str]] = None

class DatasetRouter:
    """
    Generic router factory for datasets.
    Creates FastAPI routers for any dataset based on configuration.
    """
    
    def __init__(self, job_manager: JobManager):
        """Initialize the router factory."""
        self.job_manager = job_manager
        self.logger = logging.getLogger(__name__)
    
    def create_router(
        self,
        dataset_name: str,
        service_factory: callable,  # Function that returns IDataManagementService
        config: Optional[DatasetSchemaConfig] = None
    ) -> APIRouter:
        """Create a router for a specific dataset."""
        
        config = config or get_dataset_config(dataset_name)
        router = APIRouter(
            prefix=config.api_prefix,
            tags=config.api_tags,
            responses={
                404: {"model": ErrorResponse},
                422: {"model": ErrorResponse},
                500: {"model": ErrorResponse}
            }
        )
        
        # Dynamic schema classes
        schema_class = DatasetFactory.get_schema_class(dataset_name)
        create_schema_class = type(f"{dataset_name.title()}CreateSchema", (schema_class,), {})
        update_schema_class = type(f"{dataset_name.title()}UpdateSchema", (schema_class,), {})
        
        # Helper function to get request ID
        async def get_request_id(request: Request) -> str:
            """Generate or extract request ID for tracing."""
            return request.headers.get("X-Request-ID", str(uuid.uuid4()))
        
        # Helper function to convert query params to QueryOptions
        def create_query_options(
            page: int = 1,
            page_size: int = 100,
            filters: Optional[List[GenericFilter]] = None,
            sorts: Optional[List[GenericSort]] = None
        ) -> QueryOptions:
            """Convert API parameters to QueryOptions."""
            
            query_filters = []
            if filters:
                query_filters = [
                    QueryFilter(field=f.field, operator=f.operator, value=f.value)
                    for f in filters
                ]
            
            query_sorts = []
            if sorts:
                query_sorts = [
                    QuerySort(field=s.field, direction=s.direction)
                    for s in sorts
                ]
            
            pagination = QueryPagination(
                offset=(page - 1) * page_size,
                limit=page_size
            )
            
            return QueryOptions(
                filters=query_filters,
                sorts=query_sorts,
                pagination=pagination
            )
        
        # CRUD Endpoints
        @router.get("/", response_model=SuccessResponse)
        @async_timed
        async def get_all_items(
            request: Request,
            page: int = Query(1, ge=1, description="Page number"),
            page_size: int = Query(100, ge=1, le=1000, description="Items per page"),
            search: Optional[str] = Query(None, description="Search query"),
            request_id: str = Depends(get_request_id)
        ):
            """Get all items with pagination, filtering, and search."""
            try:
                service = service_factory()
                
                if search:
                    result = await service.search(
                        search,
                        create_query_options(page, page_size)
                    )
                else:
                    result = await service.get_all(
                        create_query_options(page, page_size)
                    )
                
                return ResponseBuilder.success(
                    data={
                        "items": [item.model_dump() if hasattr(item, 'model_dump') else item.__dict__ for item in result.items],
                        "pagination": {
                            "page": result.page,
                            "page_size": result.page_size,
                            "total_count": result.total_count,
                            "total_pages": result.total_pages,
                            "has_next": result.has_next,
                            "has_previous": result.has_previous
                        }
                    },
                    message=f"Retrieved {len(result.items)} {config.display_name.lower()} records",
                    request_id=request_id
                )
                
            except Exception as e:
                self.logger.error(f"Error getting {dataset_name}: {str(e)}")
                error = ApplicationException(
                    message=f"Failed to retrieve {config.display_name.lower()}",
                    cause=e
                )
                return ResponseBuilder.error(error, request_id=request_id)
        
        @router.get("/{item_id}", response_model=SuccessResponse)
        @async_timed
        async def get_item_by_id(
            item_id: str = Path(..., description="Item ID"),
            request: Request = None,
            request_id: str = Depends(get_request_id)
        ):
            """Get item by ID."""
            try:
                service = service_factory()
                item = await service.get_by_id(item_id)
                
                if not item:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"{config.display_name} not found"
                    )
                
                return ResponseBuilder.success(
                    data=item.model_dump() if hasattr(item, 'model_dump') else item.__dict__,
                    message=f"{config.display_name} retrieved successfully",
                    request_id=request_id
                )
                
            except HTTPException:
                raise
            except Exception as e:
                self.logger.error(f"Error getting {dataset_name} by ID {item_id}: {str(e)}")
                error = ApplicationException(
                    message=f"Failed to retrieve {config.display_name.lower()}",
                    cause=e
                )
                return ResponseBuilder.error(error, request_id=request_id)
        
        @router.post("/", status_code=status.HTTP_201_CREATED, response_model=SuccessResponse)
        @async_timed
        async def create_item(
            item: create_schema_class,
            request: Request,
            request_id: str = Depends(get_request_id)
        ):
            """Create a new item."""
            try:
                service = service_factory()
                result = await service.create(item)
                
                if result.success:
                    return ResponseBuilder.success(
                        data=result.data.model_dump() if hasattr(result.data, 'model_dump') else result.data.__dict__,
                        message=result.message,
                        request_id=request_id
                    )
                else:
                    return ResponseBuilder.error(
                        ValidationException(
                            message=result.message,
                            validation_errors=result.errors
                        ),
                        request_id=request_id
                    )
                
            except Exception as e:
                self.logger.error(f"Error creating {dataset_name}: {str(e)}")
                error = ApplicationException(
                    message=f"Failed to create {config.display_name.lower()}",
                    cause=e
                )
                return ResponseBuilder.error(error, request_id=request_id)
        
        @router.put("/{item_id}", response_model=SuccessResponse)
        @async_timed
        async def update_item(
            item_id: str = Path(..., description="Item ID"),
            item: update_schema_class = Body(...),
            request: Request = None,
            request_id: str = Depends(get_request_id)
        ):
            """Update an existing item."""
            try:
                service = service_factory()
                result = await service.update(item_id, item)
                
                if result.success:
                    return ResponseBuilder.success(
                        data=result.data.model_dump() if hasattr(result.data, 'model_dump') else result.data.__dict__,
                        message=result.message,
                        request_id=request_id
                    )
                else:
                    return ResponseBuilder.error(
                        ValidationException(
                            message=result.message,
                            validation_errors=result.errors
                        ),
                        request_id=request_id
                    )
                
            except Exception as e:
                self.logger.error(f"Error updating {dataset_name} {item_id}: {str(e)}")
                error = ApplicationException(
                    message=f"Failed to update {config.display_name.lower()}",
                    cause=e
                )
                return ResponseBuilder.error(error, request_id=request_id)
        
        @router.delete("/{item_id}", response_model=SuccessResponse)
        @async_timed
        async def delete_item(
            item_id: str = Path(..., description="Item ID"),
            request: Request = None,
            request_id: str = Depends(get_request_id)
        ):
            """Delete an item."""
            try:
                service = service_factory()
                result = await service.delete(item_id)
                
                return ResponseBuilder.success(
                    data={"deleted": result.data},
                    message=result.message,
                    request_id=request_id
                )
                
            except Exception as e:
                self.logger.error(f"Error deleting {dataset_name} {item_id}: {str(e)}")
                error = ApplicationException(
                    message=f"Failed to delete {config.display_name.lower()}",
                    cause=e
                )
                return ResponseBuilder.error(error, request_id=request_id)
        
        # Import endpoints
        @router.post("/import", status_code=status.HTTP_201_CREATED, response_model=SuccessResponse)
        @async_timed
        async def import_data(
            request: Request,
            import_request: GenericImportRequest = Body(...),
            request_id: str = Depends(get_request_id)
        ):
            """Import data from external source."""
            try:
                service = service_factory()
                
                import_options = ImportOptions(
                    batch_size=import_request.batch_size,
                    skip_duplicates=import_request.skip_duplicates,
                    validate_data=import_request.validate_data,
                    filters=import_request.filters
                )
                
                result = await service.import_from_external(import_options)
                
                return ResponseBuilder.success(
                    data={
                        "import_result": {
                            "total_items": result.total_items,
                            "successful_items": result.successful_items,
                            "failed_items": result.failed_items,
                            "success_rate": result.success_rate,
                            "processing_time_seconds": result.processing_time_seconds
                        }
                    },
                    message=f"Import completed: {result.successful_items}/{result.total_items} records imported",
                    request_id=request_id
                )
                
            except Exception as e:
                self.logger.error(f"Error importing {dataset_name}: {str(e)}")
                error = ApplicationException(
                    message=f"Failed to import {config.display_name.lower()}",
                    cause=e
                )
                return ResponseBuilder.error(error, request_id=request_id)
        
        @router.get("/import/trigger", response_model=SuccessResponse)
        async def trigger_import(
            request: Request,
            request_id: str = Depends(get_request_id)
        ):
            """Trigger background import."""
            try:
                job_id = await self.job_manager.create_job()
                if not job_id:
                    error = BusinessRuleViolationException(
                        message="An import is already in progress. Please wait for it to complete.",
                        rule="SINGLE_IMPORT_RULE"
                    )
                    return ResponseBuilder.error(error, request_id=request_id)
                
                # Start import in background
                asyncio.create_task(self._run_background_import(job_id, service_factory, dataset_name))
                
                return ResponseBuilder.success(
                    data={"job_id": job_id},
                    message="Import started successfully",
                    request_id=request_id
                )
                
            except Exception as e:
                self.logger.error(f"Error triggering import for {dataset_name}: {str(e)}")
                error = ApplicationException(
                    message=f"Failed to trigger import for {config.display_name.lower()}",
                    cause=e
                )
                return ResponseBuilder.error(error, request_id=request_id)
        
        @router.get("/import/status/{job_id}", response_model=SuccessResponse)
        async def get_import_status(
            job_id: str = Path(..., description="Job ID"),
            request: Request = None,
            request_id: str = Depends(get_request_id)
        ):
            """Get import job status."""
            try:
                job = await self.job_manager.get_job(job_id)
                if not job:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Job not found"
                    )
                
                return ResponseBuilder.success(
                    data=job,
                    message="Job status retrieved successfully",
                    request_id=request_id
                )
                
            except HTTPException:
                raise
            except Exception as e:
                self.logger.error(f"Error getting import status {job_id}: {str(e)}")
                error = ApplicationException(
                    message="Failed to retrieve import status",
                    cause=e
                )
                return ResponseBuilder.error(error, request_id=request_id)
        
        # Export endpoints (if enabled)
        if config.enable_download:
            @router.get("/export", response_class=FileResponse)
            @async_timed
            async def export_data(
                request: Request,
                format: str = Query("csv", description="Export format"),
                request_id: str = Depends(get_request_id)
            ):
                """Export data to file."""
                try:
                    service = service_factory()
                    
                    export_options = ExportOptions(format=format)
                    result = await service.export_to_file(export_options)
                    
                    return FileResponse(
                        path=result.file_path,
                        filename=f"{dataset_name}_{int(time.time())}.{format}",
                        media_type="application/octet-stream"
                    )
                    
                except Exception as e:
                    self.logger.error(f"Error exporting {dataset_name}: {str(e)}")
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=f"Failed to export {config.display_name.lower()}"
                    )
        
        # Statistics endpoint (if enabled)
        if config.enable_stats:
            @router.get("/stats", response_model=SuccessResponse)
            @async_timed
            async def get_statistics(
                request: Request,
                request_id: str = Depends(get_request_id)
            ):
                """Get dataset statistics."""
                try:
                    service = service_factory()
                    stats = await service.get_statistics()
                    
                    return ResponseBuilder.success(
                        data={
                            "dataset_name": stats.dataset_name,
                            "total_records": stats.total_records,
                            "active_records": stats.active_records,
                            "last_updated": stats.last_updated.isoformat() if stats.last_updated else None,
                            "aggregates": stats.aggregates,
                            "field_statistics": stats.field_statistics
                        },
                        message=f"Statistics for {config.display_name} retrieved successfully",
                        request_id=request_id
                    )
                    
                except Exception as e:
                    self.logger.error(f"Error getting statistics for {dataset_name}: {str(e)}")
                    error = ApplicationException(
                        message=f"Failed to get statistics for {config.display_name.lower()}",
                        cause=e
                    )
                    return ResponseBuilder.error(error, request_id=request_id)
        
        return router
    
    async def _run_background_import(self, job_id: str, service_factory: callable, dataset_name: str):
        """Run import in background and update job status."""
        try:
            await self.job_manager.update_job(job_id, status=JobStatus.RUNNING)
            
            service = service_factory()
            result = await service.import_from_external()
            
            await self.job_manager.update_job(
                job_id,
                status=JobStatus.COMPLETED,
                result={
                    "total_items": result.total_items,
                    "successful_items": result.successful_items,
                    "failed_items": result.failed_items,
                    "success_rate": result.success_rate
                }
            )
            
        except Exception as e:
            self.logger.error(f"Background import failed for {dataset_name}: {str(e)}")
            await self.job_manager.update_job(
                job_id,
                status=JobStatus.FAILED,
                error=str(e)
            )

def create_dataset_router(
    dataset_name: str,
    service_factory: callable,
    job_manager: JobManager,
    config: Optional[DatasetSchemaConfig] = None
) -> APIRouter:
    """Convenience function to create a dataset router."""
    router_factory = DatasetRouter(job_manager)
    return router_factory.create_router(dataset_name, service_factory, config)

# Main datasets router that includes all configured datasets
def create_main_datasets_router(
    service_factory_registry: Dict[str, callable],
    job_manager: JobManager
) -> APIRouter:
    """Create main router that includes all configured datasets."""
    
    main_router = APIRouter(prefix="/api/v1", tags=["datasets"])
    router_factory = DatasetRouter(job_manager)
    
    # Get list of available datasets
    @main_router.get("/datasets", response_model=SuccessResponse)
    async def list_datasets():
        """List all available datasets."""
        datasets = []
        for dataset_name in get_available_datasets():
            config = get_dataset_config(dataset_name)
            datasets.append({
                "name": dataset_name,
                "display_name": config.display_name,
                "description": config.description,
                "dataset_type": config.dataset_type,
                "api_prefix": config.api_prefix,
                "endpoints": {
                    "list": f"{config.api_prefix}/",
                    "create": f"{config.api_prefix}/",
                    "import": f"{config.api_prefix}/import",
                    "stats": f"{config.api_prefix}/stats" if config.enable_stats else None,
                    "export": f"{config.api_prefix}/export" if config.enable_download else None
                }
            })
        
        return ResponseBuilder.success(
            data={"datasets": datasets},
            message=f"Found {len(datasets)} available datasets"
        )
    
    # Create routers for each dataset
    for dataset_name, service_factory in service_factory_registry.items():
        try:
            dataset_router = router_factory.create_router(
                dataset_name,
                service_factory
            )
            main_router.include_router(dataset_router)
        except Exception as e:
            logger.error(f"Failed to create router for dataset {dataset_name}: {str(e)}")
    
    return main_router 