"""
Improved well production API routes with standardized error handling and dependency injection.
Uses proper exception handling and standardized response formats.
"""
import logging
import time
import uuid
from typing import Optional
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.responses import FileResponse

from ...application.services.well_production_service import WellProductionService
from ...shared.dependencies import get_well_production_service
from ...shared.exceptions import ApplicationException, ValidationException
from ...shared.responses import ResponseBuilder, SuccessResponse, ErrorResponse, ApiResponse

# Configure logging
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/wells", tags=["wells"])


async def get_request_id(request: Request) -> str:
    """Generate or extract request ID for tracing"""
    return request.headers.get("X-Request-ID", str(uuid.uuid4()))


@router.post("/import", status_code=status.HTTP_201_CREATED, response_model=ApiResponse)
async def import_well_production(
    request: Request,
    filters: Optional[dict] = None,
    service: WellProductionService = Depends(get_well_production_service),
    request_id: str = Depends(get_request_id)
):
    """
    Import well production data from external source with batch processing.
    
    This endpoint imports data using optimized batch processing with memory management.
    Supports millions of records efficiently with comprehensive error handling.
    Accepts optional filters in the request body for selective importing.
    """
    start_time = time.time()
    
    try:
        logger.info(f"Starting import request {request_id}")
        
        # Generate batch ID for tracking
        batch_id = f"import_{request_id}_{int(time.time())}"
        
        # Import data using the service
        result = await service.import_production_data(
            filters=filters,
            batch_id=batch_id
        )
        
        execution_time_ms = (time.time() - start_time) * 1000
        
        return ResponseBuilder.success(
            data={
                "batch_result": result.model_dump(),
                "import_summary": {
                    "total_records": result.total_items,
                    "successful_records": result.processed_items,
                    "failed_records": result.failed_items,
                    "success_rate": result.success_rate,
                    "batch_id": batch_id
                },
                "performance": {
                    "execution_time_ms": execution_time_ms,
                    "memory_usage_mb": result.memory_usage_mb,
                    "throughput_records_per_second": (
                        result.processed_items / (execution_time_ms / 1000)
                        if execution_time_ms > 0 else 0
                    )
                }
            },
            message=f"Successfully imported {result.processed_items} out of {result.total_items} records",
            request_id=request_id,
            execution_time_ms=execution_time_ms
        )
        
    except ValidationException as e:
        logger.warning(f"Validation error in import {request_id}: {e.message}")
        return ResponseBuilder.error(e, request_id=request_id)
        
    except ApplicationException as e:
        logger.error(f"Application error in import {request_id}: {e.message}")
        return ResponseBuilder.error(e, request_id=request_id)
        
    except Exception as e:
        logger.error(f"Unexpected error in import {request_id}: {str(e)}", exc_info=True)
        error = ApplicationException(
            message=f"Unexpected error during import: {str(e)}",
            cause=e
        )
        return ResponseBuilder.error(error, request_id=request_id)


@router.get("/import/trigger", status_code=status.HTTP_200_OK, response_model=ApiResponse)
async def trigger_import_well_production(
    request: Request,
    service: WellProductionService = Depends(get_well_production_service),
    request_id: str = Depends(get_request_id)
):
    """
    Trigger well production data import with a simple GET request.
    
    This endpoint provides a simple way to trigger the import routine without 
    requiring a POST request or filters. Imports all available data from the 
    external source using the same optimized batch processing.
    """
    start_time = time.time()
    
    try:
        logger.info(f"Starting GET import trigger request {request_id}")
        
        # Generate batch ID for tracking
        batch_id = f"import_trigger_{request_id}_{int(time.time())}"
        
        # Import data using the service (no filters = import all)
        result = await service.import_production_data(
            filters=None,
            batch_id=batch_id
        )
        
        execution_time_ms = (time.time() - start_time) * 1000
        
        return ResponseBuilder.success(
            data={
                "batch_result": result.model_dump(),
                "import_summary": {
                    "total_records": result.total_items,
                    "successful_records": result.processed_items,
                    "failed_records": result.failed_items,
                    "success_rate": result.success_rate,
                    "batch_id": batch_id
                },
                "performance": {
                    "execution_time_ms": execution_time_ms,
                    "memory_usage_mb": result.memory_usage_mb,
                    "throughput_records_per_second": (
                        result.processed_items / (execution_time_ms / 1000)
                        if execution_time_ms > 0 else 0
                    )
                }
            },
            message=f"Successfully imported {result.processed_items} out of {result.total_items} records",
            request_id=request_id,
            execution_time_ms=execution_time_ms
        )
        
    except ValidationException as e:
        logger.warning(f"Validation error in import trigger {request_id}: {e.message}")
        return ResponseBuilder.error(e, request_id=request_id)
        
    except ApplicationException as e:
        logger.error(f"Application error in import trigger {request_id}: {e.message}")
        return ResponseBuilder.error(e, request_id=request_id)
        
    except Exception as e:
        logger.error(f"Unexpected error in import trigger {request_id}: {str(e)}", exc_info=True)
        error = ApplicationException(
            message=f"Unexpected error during import: {str(e)}",
            cause=e
        )
        return ResponseBuilder.error(error, request_id=request_id)


@router.get("/download", response_class=FileResponse)
async def download_well_production(
    request: Request,
    service: WellProductionService = Depends(get_well_production_service),
    request_id: str = Depends(get_request_id)
):
    """
    Download the well production data as CSV.
    
    This endpoint exports the latest data from the primary storage to CSV format.
    """
    try:
        logger.info(f"Starting download request {request_id}")
        
        # Get repository and export to CSV
        repository = service.repository
        csv_path = await repository.export_to_csv()
        
        if not csv_path.exists():
            raise ValidationException(
                message="No well production data available for download",
                field="csv_export"
            )
        
        return FileResponse(
            path=csv_path,
            filename="wells_production.csv",
            media_type="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=wells_production.csv",
                "X-Request-ID": request_id
            }
        )
        
    except ApplicationException as e:
        logger.error(f"Error in download {request_id}: {e.message}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=e.to_dict()
        )
        
    except Exception as e:
        logger.error(f"Unexpected error in download {request_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": True,
                "error_code": "INTERNAL_ERROR",
                "message": f"Unexpected error during download: {str(e)}",
                "request_id": request_id
            }
        )


@router.get("/stats", response_model=SuccessResponse)
async def get_well_production_stats(
    request: Request,
    service: WellProductionService = Depends(get_well_production_service),
    request_id: str = Depends(get_request_id)
):
    """Get comprehensive statistics about the well production data."""
    start_time = time.time()
    
    try:
        logger.info(f"Getting stats for request {request_id}")
        
        stats = await service.get_production_statistics()
        execution_time_ms = (time.time() - start_time) * 1000
        
        return ResponseBuilder.success(
            data=stats,
            message="Statistics retrieved successfully",
            request_id=request_id,
            execution_time_ms=execution_time_ms
        )
        
    except ApplicationException as e:
        logger.error(f"Error getting stats {request_id}: {e.message}")
        return ResponseBuilder.error(e, request_id=request_id)
        
    except Exception as e:
        logger.error(f"Unexpected error getting stats {request_id}: {str(e)}", exc_info=True)
        error = ApplicationException(
            message=f"Unexpected error getting statistics: {str(e)}",
            cause=e
        )
        return ResponseBuilder.error(error, request_id=request_id)


@router.get("/well/{well_code}", response_model=SuccessResponse)
async def get_well_by_code(
    well_code: int,
    request: Request,
    period_start: Optional[str] = None,
    period_end: Optional[str] = None,
    service: WellProductionService = Depends(get_well_production_service),
    request_id: str = Depends(get_request_id)
):
    """Get well production data by well code with optional date filtering."""
    start_time = time.time()
    
    try:
        logger.info(f"Getting well {well_code} for request {request_id}")
        
        # Parse date parameters if provided
        period_start_dt = None
        period_end_dt = None
        
        if period_start:
            try:
                period_start_dt = datetime.fromisoformat(period_start)
            except ValueError:
                raise ValidationException(
                    message="Invalid period_start format. Use ISO format (YYYY-MM-DD)",
                    field="period_start",
                    value=period_start
                )
        
        if period_end:
            try:
                period_end_dt = datetime.fromisoformat(period_end)
            except ValueError:
                raise ValidationException(
                    message="Invalid period_end format. Use ISO format (YYYY-MM-DD)",
                    field="period_end",
                    value=period_end
                )
        
        wells = await service.get_production_by_well(
            well_code=well_code,
            period_start=period_start_dt,
            period_end=period_end_dt
        )
        
        if not wells:
            raise ValidationException(
                message=f"Well with code {well_code} not found",
                field="well_code",
                value=well_code
            )
        
        execution_time_ms = (time.time() - start_time) * 1000
        
        # Convert wells to response format
        wells_data = []
        for well in wells:
            wells_data.append({
                "well_code": well.well_code,
                "well_name": well.well_name,
                "field_name": well.field_name,
                "production_data": {
                    "oil_production_kbd": well.oil_production_kbd,
                    "gas_production_mmcfd": well.gas_production_mmcfd,
                    "liquids_production_kbd": well.liquids_production_kbd,
                    "water_production_kbd": well.water_production_kbd,
                    "total_production_kbd": well.calculate_total_production(),
                    "is_producing": well.is_producing()
                },
                "metadata": {
                    "production_period": well.production_period,
                    "days_on_production": well.days_on_production,
                    "data_source": well.data_source
                }
            })
        
        return ResponseBuilder.success(
            data={
                "well_code": well_code,
                "records_found": len(wells_data),
                "wells": wells_data
            },
            message=f"Found {len(wells_data)} records for well {well_code}",
            request_id=request_id,
            execution_time_ms=execution_time_ms
        )
        
    except ValidationException as e:
        logger.warning(f"Validation error for well {well_code} {request_id}: {e.message}")
        return ResponseBuilder.error(e, request_id=request_id)
        
    except ApplicationException as e:
        logger.error(f"Error getting well {well_code} {request_id}: {e.message}")
        return ResponseBuilder.error(e, request_id=request_id)
        
    except Exception as e:
        logger.error(f"Unexpected error getting well {well_code} {request_id}: {str(e)}", exc_info=True)
        error = ApplicationException(
            message=f"Unexpected error getting well: {str(e)}",
            cause=e
        )
        return ResponseBuilder.error(error, request_id=request_id)


@router.get("/field/{field_code}", response_model=SuccessResponse)
async def get_wells_by_field(
    field_code: int,
    request: Request,
    limit: Optional[int] = None,
    service: WellProductionService = Depends(get_well_production_service),
    request_id: str = Depends(get_request_id)
):
    """Get all wells for a specific field with optional limit."""
    start_time = time.time()
    
    try:
        logger.info(f"Getting field {field_code} wells for request {request_id}")
        
        wells = await service.get_production_by_field(
            field_code=field_code,
            limit=limit
        )
        
        if not wells:
            raise ValidationException(
                message=f"No wells found for field code {field_code}",
                field="field_code",
                value=field_code
            )
        
        execution_time_ms = (time.time() - start_time) * 1000
        
        # Prepare summary data
        field_summary = {
            "field_code": field_code,
            "field_name": wells[0].field_name if wells else None,
            "wells_count": len(wells),
            "total_production": sum(well.calculate_total_production() for well in wells),
            "active_wells": sum(1 for well in wells if well.is_producing())
        }
        
        # Prepare wells data
        wells_data = []
        for well in wells:
            wells_data.append({
                "well_code": well.well_code,
                "well_name": well.well_name,
                "production_period": well.production_period,
                "total_production_kbd": well.calculate_total_production(),
                "is_producing": well.is_producing()
            })
        
        return ResponseBuilder.success(
            data={
                "field_summary": field_summary,
                "wells": wells_data
            },
            message=f"Found {len(wells)} wells for field {field_code}",
            request_id=request_id,
            execution_time_ms=execution_time_ms
        )
        
    except ValidationException as e:
        logger.warning(f"Validation error for field {field_code} {request_id}: {e.message}")
        return ResponseBuilder.error(e, request_id=request_id)
        
    except ApplicationException as e:
        logger.error(f"Error getting field {field_code} {request_id}: {e.message}")
        return ResponseBuilder.error(e, request_id=request_id)
        
    except Exception as e:
        logger.error(f"Unexpected error getting field {field_code} {request_id}: {str(e)}", exc_info=True)
        error = ApplicationException(
            message=f"Unexpected error getting field wells: {str(e)}",
            cause=e
        )
        return ResponseBuilder.error(error, request_id=request_id) 