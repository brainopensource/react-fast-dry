"""
Improved well production API routes with standardized error handling and dependency injection.
Uses proper exception handling and standardized response formats.
"""
import logging
import time
import uuid
from typing import Optional
from datetime import datetime
import asyncio

from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.responses import FileResponse

# Updated service imports
from ...application.services.well_production_import_service import WellProductionImportService
from ...application.services.well_production_query_service import WellProductionQueryService
from ...application.services.odata_well_production_import_service import ODataWellProductionImportService

# Updated dependency imports
from ...shared.dependencies import (
    provide_well_production_import_service,
    provide_well_production_query_service,
    provide_odata_well_production_import_service,
)
from ...shared.exceptions import (
    ApplicationException, 
    ValidationException,
    BusinessRuleViolationException,
    ErrorCode
)
from ...shared.responses import ResponseBuilder, SuccessResponse, ErrorResponse
from ...shared.job_manager import JobManager, JobStatus
from ...shared.utils.timing_decorator import async_timed
from ...shared.config.settings import get_settings

# Configure logging
logger = logging.getLogger(__name__)

# Get settings instance
settings = get_settings()

router = APIRouter(prefix="/api/v1/wells", tags=["wells"])

# Create a singleton instance
job_manager = JobManager()


async def get_request_id(request: Request) -> str:
    """Generate or extract request ID for tracing"""
    return request.headers.get("X-Request-ID", str(uuid.uuid4()))


@router.get("/import/trigger", status_code=status.HTTP_200_OK, response_model=SuccessResponse)
async def trigger_import_well_production(
    request: Request,
    service: WellProductionImportService = Depends(provide_well_production_import_service),
    request_id: str = Depends(get_request_id)
):
    """Trigger well production data import with a simple GET request."""
    try:
        # Try to create a new job
        job_id = await job_manager.create_job()
        if not job_id:
            error = BusinessRuleViolationException(
                message="An import is already in progress. Please wait for it to complete.",
                rule="SINGLE_IMPORT_RULE"
            )
            return ResponseBuilder.error(error, request_id=request_id)

        # Start import in background
        asyncio.create_task(run_import(job_id, service))
        
        return ResponseBuilder.success(
            data={"job_id": job_id},
            message="Import started successfully"
        )
        
    except TypeError as e:
        logger.error(f"Type error in import trigger {request_id}: {str(e)}")
        error = ApplicationException(
            message=f"Error creating import job: {str(e)}",
            error_code=ErrorCode.USE_CASE_ERROR
        )
        return ResponseBuilder.error(error, request_id=request_id)
    except Exception as e:
        logger.error(f"Error in import trigger {request_id}: {str(e)}")
        error = ApplicationException(
            message=f"Unexpected error: {str(e)}",
            error_code=ErrorCode.INTERNAL_ERROR
        )
        return ResponseBuilder.error(error, request_id=request_id)

async def run_import(job_id: str, service: WellProductionImportService):
    """Run the import process and update job status"""
    try:
        # Update job status to running
        await job_manager.update_job(job_id, status=JobStatus.RUNNING)
        
        # Run import
        result = await service.import_production_data(
            filters=None,
            batch_id=job_id
        )
        
        # Update job with results
        await job_manager.update_job(
            job_id,
            status=JobStatus.COMPLETED,
            total_records=result.total_items,
            new_records=result.processed_items,
            duplicate_records=result.metadata.get('duplicate_records', 0)
        )
        
    except Exception as e:
        # Update job with error
        await job_manager.update_job(
            job_id,
            status=JobStatus.FAILED,
            error=str(e)
        )

@router.get("/import/status/{job_id}", status_code=status.HTTP_200_OK)
async def get_import_status(
    job_id: str,
    request: Request,
    request_id: str = Depends(get_request_id)
):
    """Get the status of an import job"""
    start_time = time.time()
    
    try:
        job_status = job_manager.get_job_status(job_id)
        if not job_status:
            error = ValidationException(
                message="Job not found",
                field="job_id",
                value=job_id,
                status_code_override=status.HTTP_404_NOT_FOUND
            )
            return ResponseBuilder.error(error, request_id=request_id)
        
        # Calculate execution time based on job timing
        execution_time_ms = None
        if job_status.get('started_at') and job_status.get('completed_at'):
            # Job is completed, calculate total execution time
            execution_time_ms = (job_status['completed_at'] - job_status['started_at']) * 1000
        elif job_status.get('started_at'):
            # Job is still running, calculate current execution time
            execution_time_ms = (time.time() - job_status['started_at']) * 1000
        
        # Add execution time to the job status data
        enhanced_status = {
            **job_status,
            'execution_time_seconds': execution_time_ms / 1000
        }
        
        response_execution_time_ms = (time.time() - start_time) * 1000
        
        return ResponseBuilder.success(
            data=enhanced_status,
            message="Job status retrieved successfully",
            request_id=request_id,
            execution_time_ms=response_execution_time_ms
        )
        
    except Exception as e:
        logger.error(f"Unexpected error getting job status {request_id}: {str(e)}", exc_info=True)
        error = ApplicationException(
            message=f"Unexpected error getting job status: {str(e)}",
            cause=e
        )
        return ResponseBuilder.error(error, request_id=request_id)


@router.get(
    "/download",
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": "Successful CSV export of well production data.",
        },
        404: {
            "model": ErrorResponse,
            "description": "Data not found if no data is available for export.",
        },
        500: {
            "model": ErrorResponse,
            "description": "Internal server error if any unexpected issue occurs.",
        },
    }
)
@async_timed
async def download_well_production(
    request: Request,
    service: WellProductionQueryService = Depends(provide_well_production_query_service),
    request_id: str = Depends(get_request_id)
):
    """
    Download the well production data as CSV.
    
    This endpoint exports the latest data from the primary storage to CSV format.
    """
    try:
        logger.info(f"Starting download request {request_id}")
        
        # Export to CSV using the service
        csv_path = await service.export_to_csv()
        
        if not csv_path.exists():
            raise ValidationException(
                message="No well production data available for download",
                field="csv_export",
                status_code_override=status.HTTP_404_NOT_FOUND
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
        # ValidationException will also be caught here
        return ResponseBuilder.error(e, request_id=request_id)
        
    except Exception as e:
        logger.error(f"Unexpected error in download {request_id}: {str(e)}", exc_info=True)
        error = ApplicationException(
            message=f"Unexpected error during download: {str(e)}",
            cause=e
        )
        return ResponseBuilder.error(error, request_id=request_id)


@router.get("/well/{well_code}", response_model=SuccessResponse)
async def get_well_by_code(
    well_code: int,
    request: Request,
    period_start: Optional[str] = None,
    period_end: Optional[str] = None,
    service: WellProductionQueryService = Depends(provide_well_production_query_service),
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
                value=well_code,
                status_code_override=status.HTTP_404_NOT_FOUND
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
                    "water_production_kbd": well.water_production_kbd
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

@router.get("/import/run", status_code=status.HTTP_200_OK, response_model=SuccessResponse)
async def run_odata_import_well_production(
    request: Request,
    service: ODataWellProductionImportService = Depends(provide_odata_well_production_import_service),
    request_id: str = Depends(get_request_id)
):
    """
    Run well production data import from external OData API with pagination support.
    
    This endpoint fetches data from an external OData API using Basic Authentication
    and handles pagination automatically until all data is retrieved.
    
    Features:
    - Automatic pagination handling (up to 999 records per request)
    - Basic Authentication support
    - Comprehensive error handling and retry logic
    - Data validation and deduplication
    - Background job processing with status tracking
    
    Returns:
        SuccessResponse with job_id for tracking import progress
    """
    try:
        # Try to create a new job
        job_id = await job_manager.create_job()
        if not job_id:
            error = BusinessRuleViolationException(
                message="An OData import is already in progress. Please wait for it to complete.",
                rule="SINGLE_ODATA_IMPORT_RULE"
            )
            return ResponseBuilder.error(error, request_id=request_id)

        # Start OData import in background
        asyncio.create_task(run_odata_import(job_id, service))
        
        return ResponseBuilder.success(
            data={
                "job_id": job_id,
                "import_type": "odata_api",
                "api_endpoint": "External OData API with pagination",
                "authentication": "Basic Auth",
                "max_records_per_request": settings.ODATA_MAX_RECORDS_PER_REQUEST
            },
            message="OData import started successfully"
        )
        
    except TypeError as e:
        logger.error(f"Type error in OData import trigger {request_id}: {str(e)}")
        error = ApplicationException(
            message=f"Error creating OData import job: {str(e)}",
            error_code=ErrorCode.USE_CASE_ERROR
        )
        return ResponseBuilder.error(error, request_id=request_id)
    except Exception as e:
        logger.error(f"Error in OData import trigger {request_id}: {str(e)}")
        error = ApplicationException(
            message=f"Unexpected error: {str(e)}",
            error_code=ErrorCode.INTERNAL_ERROR
        )
        return ResponseBuilder.error(error, request_id=request_id)

async def run_odata_import(job_id: str, service: ODataWellProductionImportService):
    """Run the OData import process and update job status"""
    try:
        # Update job status to running
        await job_manager.update_job(job_id, status=JobStatus.RUNNING)
        
        # Run OData import
        result = await service.import_production_data_from_odata(
            batch_id=job_id
        )
        
        # Update job with results
        await job_manager.update_job(
            job_id,
            status=JobStatus.COMPLETED,
            total_records=result.total_items,
            new_records=result.processed_items,
            duplicate_records=result.metadata.get('duplicate_records', 0)
        )
        
    except Exception as e:
        # Update job with error
        await job_manager.update_job(
            job_id,
            status=JobStatus.FAILED,
            error=str(e)
        ) 