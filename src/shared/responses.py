"""
Standardized response models for consistent API responses.
Provides typed, structured responses for success and error cases.
"""
from typing import Optional, Dict, Any, Generic, TypeVar, List, Union
from pydantic import BaseModel
from datetime import datetime
from fastapi import status # Added
from fastapi.responses import JSONResponse # Added
from .exceptions import ApplicationException, ErrorCode

T = TypeVar('T')


class ResponseMetadata(BaseModel):
    """Metadata for API responses"""
    timestamp: datetime
    request_id: Optional[str] = None
    version: str = "1.0.0"
    execution_time_ms: Optional[float] = None


class SuccessResponse(BaseModel, Generic[T]):
    """Standardized success response"""
    success: bool = True
    data: T
    metadata: ResponseMetadata
    message: Optional[str] = None


class ErrorDetail(BaseModel):
    """Detailed error information"""
    error_code: str
    message: str
    context: Optional[Dict[str, Any]] = None
    field: Optional[str] = None


class ErrorResponse(BaseModel):
    """Standardized error response"""
    success: bool = False
    error: ErrorDetail
    metadata: ResponseMetadata
    trace_id: Optional[str] = None


# Union type for endpoints that can return either success or error
ApiResponse = Union[SuccessResponse, ErrorResponse]


class BatchResult(BaseModel):
    """Result of batch processing operations"""
    batch_id: str
    total_items: int
    processed_items: int
    failed_items: int
    success_rate: float
    errors: List[ErrorDetail] = []
    execution_time_ms: float
    memory_usage_mb: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None


class PaginatedResponse(BaseModel, Generic[T]):
    """Paginated response for large datasets"""
    items: List[T]
    total_count: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_previous: bool


class ResponseBuilder:
    """Builder for creating standardized responses"""
    
    @staticmethod
    def success(
        data: Any,
        message: Optional[str] = None,
        request_id: Optional[str] = None,
        execution_time_ms: Optional[float] = None
    ) -> SuccessResponse:
        """Build a success response"""
        return SuccessResponse(
            data=data,
            message=message,
            metadata=ResponseMetadata(
                timestamp=datetime.utcnow(),
                request_id=request_id,
                execution_time_ms=execution_time_ms
            )
        )
    
    @staticmethod
    def error(
        exception: ApplicationException,
        request_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> JSONResponse: # Return type changed
        """Build an error response from an exception"""
        error_payload = ErrorResponse(
            error=ErrorDetail(
                error_code=exception.error_code.value,
                message=exception.message,
                context=exception.context
            ),
            metadata=ResponseMetadata(
                timestamp=datetime.utcnow(),
                request_id=request_id
            ),
            trace_id=trace_id
        )
        return JSONResponse(
            status_code=exception.http_status_code,
            content=error_payload.model_dump()
        )
    
    @staticmethod
    def validation_error(
        message: str,
        field: Optional[str] = None,
        request_id: Optional[str] = None
    ) -> ErrorResponse:
        """Build a validation error response"""
        return ErrorResponse(
            error=ErrorDetail(
                error_code=ErrorCode.VALIDATION_ERROR.value,
                message=message,
                field=field
            ),
            metadata=ResponseMetadata(
                timestamp=datetime.utcnow(),
                request_id=request_id
            )
        )
    
    @staticmethod
    def batch_result(
        batch_id: str,
        total_items: int,
        processed_items: int,
        failed_items: int,
        execution_time_ms: float,
        errors: List[ErrorDetail] = None,
        memory_usage_mb: Optional[float] = None
    ) -> BatchResult:
        """Build a batch processing result"""
        success_rate = (processed_items / total_items * 100) if total_items > 0 else 0
        
        return BatchResult(
            batch_id=batch_id,
            total_items=total_items,
            processed_items=processed_items,
            failed_items=failed_items,
            success_rate=success_rate,
            errors=errors or [],
            execution_time_ms=execution_time_ms,
            memory_usage_mb=memory_usage_mb
        )
    
    @staticmethod
    def paginated(
        items: List[Any],
        total_count: int,
        page: int,
        page_size: int
    ) -> PaginatedResponse:
        """Build a paginated response"""
        total_pages = (total_count + page_size - 1) // page_size
        
        return PaginatedResponse(
            items=items,
            total_count=total_count,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_previous=page > 1
        ) 