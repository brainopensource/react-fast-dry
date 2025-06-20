"""
Standardized exception hierarchy for the application.
Provides clear, typed exceptions with proper error context.
"""
from typing import Optional, Dict, Any
from enum import Enum
from fastapi import status

class ErrorCode(str, Enum):
    """Standardized error codes for the application"""
    # Domain errors
    VALIDATION_ERROR = "VALIDATION_ERROR"
    BUSINESS_RULE_VIOLATION = "BUSINESS_RULE_VIOLATION"
    
    # Infrastructure errors
    DATABASE_ERROR = "DATABASE_ERROR"
    EXTERNAL_API_ERROR = "EXTERNAL_API_ERROR"
    FILE_SYSTEM_ERROR = "FILE_SYSTEM_ERROR"
    
    # Application errors
    USE_CASE_ERROR = "USE_CASE_ERROR"
    BATCH_PROCESSING_ERROR = "BATCH_PROCESSING_ERROR"
    MEMORY_ERROR = "MEMORY_ERROR"
    
    # Generic errors
    INTERNAL_ERROR = "INTERNAL_ERROR"
    NOT_FOUND_ERROR = "NOT_FOUND_ERROR"
    TIMEOUT_ERROR = "TIMEOUT_ERROR"


class ApplicationException(Exception):
    """Base exception for all application-specific errors"""
    
    def __init__(
        self, 
        message: str, 
        error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
        context: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
        http_status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.context = context or {}
        self.cause = cause
        if error_code == ErrorCode.NOT_FOUND_ERROR:
            self.http_status_code = status.HTTP_404_NOT_FOUND
        else:
            self.http_status_code = http_status_code
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for API responses"""
        return {
            "error": True,
            "error_code": self.error_code.value,
            "message": self.message,
            "context": self.context
        }


class DomainException(ApplicationException):
    """Exceptions from the domain layer"""
    pass


class ValidationException(DomainException):
    """Data validation errors"""
    
    def __init__(self, message: str, field: Optional[str] = None, value: Any = None, status_code_override: Optional[int] = None):
        context = {}
        if field:
            context["field"] = field
        if value is not None:
            context["value"] = str(value)
        
        super().__init__(
            message=message,
            error_code=ErrorCode.VALIDATION_ERROR,
            context=context,
            http_status_code=status_code_override if status_code_override is not None else status.HTTP_400_BAD_REQUEST
        )
        if status_code_override: # Ensure error_code reflects not_found if 404 is passed
            if status_code_override == status.HTTP_404_NOT_FOUND:
                self.error_code = ErrorCode.NOT_FOUND_ERROR


class BusinessRuleViolationException(DomainException):
    """Business rule violations"""
    
    def __init__(self, message: str, rule: str, context: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            error_code=ErrorCode.BUSINESS_RULE_VIOLATION,
            context={**(context or {}), "rule": rule},
            http_status_code=status.HTTP_400_BAD_REQUEST # Or status.HTTP_409_CONFLICT
        )


class InfrastructureException(ApplicationException):
    """Exceptions from the infrastructure layer"""
    pass


class DatabaseException(InfrastructureException):
    """Database-related errors"""
    
    def __init__(self, message: str, query: Optional[str] = None, cause: Optional[Exception] = None):
        context = {}
        if query:
            context["query"] = query
        
        super().__init__(
            message=message,
            error_code=ErrorCode.DATABASE_ERROR,
            context=context,
            cause=cause,
            http_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


class ExternalApiException(InfrastructureException):
    """External API errors"""
    
    def __init__(
        self, 
        message: str, 
        endpoint: Optional[str] = None, 
        status_code: Optional[int] = None,
        cause: Optional[Exception] = None
    ):
        context = {}
        if endpoint:
            context["endpoint"] = endpoint
        if status_code:
            context["status_code"] = status_code
        
        super().__init__(
            message=message,
            error_code=ErrorCode.EXTERNAL_API_ERROR,
            context=context,
            cause=cause,
            # Defaulting to 500, but could be more specific if endpoint status_code is relayed
            http_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


class FileSystemException(InfrastructureException):
    """File system errors"""
    
    def __init__(self, message: str, file_path: Optional[str] = None, cause: Optional[Exception] = None):
        context = {}
        if file_path:
            context["file_path"] = file_path
        
        super().__init__(
            message=message,
            error_code=ErrorCode.FILE_SYSTEM_ERROR,
            context=context,
            cause=cause,
            http_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


class BatchProcessingException(ApplicationException):
    """Batch processing errors"""
    
    def __init__(
        self, 
        message: str, 
        batch_id: Optional[str] = None,
        processed_count: Optional[int] = None,
        failed_count: Optional[int] = None,
        cause: Optional[Exception] = None
    ):
        context = {}
        if batch_id:
            context["batch_id"] = batch_id
        if processed_count is not None:
            context["processed_count"] = processed_count
        if failed_count is not None:
            context["failed_count"] = failed_count
        
        super().__init__(
            message=message,
            error_code=ErrorCode.BATCH_PROCESSING_ERROR,
            context=context,
            cause=cause,
            http_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


class MemoryException(ApplicationException):
    """Memory-related errors"""
    
    def __init__(self, message: str, memory_usage_mb: Optional[float] = None):
        context = {}
        if memory_usage_mb:
            context["memory_usage_mb"] = memory_usage_mb
        
        super().__init__(
            message=message,
            error_code=ErrorCode.MEMORY_ERROR,
            context=context,
            http_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        ) 