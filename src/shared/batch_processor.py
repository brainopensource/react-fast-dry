"""
Batch processing system for handling large-scale data operations.
Provides memory-efficient processing with error recovery and monitoring.
"""
import asyncio
import gc
import logging
import uuid
from typing import List, TypeVar, Callable, Optional, Dict, Any, AsyncGenerator
from dataclasses import dataclass
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None

from .exceptions import BatchProcessingException, MemoryException
from .responses import ErrorDetail, BatchResult

T = TypeVar('T')
R = TypeVar('R')

logger = logging.getLogger(__name__)


@dataclass
class BatchConfig:
    """Configuration for batch processing"""
    batch_size: int = 1000
    max_memory_mb: float = 512.0
    max_concurrent_batches: int = 3
    retry_attempts: int = 3
    retry_delay_seconds: float = 1.0
    enable_memory_monitoring: bool = True
    gc_threshold_mb: float = 256.0


@dataclass
class BatchMetrics:
    """Metrics for batch processing"""
    batch_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    total_items: int = 0
    processed_items: int = 0
    failed_items: int = 0
    memory_peak_mb: float = 0.0
    processing_time_ms: float = 0.0
    errors: List[ErrorDetail] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class MemoryMonitor:
    """Memory monitoring utility"""
    
    @staticmethod
    def get_memory_usage() -> float:
        """Get current memory usage in MB"""
        if not PSUTIL_AVAILABLE:
            # Return a default value when psutil is not available
            return 0.0
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    
    @staticmethod
    def check_memory_threshold(threshold_mb: float) -> bool:
        """Check if memory usage exceeds threshold"""
        if not PSUTIL_AVAILABLE:
            # Always return False when psutil is not available
            return False
        return MemoryMonitor.get_memory_usage() > threshold_mb
    
    @staticmethod
    def force_garbage_collection():
        """Force garbage collection"""
        gc.collect()


class BatchProcessor:
    """High-performance batch processor with memory management"""
    
    def __init__(self, config: Optional[BatchConfig] = None):
        self.config = config or BatchConfig()
        self.memory_monitor = MemoryMonitor()
        self._active_batches: Dict[str, BatchMetrics] = {}
    
    async def process_async(
        self,
        items: List[T],
        processor: Callable[[List[T]], R],
        batch_id: Optional[str] = None
    ) -> BatchResult:
        """
        Process items asynchronously in batches with memory management.
        
        Args:
            items: List of items to process
            processor: Function to process each batch
            batch_id: Optional batch identifier
            
        Returns:
            BatchResult with processing statistics
        """
        batch_id = batch_id or str(uuid.uuid4())
        metrics = BatchMetrics(
            batch_id=batch_id,
            start_time=datetime.utcnow(),
            total_items=len(items)
        )
        
        self._active_batches[batch_id] = metrics
        
        try:
            logger.info(f"Starting batch processing {batch_id} with {len(items)} items")
            
            # Split items into batches
            batches = self._create_batches(items)
            
            # Process batches with concurrency control
            results = await self._process_batches_async(batches, processor, metrics)
            
            metrics.end_time = datetime.utcnow()
            metrics.processing_time_ms = (
                metrics.end_time - metrics.start_time
            ).total_seconds() * 1000
            
            return self._create_batch_result(metrics)
            
        except Exception as e:
            logger.error(f"Batch processing failed for {batch_id}: {str(e)}")
            raise BatchProcessingException(
                message=f"Batch processing failed: {str(e)}",
                batch_id=batch_id,
                processed_count=metrics.processed_items,
                failed_count=metrics.failed_items,
                cause=e
            )
        finally:
            self._active_batches.pop(batch_id, None)
            self.memory_monitor.force_garbage_collection()
    
    async def process_stream(
        self,
        items: AsyncGenerator[T, None],
        processor: Callable[[List[T]], R],
        batch_id: Optional[str] = None
    ) -> AsyncGenerator[BatchResult, None]:
        """
        Process items as a stream for very large datasets.
        
        Args:
            items: Async generator of items
            processor: Function to process each batch
            batch_id: Optional batch identifier
            
        Yields:
            BatchResult for each processed batch
        """
        batch_id = batch_id or str(uuid.uuid4())
        current_batch = []
        batch_number = 0
        
        try:
            async for item in items:
                current_batch.append(item)
                
                # Check memory usage
                if self.config.enable_memory_monitoring:
                    if self.memory_monitor.check_memory_threshold(self.config.max_memory_mb):
                        logger.warning(f"Memory threshold exceeded, forcing GC")
                        self.memory_monitor.force_garbage_collection()
                
                # Process batch when full
                if len(current_batch) >= self.config.batch_size:
                    batch_number += 1
                    sub_batch_id = f"{batch_id}_stream_{batch_number}"
                    
                    result = await self.process_async(
                        current_batch, 
                        processor, 
                        sub_batch_id
                    )
                    yield result
                    
                    current_batch = []
            
            # Process remaining items
            if current_batch:
                batch_number += 1
                sub_batch_id = f"{batch_id}_stream_{batch_number}_final"
                
                result = await self.process_async(
                    current_batch, 
                    processor, 
                    sub_batch_id
                )
                yield result
                
        except Exception as e:
            logger.error(f"Stream processing failed for {batch_id}: {str(e)}")
            raise BatchProcessingException(
                message=f"Stream processing failed: {str(e)}",
                batch_id=batch_id,
                cause=e
            )
    
    def _create_batches(self, items: List[T]) -> List[List[T]]:
        """Split items into manageable batches"""
        batches = []
        for i in range(0, len(items), self.config.batch_size):
            batch = items[i:i + self.config.batch_size]
            batches.append(batch)
        return batches
    
    async def _process_batches_async(
        self,
        batches: List[List[T]],
        processor: Callable[[List[T]], R],
        metrics: BatchMetrics
    ) -> List[R]:
        """Process batches with controlled concurrency"""
        semaphore = asyncio.Semaphore(self.config.max_concurrent_batches)
        tasks = []
        
        for batch_index, batch in enumerate(batches):
            task = self._process_single_batch(
                batch, processor, metrics, batch_index, semaphore
            )
            tasks.append(task)
        
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _process_single_batch(
        self,
        batch: List[T],
        processor: Callable[[List[T]], R],
        metrics: BatchMetrics,
        batch_index: int,
        semaphore: asyncio.Semaphore
    ) -> R:
        """Process a single batch with error handling and retries"""
        async with semaphore:
            for attempt in range(self.config.retry_attempts):
                try:
                    # Memory check before processing
                    if self.config.enable_memory_monitoring:
                        current_memory = self.memory_monitor.get_memory_usage()
                        metrics.memory_peak_mb = max(metrics.memory_peak_mb, current_memory)
                        
                        if current_memory > self.config.max_memory_mb:
                            raise MemoryException(
                                f"Memory usage ({current_memory:.2f}MB) exceeds limit ({self.config.max_memory_mb}MB)",
                                memory_usage_mb=current_memory
                            )
                    
                    # Process the batch
                    result = await asyncio.get_event_loop().run_in_executor(
                        None, processor, batch
                    )
                    
                    metrics.processed_items += len(batch)
                    logger.debug(f"Processed batch {batch_index} with {len(batch)} items")
                    
                    # Garbage collection if needed
                    if (self.config.enable_memory_monitoring and 
                        self.memory_monitor.check_memory_threshold(self.config.gc_threshold_mb)):
                        self.memory_monitor.force_garbage_collection()
                    
                    return result
                    
                except Exception as e:
                    attempt_msg = f"Batch {batch_index} attempt {attempt + 1}/{self.config.retry_attempts}"
                    logger.warning(f"{attempt_msg} failed: {str(e)}")
                    
                    if attempt == self.config.retry_attempts - 1:
                        # Final attempt failed
                        metrics.failed_items += len(batch)
                        error_detail = ErrorDetail(
                            error_code="BATCH_PROCESSING_ERROR",
                            message=f"Batch {batch_index} failed after {self.config.retry_attempts} attempts: {str(e)}",
                            context={"batch_index": batch_index, "batch_size": len(batch)}
                        )
                        metrics.errors.append(error_detail)
                        raise e
                    else:
                        # Wait before retry
                        await asyncio.sleep(self.config.retry_delay_seconds * (attempt + 1))
    
    def _create_batch_result(self, metrics: BatchMetrics) -> BatchResult:
        """Create BatchResult from metrics"""
        return BatchResult(
            batch_id=metrics.batch_id,
            total_items=metrics.total_items,
            processed_items=metrics.processed_items,
            failed_items=metrics.failed_items,
            success_rate=(metrics.processed_items / metrics.total_items * 100) if metrics.total_items > 0 else 0,
            errors=metrics.errors,
            execution_time_ms=metrics.processing_time_ms,
            memory_usage_mb=metrics.memory_peak_mb
        )
    
    def get_active_batches(self) -> Dict[str, BatchMetrics]:
        """Get currently active batch metrics"""
        return self._active_batches.copy()
    
    def get_memory_status(self) -> Dict[str, float]:
        """Get current memory status"""
        current_memory = self.memory_monitor.get_memory_usage()
        if not PSUTIL_AVAILABLE:
            return {
                "current_mb": 0.0,
                "threshold_mb": self.config.max_memory_mb,
                "gc_threshold_mb": self.config.gc_threshold_mb,
                "usage_percentage": 0.0,
                "psutil_available": False
            }
        return {
            "current_mb": current_memory,
            "threshold_mb": self.config.max_memory_mb,
            "gc_threshold_mb": self.config.gc_threshold_mb,
            "usage_percentage": (current_memory / self.config.max_memory_mb) * 100,
            "psutil_available": True
        } 