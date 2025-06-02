import asyncio
import time
import logging
from functools import wraps

# Configure a specific logger for timing, or use a general one
timing_logger = logging.getLogger("timing")
if not timing_logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    timing_logger.addHandler(handler)
    timing_logger.setLevel(logging.INFO)

def timed(func):
    """
    This decorator prints the execution time of the function it decorates.
    Logs to a logger named 'timing'.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        class_name = ""
        if hasattr(func, '__qualname__'):
            qualname_parts = func.__qualname__.split('.')
            if len(qualname_parts) > 1:
                class_name = qualname_parts[-2] + "."
        
        timing_logger.info(f"Sync function {class_name}{func.__name__} took {total_time:.4f} seconds to execute.")
        return result
    return wrapper

def async_timed(func):
    """
    This decorator prints the execution time of an async function it decorates.
    Logs to a logger named 'timing'.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = await func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        
        class_name = ""
        if hasattr(func, '__qualname__'):
            qualname_parts = func.__qualname__.split('.')
            if len(qualname_parts) > 1:
                class_name = qualname_parts[-2] + "."

        timing_logger.info(f"Async function {class_name}{func.__name__} took {total_time:.4f} seconds to execute.")
        return result
    return wrapper 