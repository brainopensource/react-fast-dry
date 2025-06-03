"""
Generic FastAPI application entry point.
Implements DRY, hexagonal architecture, DDD, SOLID, and Clean Architecture principles.
Now supports any dataset through configuration-driven architecture.
"""

import logging
import sys
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from datetime import datetime

# Import settings and dependency configuration
from src.shared.config.settings import get_settings
from src.shared.generic_dependencies import (
    configure_dependencies, get_service_factory_registry,
    startup_dependencies, shutdown_dependencies, check_dependencies_health
)

# Import generic router system
from src.interfaces.api.generic_dataset_router import create_main_datasets_router
from src.shared.job_manager import JobManager

# Import specific dataset configurations
from src.shared.config.schemas import get_available_datasets, get_dataset_config

# Ensure logs directory exists
settings = get_settings()
Path(settings.LOGS_DIR_NAME).mkdir(exist_ok=True)

# Configure logging with both console and file handlers
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"{settings.LOGS_DIR_NAME}/{settings.LOG_FILENAME}", mode="a")
    ]
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management with generic dependency injection."""
    logger.info("Starting Generic Data Management API...")
    
    # Ensure data directories exist
    Path(settings.DATA_ROOT_DIR).mkdir(parents=True, exist_ok=True)
    Path(settings.DOWNLOADS_DIR_NAME).mkdir(parents=True, exist_ok=True)
    Path(settings.TEMP_DIR_NAME).mkdir(parents=True, exist_ok=True)

    # Configure generic dependencies
    app_config = {
        "external_api": {
            "base_url": settings.ODATA_BASE_URL,
            "mock_mode": settings.USE_MOCK_DATA,
            "mock_file_path": str(settings.MOCKED_RESPONSE_PATH),
            "timeout_seconds": settings.EXTERNAL_API_TIMEOUT_SECONDS,
            "max_retries": settings.EXTERNAL_API_MAX_RETRIES,
            "retry_delay_seconds": settings.EXTERNAL_API_RETRY_DELAY_SECONDS
        },
        "repository_paths": {
            "data_dir": str(settings.DATA_ROOT_DIR),
            "downloads_dir": settings.DOWNLOADS_DIR_NAME,
            "duckdb_filename": settings.DUCKDB_FILENAME,
            "csv_filename": settings.CSV_EXPORT_FILENAME
        },
        "batch_processing": {
            "batch_size": settings.BATCH_SIZE,
            "max_memory_mb": settings.BATCH_MAX_MEMORY_MB,
            "gc_threshold_mb": settings.BATCH_GC_THRESHOLD_MB,
            "max_concurrent_batches": settings.BATCH_MAX_CONCURRENT_BATCHES,
            "retry_attempts": settings.BATCH_RETRY_ATTEMPTS,
            "retry_delay_seconds": settings.BATCH_RETRY_DELAY_SECONDS,
            "enable_memory_monitoring": settings.BATCH_ENABLE_MEMORY_MONITORING
        }
    }
    
    # Configure generic dependencies
    configure_dependencies(config=app_config)
    
    # Initialize dependencies
    await startup_dependencies()
    
    # Log available datasets
    available_datasets = get_available_datasets()
    logger.info(f"Available datasets: {available_datasets}")
    
    for dataset_name in available_datasets:
        try:
            config = get_dataset_config(dataset_name)
            logger.info(f"Dataset '{dataset_name}': {config.display_name} - {config.api_prefix}")
        except Exception as e:
            logger.error(f"Error loading configuration for dataset '{dataset_name}': {str(e)}")
    
    yield
    
    logger.info("Shutting down Generic Data Management API...")
    await shutdown_dependencies()

# Create FastAPI application with generic configuration
app = FastAPI(
    title="Generic Data Management API",
    description="""
    Generic, high-performance API for managing multiple datasets using DuckDB.
    
    ## Architecture
    - **DRY (Don't Repeat Yourself)**: Generic services and routers handle all datasets
    - **Hexagonal Architecture**: Clear separation between domain, application, infrastructure, and interface layers
    - **DDD (Domain-Driven Design)**: Rich domain models with business logic
    - **SOLID Principles**: Single responsibility, dependency injection, interface segregation
    - **Clean Architecture**: Dependency inversion and layered structure
    
    ## Features
    - **Multi-Dataset Support**: Add new datasets through configuration
    - **High Performance**: DuckDB backend optimized for millions of records
    - **Async Operations**: Full async/await support for scalability
    - **Comprehensive APIs**: CRUD, import, export, analytics for each dataset
    - **Type Safety**: Dynamic schema generation with Pydantic validation
    
    ## Available Datasets
    Check `/api/v1/datasets` for all available datasets and their endpoints.
    
    ## Configuration-Driven
    Add new datasets by simply updating the configuration in `src/shared/config/schemas.py`.
    """,
    version="2.0.0",
    lifespan=lifespan
)

# Configure CORS for production
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Create job manager for background operations
job_manager = JobManager()

# Get service factory registry for all datasets
service_factory_registry = get_service_factory_registry()

# Include generic dataset routers
datasets_router = create_main_datasets_router(service_factory_registry, job_manager)
app.include_router(datasets_router)

@app.get("/")
async def root():
    """API root endpoint with information about all available datasets."""
    try:
        available_datasets = get_available_datasets()
        datasets_info = {}
        
        for dataset_name in available_datasets:
            try:
                config = get_dataset_config(dataset_name)
                datasets_info[dataset_name] = {
                    "display_name": config.display_name,
                    "description": config.description,
                    "dataset_type": config.dataset_type.value,
                    "api_prefix": config.api_prefix,
                    "endpoints": {
                        "list": f"{config.api_prefix}/",
                        "create": f"{config.api_prefix}/",
                        "import": f"{config.api_prefix}/import",
                        "import_trigger": f"{config.api_prefix}/import/trigger",
                        "stats": f"{config.api_prefix}/stats" if config.enable_stats else None,
                        "export": f"{config.api_prefix}/export" if config.enable_download else None
                    }
                }
            except Exception as e:
                logger.error(f"Error getting info for dataset {dataset_name}: {str(e)}")
                datasets_info[dataset_name] = {"error": str(e)}
        
        return {
            "message": "Generic Data Management API",
            "version": "2.0.0",
            "architecture": {
                "pattern": "Hexagonal Architecture with DDD",
                "principles": ["DRY", "SOLID", "Clean Architecture"],
                "features": ["Generic Datasets", "Configuration-Driven", "High Performance"]
            },
            "datasets": datasets_info,
            "total_datasets": len(available_datasets),
            "global_endpoints": {
                "datasets_list": "/api/v1/datasets",
                "health": "/health",
                "docs": "/docs",
                "redoc": "/redoc"
            }
        }
        
    except Exception as e:
        logger.error(f"Error in root endpoint: {str(e)}")
        return {
            "message": "Generic Data Management API",
            "version": "2.0.0",
            "error": "Failed to load dataset information",
            "details": str(e)
        }

@app.get("/health")
async def health_check():
    """Comprehensive health check endpoint."""
    try:
        # Get dependency health
        dependency_health = await check_dependencies_health()
        
        # Overall service health
        overall_status = "healthy"
        if dependency_health["dependencies"] == "unhealthy":
            overall_status = "unhealthy"
        elif dependency_health["dependencies"] == "degraded":
            overall_status = "degraded"
        
        health_info = {
            "status": overall_status,
            "service": "generic-data-management-api",
            "version": "2.0.0",
            "timestamp": datetime.now().isoformat(),
            "architecture": "Hexagonal + DDD + SOLID + Clean",
            "database": "DuckDB",
            "dependencies": dependency_health,
            "available_datasets": get_available_datasets(),
            "total_datasets": len(get_available_datasets())
        }
        
        # Add performance metrics if available
        try:
            import psutil
            health_info["system"] = {
                "cpu_percent": psutil.cpu_percent(),
                "memory_percent": psutil.virtual_memory().percent,
                "disk_usage_percent": psutil.disk_usage('/').percent
            }
        except ImportError:
            health_info["system"] = "metrics not available"
        
        return health_info
        
    except Exception as e:
        logger.error(f"Error in health check: {str(e)}")
        return {
            "status": "unhealthy",
            "service": "generic-data-management-api",
            "version": "2.0.0",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }

# Mount static files after all routes are defined
app.mount("/static", StaticFiles(directory="src", html=True), name="static")
app.mount("/", StaticFiles(directory=".", html=True), name="root")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.SERVER_HOST, port=8080)
