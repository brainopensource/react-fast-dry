### main.py
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
from src.shared.dependencies import configure_dependencies

# Import API routes from the interfaces layer
from src.interfaces.api.well_production_routes import router as well_production_router

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
    """Application lifespan management."""
    logger.info("Starting Well Production API...")
    settings = get_settings()    # Ensure data directory exists
    Path(settings.DATA_ROOT_DIR).mkdir(parents=True, exist_ok=True)
    Path(settings.DOWNLOADS_DIR_NAME).mkdir(parents=True, exist_ok=True)
    Path(settings.TEMP_DIR_NAME).mkdir(parents=True, exist_ok=True)

    app_config = {
        "external_api": {
            "base_url": settings.ODATA_BASE_URL,
            "mock_mode": settings.USE_MOCK_DATA,  # Explicit control over mock mode
            "mock_file_path": str(settings.MOCKED_RESPONSE_PATH),
            "timeout_seconds": settings.EXTERNAL_API_TIMEOUT_SECONDS,
            "max_retries": settings.EXTERNAL_API_MAX_RETRIES,
            "retry_delay_seconds": settings.EXTERNAL_API_RETRY_DELAY_SECONDS
        },
        "repository_paths": { # Add a new section for repository paths
            "data_dir": str(settings.DATA_ROOT_DIR),
            "downloads_dir": settings.DOWNLOADS_DIR_NAME,
            "duckdb_filename": settings.DUCKDB_FILENAME,
            "csv_filename": settings.CSV_EXPORT_FILENAME
        },
        # Add other configurations like batch_processing if needed
    }
    configure_dependencies(config=app_config)
    yield
    logger.info("Shutting down Well Production API...")

app = FastAPI(
    title=settings.API_TITLE,
    description=settings.API_DESCRIPTION,
    version=settings.API_VERSION,
    lifespan=lifespan
)

# Fetch settings for CORS configuration
settings = get_settings()

# Configure CORS for production
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOWED_ORIGINS, # Use configured origins
    allow_credentials=True,
    allow_methods=["GET", "POST"], # Consider making this configurable too if needed
    allow_headers=["*"], # Consider making this configurable too if needed
)

# Include API routes
app.include_router(well_production_router)

@app.get("/")
async def root():
    """API root endpoint with information."""
    return {
        "message": settings.API_TITLE,
        "version": settings.API_VERSION,
        "features": {
            "storage": "DuckDB primary storage with on-demand CSV export",
            "performance": "Optimized for millions of records",
            "architecture": "Hexagonal Architecture with DDD principles"
        },
        "endpoints": {
            "import": "/api/v1/wells/import (POST with filters)",
            "import_trigger": "/api/v1/wells/import/trigger (GET - simple trigger)",
            "import_run": "/api/v1/wells/import/run (GET - OData API import with pagination)",
            "download": "/api/v1/wells/download",
            "stats": "/api/v1/wells/stats",
            "docs": "/docs"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy", 
        "service": "well-production-api",
        "version": settings.API_VERSION,
        "database": "duckdb",
        "timestamp": datetime.now().isoformat()
    }

# Mount static files after all routes are defined
app.mount("/static", StaticFiles(directory="src", html=True), name="static")
# Mount root directory for favicon and other root-level static files
app.mount("/", StaticFiles(directory=".", html=True), name="root")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.SERVER_HOST, port=settings.SERVER_PORT)
