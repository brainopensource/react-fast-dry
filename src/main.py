### main.py
# Standard library imports
import logging
import sys
from pathlib import Path
from datetime import datetime
from contextlib import asynccontextmanager

# Third-party imports
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Local application imports
from src.shared.config.settings import get_settings
from src.shared.dependencies import configure_dependencies
from src.interfaces.api.well_production_routes import router as well_production_router

# ============= Configuration Setup =============
def setup_logging():
    """Configure logging with both console and file handlers."""
    settings = get_settings()
    Path(settings.LOGS_DIR_NAME).mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f"{settings.LOGS_DIR_NAME}/{settings.LOG_FILENAME}", mode="a")
        ]
    )
    return logging.getLogger(__name__)

def setup_directories():
    """Create necessary directories for the application."""
    settings = get_settings()
    Path(settings.DATA_ROOT_DIR).mkdir(parents=True, exist_ok=True)
    Path(settings.DOWNLOADS_DIR_NAME).mkdir(parents=True, exist_ok=True)
    Path(settings.TEMP_DIR_NAME).mkdir(parents=True, exist_ok=True)
    Path("styles").mkdir(exist_ok=True)

def get_app_config():
    """Get application configuration dictionary."""
    settings = get_settings()
    return {
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
        }
    }

# ============= Application Setup =============
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management."""
    logger = logging.getLogger(__name__)
    logger.info("Starting Well Production API...")
    
    setup_directories()
    configure_dependencies(config=get_app_config())
    
    yield
    
    logger.info("Shutting down Well Production API...")

def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()
    
    app = FastAPI(
        title=settings.API_TITLE,
        description=settings.API_DESCRIPTION,
        version=settings.API_VERSION,
        lifespan=lifespan,
        swagger_ui_parameters={
            "syntaxHighlight.theme": "monokai",
            "customSiteTitle": settings.API_TITLE,
            "customfavIcon": "/styles/favicon.ico",
            "customCssUrl": "/styles/custom.css"
        }
    )
    
    # Configure CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )
    
    # Include API routes
    app.include_router(well_production_router)
    
    # Mount static files
    app.mount("/static", StaticFiles(directory="src", html=True), name="static")
    app.mount("/", StaticFiles(directory=".", html=True), name="root")
    
    return app

# ============= API Endpoints =============
def configure_endpoints(app: FastAPI):
    """Configure API endpoints."""
    settings = get_settings()
    
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

# ============= Application Entry Point =============
def main():
    """Main application entry point."""
    # Initialize logging
    logger = setup_logging()
    
    # Create and configure the application
    app = create_app()
    configure_endpoints(app)
    
    # Read and apply CSS to docs
    try:
        with open("styles/custom.css", "r") as f:
            app.swagger_ui_parameters["customCss"] = f.read()
    except FileNotFoundError:
        logger.warning("Custom CSS file not found. Using default styling.")
    
    return app

if __name__ == "__main__":
    import uvicorn
    app = main()
    uvicorn.run(app, host="0.0.0.0", port=8080)

# Expose app at module level
app = main()
