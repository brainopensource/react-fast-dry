### main.py
import logging
import sys
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from datetime import datetime

# Import API routes from the interfaces layer
from .interfaces.api.well_production_routes import router as well_production_router

# Ensure logs directory exists
Path("logs").mkdir(exist_ok=True)

# Configure logging with both console and file handlers
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/wells_api.log", mode="a")
    ]
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management."""
    logger.info("Starting Well Production API...")
    yield
    logger.info("Shutting down Well Production API...")

app = FastAPI(
    title="Well Production API",
    description="High-performance API for managing millions of well production records using DuckDB and CSV storage",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS for production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(well_production_router)

@app.get("/")
async def root():
    """API root endpoint with information."""
    return {
        "message": "Well Production API",
        "version": "1.0.0",
        "features": {
            "storage": "Dual storage with DuckDB (primary) and CSV (export)",
            "performance": "Optimized for millions of records",
            "architecture": "Hexagonal Architecture with DDD principles"
        },
        "endpoints": {
            "import": "/api/v1/wells/import (POST with filters)",
            "import_trigger": "/api/v1/wells/import/trigger (GET - simple trigger)",
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
        "version": "1.0.0",
        "database": "duckdb",
        "timestamp": datetime.now().isoformat()
    }
