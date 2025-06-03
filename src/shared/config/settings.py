from pathlib import Path
from functools import lru_cache
from pydantic_settings import BaseSettings
from typing import Optional, List
import sys
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Settings(BaseSettings):
    """Application settings with schema-oriented configuration."""
    
    # Database settings
    DUCKDB_FILENAME: str = "well_production.duckdb"
    DUCKDB_EXPORT_BATCH_SIZE: int = 1000
    DUCKDB_EXPORT_MEMORY_LIMIT: int = 4096
    DUCKDB_EXPORT_THREADS: int = 4
    
    # File paths
    DATA_ROOT_DIR: Path = Path("data")
    DOWNLOADS_DIR: Path = Path("downloads")
    DOWNLOADS_DIR_NAME: str = "downloads"
    TEMP_DIR_NAME: str = "temp"
    DEFAULT_CSV_FILENAME: str = "well_production.csv"
    CSV_EXPORT_FILENAME: str = "well_production_export.csv"
    LOGS_DIR_NAME: str = "logs"
    LOG_FILENAME: str = "app.log"
    MOCKED_RESPONSE_PATH: Path = Path("external/mocked_response.json")
    
    # Schema validation settings
    VALIDATION_MIN_DAYS_ON_PRODUCTION: int = 0
    VALIDATION_MAX_DAYS_ON_PRODUCTION: int = 31
    VALIDATION_PRODUCTION_PERIOD_FORMAT: str = "%Y-%m"
    VALIDATION_PARTITION_FORMAT: str = "%Y"
    
    # Business rules
    DATA_SOURCE_REQUIRED: bool = True
    SOURCE_DATA_REQUIRED: bool = True
    
    # API settings
    API_PORT: int = 8080
    API_HOST: str = "0.0.0.0"
    API_PREFIX: str = "/api/v1"
    API_TITLE: str = "Well Production API"
    API_VERSION: str = "1.0.0"
    API_DESCRIPTION: str = "API for managing well production data"
    
    # CORS settings
    CORS_ALLOWED_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:8080"]
    CORS_ALLOWED_METHODS: List[str] = ["*"]
    CORS_ALLOWED_HEADERS: List[str] = ["*"]
    CORS_ALLOW_CREDENTIALS: bool = True
    
    # External API settings
    EXTERNAL_API_TIMEOUT: int = 30
    EXTERNAL_API_RETRY_COUNT: int = 3
    EXTERNAL_API_RETRY_DELAY: int = 1
    EXTERNAL_API_TIMEOUT_SECONDS: int = 30
    EXTERNAL_API_MAX_RETRIES: int = 3
    EXTERNAL_API_RETRY_DELAY_SECONDS: float = 1.0
    USE_MOCK_DATA: bool = True
    
    # OData settings
    ODATA_USERNAME: str
    ODATA_PASSWORD: str
    ODATA_BASE_URL: str
    ODATA_TIMEOUT_SECONDS: int = 30
    ODATA_MAX_RETRIES: int = 3
    ODATA_RETRY_DELAY_SECONDS: float = 1.0
    ODATA_MAX_RECORDS_PER_REQUEST: int = 1000
    PYTHONPATH: str = "src"
    
    # Batch processing settings
    BATCH_SIZE: int = 1000
    MAX_CONCURRENT_BATCHES: int = 4
    BATCH_PROCESSING_MAX_MEMORY_MB: float = 4096.0
    BATCH_PROCESSING_RETRY_ATTEMPTS: int = 3
    BATCH_PROCESSING_RETRY_DELAY_SECONDS: float = 2.0
    BATCH_PROCESSING_ENABLE_MEMORY_MONITORING: bool = True
    BATCH_PROCESSING_GC_THRESHOLD_MB: float = 2048.0
    
    # Logging settings
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
        extra = "allow"  # Allow extra fields in the settings

    def setup_directories(self):
        """Create necessary directories if they don't exist."""
        # Ensure DATA_ROOT_DIR is created, TEMP_DIR can remain if used by other parts or be removed if not
        for path in [self.DATA_ROOT_DIR, self.DOWNLOADS_DIR]:
            path.mkdir(parents=True, exist_ok=True)

@lru_cache()
def get_settings() -> Settings:
    """Get application settings instance."""
    settings = Settings()
    settings.setup_directories()
    return settings
