from pathlib import Path
from functools import lru_cache
from pydantic_settings import BaseSettings
from typing import Optional
import sys
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Settings(BaseSettings):
    """Application settings.
    
    Only sensitive OData credentials use environment variables.
    Everything else is hardcoded in settings.
    """
    # Environment
    ENV: str = "development"  # Options: "development", "testing", "production"
    DEBUG: bool = True
    
    # Mock Mode Configuration (independent of ENV for testing flexibility)
    USE_MOCK_DATA: bool = True  # Set to False to test real API calls
    
    # Application paths
    APP_DIR: Path = Path(__file__).parent.parent.parent.parent
    TEMP_DIR: Path = APP_DIR / "temp"
    SQL_DIR: Path = APP_DIR / "src" / "sql"

    # External API configuration
    ODATA_BASE_URL: Optional[str] = os.getenv("ODATA_BASE_URL", "https://example.com/odata")
    ODATA_USERNAME: Optional[str] = os.getenv("ODATA_USERNAME", "dev_user")
    ODATA_PASSWORD: Optional[str] = os.getenv("ODATA_PASSWORD", "dev_password")

    ODATA_TIMEOUT_SECONDS: int = 60
    ODATA_MAX_RETRIES: int = 3
    ODATA_RETRY_DELAY_SECONDS: float = 2.0
    ODATA_MAX_RECORDS_PER_REQUEST: int = 998

    # Batch Processing Configuration
    BATCH_SIZE: int = 1000
    BATCH_MAX_MEMORY_MB: float = 6000.0
    BATCH_GC_THRESHOLD_MB: float = 4000.0
    BATCH_MAX_CONCURRENT_BATCHES: int = 3
    BATCH_RETRY_ATTEMPTS: int = 3
    BATCH_RETRY_DELAY_SECONDS: float = 1.0
    BATCH_ENABLE_MEMORY_MONITORING: bool = True

    # Validation Configuration
    VALIDATION_MIN_DAYS_ON_PRODUCTION: int = 0
    VALIDATION_MAX_DAYS_ON_PRODUCTION: int = 365
    VALIDATION_MIN_PRODUCTION_VALUE: float = 0.0
    VALIDATION_MAX_PRODUCTION_VALUE: float = 999999.0

    # Configurable data paths (generic)
    DATA_ROOT_DIR: Path = APP_DIR / "data"
    DUCKDB_FILENAME: str = "generic_data.duckdb"
    CSV_EXPORT_FILENAME: str = "data_export.csv"
    
    # Database
    DB_PATH: Path = DATA_ROOT_DIR / DUCKDB_FILENAME
    
    # Mocked response path
    MOCKED_RESPONSE_PATH: Path = APP_DIR / "external" / "mocked_response.json"

    # CORS settings
    CORS_ALLOWED_ORIGINS: list[str] = ["*"]  # Allow all origins for development, configure for production
    
    # Server Configuration
    SERVER_HOST: str = "0.0.0.0"
    SERVER_PORT: int = 8080
    
    # External API Configuration (non-OData)
    EXTERNAL_API_TIMEOUT_SECONDS: int = 30
    EXTERNAL_API_MAX_RETRIES: int = 3
    EXTERNAL_API_RETRY_DELAY_SECONDS: float = 4.0
    
    # Directory Names (for hardcoded directory creation)
    LOGS_DIR_NAME: str = "logs"
    DOWNLOADS_DIR_NAME: str = "downloads"
    TEMP_DIR_NAME: str = "temp"
    
    # Log File Configuration
    LOG_FILENAME: str = "generic_api.log"
    LOG_LEVEL: str = "INFO"
    
    # DuckDB Export Configuration
    DUCKDB_EXPORT_BATCH_SIZE: int = 100_000
    DUCKDB_EXPORT_MEMORY_LIMIT: str = "6GB"
    DUCKDB_EXPORT_THREADS: int = 4
    
    # Default Filenames
    DEFAULT_CSV_FILENAME: str = "data_export.csv"
    
    # Application Information
    API_TITLE: str = "Generic Data Management API"
    API_DESCRIPTION: str = "High-performance API for managing multiple datasets using DuckDB with configuration-driven architecture"
    API_VERSION: str = "2.0.0"
    
    class Config:
        env_prefix = "APP_"
        use_enum_values = True

    def setup_directories(self):
        """Create necessary directories if they don't exist."""
        for path in [self.DATA_ROOT_DIR, self.TEMP_DIR]:
            path.mkdir(parents=True, exist_ok=True)

@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    settings = Settings()
    settings.setup_directories()
    return settings
