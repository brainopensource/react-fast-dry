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
    """Application settings with schema-oriented configuration."""
    
    # Database settings
    DATABASE_PATH: Path = Path("data/well_production.db")
    DATABASE_BACKUP_PATH: Optional[Path] = None
    
    # File paths
    DOWNLOADS_DIR: Path = Path("downloads")
    DEFAULT_CSV_FILENAME: str = "well_production.csv"
    
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
    
    # External API settings
    EXTERNAL_API_TIMEOUT: int = 30
    EXTERNAL_API_RETRY_COUNT: int = 3
    EXTERNAL_API_RETRY_DELAY: int = 1
    
    # Batch processing settings
    BATCH_SIZE: int = 1000
    MAX_CONCURRENT_BATCHES: int = 4
    
    # Logging settings
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True

    def setup_directories(self):
        """Create necessary directories if they don't exist."""
        # Ensure DATA_ROOT_DIR is created, TEMP_DIR can remain if used by other parts or be removed if not
        for path in [self.DATABASE_PATH, self.DOWNLOADS_DIR]:
            path.mkdir(parents=True, exist_ok=True)

@lru_cache()
def get_settings() -> Settings:
    """Get application settings instance."""
    settings = Settings()
    settings.setup_directories()
    return settings
