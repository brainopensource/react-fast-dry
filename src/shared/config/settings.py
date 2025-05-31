from pathlib import Path
from functools import lru_cache
from pydantic_settings import BaseSettings
from typing import Optional
import sys
import os

class Settings(BaseSettings):
    """Application settings.
    
    Uses pydantic BaseSettings to load configuration from environment variables.
    """
    # Environment
    ENV: str = os.getenv("APP_ENV", "development")
    DEBUG: bool = os.getenv("APP_DEBUG", "True").lower() == "true"
    
    # Application paths
    APP_DIR: Path = Path(__file__).parent.parent.parent
    # DATA_DIR: Path = APP_DIR / "data" # Commented out as DATA_ROOT_DIR will be used
    TEMP_DIR: Path = APP_DIR / "temp"
    SQL_DIR: Path = APP_DIR / "src" / "sql"

    # External API configuration
    API_BASE_URL: Optional[str] = os.getenv("API_BASE_URL")
    API_KEY: Optional[str] = os.getenv("API_KEY")

    # Configurable data paths
    DATA_ROOT_DIR: Path = APP_DIR / os.getenv("DATA_ROOT_DIR_NAME", "data")
    DUCKDB_FILENAME: str = os.getenv("DUCKDB_FILENAME", "wells_production.duckdb")
    CSV_EXPORT_FILENAME: str = os.getenv("CSV_EXPORT_FILENAME", "wells_prod.csv")
    
    # Database
    DB_PATH: Path = DATA_ROOT_DIR / DUCKDB_FILENAME
    WELLS_SQL_PATH: Path = SQL_DIR / "wells.sql"
    
    # Export paths
    WELLS_EXPORT_PATH: Path = DATA_ROOT_DIR / CSV_EXPORT_FILENAME
    # Mocked response path
    MOCKED_RESPONSE_PATH: Path = APP_DIR / "external" / "mocked_response.json"

    # CORS settings
    CORS_ALLOWED_ORIGINS: list[str] = [origin.strip() for origin in os.getenv("CORS_ALLOWED_ORIGINS", "*").split(',')]
    
    class Config:
        env_prefix = "APP_" # Note: env_prefix might not be needed if using os.getenv explicitly for all
        use_enum_values = True

    def setup_directories(self):
        """Create necessary directories if they don't exist."""
        # Ensure DATA_ROOT_DIR is created, TEMP_DIR can remain if used by other parts or be removed if not
        for path in [self.DATA_ROOT_DIR, self.TEMP_DIR]:
            path.mkdir(parents=True, exist_ok=True)

@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    settings = Settings()
    settings.setup_directories()
    return settings
