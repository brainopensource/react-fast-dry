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
    ENV: str = "development"
    DEBUG: bool = True
    
    # Application paths
    APP_DIR: Path = Path(__file__).parent.parent.parent
    DATA_DIR: Path = APP_DIR / "data"
    TEMP_DIR: Path = APP_DIR / "temp"
    SQL_DIR: Path = APP_DIR / "src" / "sql"
    
    # Database
    DB_PATH: Path = DATA_DIR / "db.duckdb"
    WELLS_SQL_PATH: Path = SQL_DIR / "wells.sql"
    
    # Export paths
    WELLS_EXPORT_PATH: Path = TEMP_DIR / "wells_prod.csv"
    
    class Config:
        env_prefix = "APP_"
        use_enum_values = True

    def setup_directories(self):
        """Create necessary directories if they don't exist."""
        for path in [self.DATA_DIR, self.TEMP_DIR]:
            path.mkdir(parents=True, exist_ok=True)

@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    settings = Settings()
    settings.setup_directories()
    return settings
