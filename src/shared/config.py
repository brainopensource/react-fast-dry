from pydantic_settings import BaseSettings
from typing import Optional
from pathlib import Path

class Settings(BaseSettings):
    # API Configuration
    EXTERNAL_API_BASE_URL: str = "https://api.example.com"
    EXTERNAL_API_KEY: Optional[str] = None
    EXTERNAL_API_TIMEOUT_SECONDS: int = 30
    EXTERNAL_API_MAX_RETRIES: int = 3
    EXTERNAL_API_RETRY_DELAY_SECONDS: float = 1.0
    
    # Database Configuration
    DUCKDB_PATH: Path = Path("data/well_production.duckdb")
    
    # Mock Configuration
    MOCK_MODE: bool = False
    MOCKED_RESPONSE_PATH: Path = Path("data/mocked_responses.json")
    
    class Config:
        env_file = ".env"
        case_sensitive = True

def get_settings() -> Settings:
    return Settings() 