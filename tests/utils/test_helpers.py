"""
Test helper utilities.
"""

import json
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional


class TestDataHelper:
    """Helper class for managing test data."""
    
    @staticmethod
    def create_temp_json_file(data: Dict[str, Any]) -> Path:
        """Create a temporary JSON file with test data."""
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(data, temp_file, indent=2)
        temp_file.close()
        return Path(temp_file.name)
    
    @staticmethod
    def create_sample_well_data() -> Dict[str, Any]:
        """Create sample well data for testing."""
        return {
            "well_code": 59806,
            "well_name": "Test Well 1",
            "well_reference": "1-C-1-BA",
            "field_code": 8908,
            "field_name": "Candeias",
            "production_data": {
                "oil_production_kbd": 123.456789,
                "gas_production_mmcfd": 78.912345,
                "total_production_kbd": 202.369134,
                "is_producing": True
            }
        }
    
    @staticmethod
    def create_sample_field_data() -> Dict[str, Any]:
        """Create sample field data for testing."""
        return {
            "field_code": 8908,
            "field_name": "Candeias",
            "wells_count": 5,
            "wells": [
                {
                    "well_code": 59806,
                    "well_name": "Test Well 1",
                    "well_reference": "1-C-1-BA"
                }
            ]
        }
    
    @staticmethod
    def create_sample_stats_data() -> Dict[str, Any]:
        """Create sample statistics data for testing."""
        return {
            "total_records": 1000,
            "storage_info": {
                "primary": "DuckDB",
                "secondary": "CSV",
                "performance": "Optimized"
            }
        }


class MockDataGenerator:
    """Generate mock data for testing external API responses."""
    
    @staticmethod
    def generate_import_response(imported_count: int = 500, total_records: int = 1000) -> Dict[str, Any]:
        """Generate a mock import response."""
        return {
            "message": "Well production data imported successfully",
            "data": {
                "imported_count": imported_count,
                "total_records": total_records,
                "source": "mock_data.json",
                "storage_info": {
                    "primary": "DuckDB database updated",
                    "secondary": "CSV file generated"
                }
            }
        }
    
    @staticmethod
    def generate_odata_response(record_count: int = 500) -> Dict[str, Any]:
        """Generate a mock OData API response."""
        return {
            "@odata.context": "http://example.com/api/$metadata#WellProduction",
            "@odata.count": record_count,
            "value": [
                {
                    "well_code": 59806,
                    "well_name": "Test Well 1",
                    "well_reference": "1-C-1-BA",
                    "field_code": 8908,
                    "field_name": "Candeias",
                    "oil_production_kbd": 123.456789,
                    "gas_production_mmcfd": 78.912345,
                    "total_production_kbd": 202.369134,
                    "is_producing": True
                }
            ]
        }


class ResponseValidator:
    """Validate API response structures."""
    
    @staticmethod
    def validate_import_response(response_data: Dict[str, Any]) -> bool:
        """Validate import response structure."""
        required_keys = ["message", "data"]
        if not all(key in response_data for key in required_keys):
            return False
        
        data_section = response_data.get("data", {})
        data_keys = ["imported_count", "total_records"]
        return all(key in data_section for key in data_keys)
    
    @staticmethod
    def validate_stats_response(response_data: Dict[str, Any]) -> bool:
        """Validate stats response structure."""
        required_keys = ["total_records", "storage_info"]
        if not all(key in response_data for key in required_keys):
            return False
        
        storage_info = response_data.get("storage_info", {})
        storage_keys = ["primary", "secondary"]
        return all(key in storage_info for key in storage_keys)
    
    @staticmethod
    def validate_well_response(response_data: Dict[str, Any]) -> bool:
        """Validate well response structure."""
        required_keys = ["well_name", "field_name"]
        return all(key in response_data for key in required_keys)
    
    @staticmethod
    def validate_field_response(response_data: Dict[str, Any]) -> bool:
        """Validate field response structure."""
        required_keys = ["field_name", "wells_count"]
        return all(key in response_data for key in required_keys)


def cleanup_temp_files(*file_paths: Path) -> None:
    """Clean up temporary files created during testing."""
    for file_path in file_paths:
        try:
            if file_path.exists():
                file_path.unlink()
        except Exception:
            pass  # Ignore cleanup errors 