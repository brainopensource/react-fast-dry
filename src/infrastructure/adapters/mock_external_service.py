"""
Mock external service adapter for testing and development.
Implements the IExternalDataService interface with configurable mock data.
"""

import logging
import json
import asyncio
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime, timedelta
import random

from ...domain.ports.services import IExternalDataService
from ...shared.config.schemas import get_dataset_config
from ...shared.exceptions import ApplicationException, ErrorCode

logger = logging.getLogger(__name__)

class MockExternalService(IExternalDataService):
    """
    Mock external service for testing and development.
    Generates realistic mock data based on dataset configuration.
    """
    
    def __init__(self, dataset_name: str, config: Dict[str, Any]):
        """Initialize mock external service."""
        self.dataset_name = dataset_name
        self.config = config
        self.dataset_config = get_dataset_config(dataset_name)
        self.logger = logging.getLogger(f"{__name__}.{dataset_name}")
        
        # Mock configuration
        self.mock_file_path = config.get('mock_file_path')
        self.mock_data_size = config.get('mock_data_size', 1000)
        self.mock_delay_seconds = config.get('mock_delay_seconds', 0.1)
        
        # Cache for generated mock data
        self._mock_data_cache: Optional[List[Dict[str, Any]]] = None
    
    async def fetch_data(self, entity_set: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Fetch mock data from external source."""
        try:
            self.logger.info(f"Fetching mock data for entity set: {entity_set}")
            
            # Simulate network delay
            if self.mock_delay_seconds > 0:
                await asyncio.sleep(self.mock_delay_seconds)
            
            # Try to load from mock file first
            if self.mock_file_path and Path(self.mock_file_path).exists():
                mock_data = await self._load_from_file()
            else:
                # Generate mock data
                mock_data = await self._generate_mock_data()
            
            # Apply filters if provided
            if filters:
                mock_data = self._apply_filters(mock_data, filters)
            
            self.logger.info(f"Returning {len(mock_data)} mock records")
            return mock_data
            
        except Exception as e:
            self.logger.error(f"Error fetching mock data: {str(e)}")
            raise ApplicationException(
                message=f"Failed to fetch mock data for {entity_set}",
                error_code=ErrorCode.EXTERNAL_SERVICE_ERROR,
                cause=e
            )
    
    async def get_data_count(self, entity_set: str, filters: Optional[Dict[str, Any]] = None) -> int:
        """Get count of mock data."""
        try:
            data = await self.fetch_data(entity_set, filters)
            return len(data)
        except Exception as e:
            self.logger.error(f"Error getting mock data count: {str(e)}")
            return 0
    
    async def test_connection(self) -> bool:
        """Test mock connection (always returns True)."""
        try:
            # Simulate a small delay
            await asyncio.sleep(0.01)
            return True
        except Exception as e:
            self.logger.error(f"Mock connection test failed: {str(e)}")
            return False
    
    async def _load_from_file(self) -> List[Dict[str, Any]]:
        """Load mock data from file."""
        try:
            with open(self.mock_file_path, 'r') as f:
                data = json.load(f)
            
            # Handle different file formats
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                # Look for common data keys
                for key in ['data', 'items', 'records', 'value']:
                    if key in data and isinstance(data[key], list):
                        return data[key]
                # If no list found, wrap the dict in a list
                return [data]
            else:
                self.logger.warning(f"Unexpected data format in mock file: {type(data)}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error loading mock data from file: {str(e)}")
            return []
    
    async def _generate_mock_data(self) -> List[Dict[str, Any]]:
        """Generate mock data based on dataset configuration."""
        if self._mock_data_cache is None:
            self._mock_data_cache = self._create_mock_data()
        
        return self._mock_data_cache.copy()
    
    def _create_mock_data(self) -> List[Dict[str, Any]]:
        """Create mock data based on dataset schema."""
        mock_data = []
        
        for i in range(self.mock_data_size):
            record = {}
            
            # Generate data for each OData field
            for external_field, internal_field in zip(
                self.dataset_config.odata_select_fields,
                [f.name for f in self.dataset_config.fields]
            ):
                # Find the corresponding field definition
                field_def = next(
                    (f for f in self.dataset_config.fields if f.name == internal_field),
                    None
                )
                
                if field_def:
                    record[external_field] = self._generate_field_value(field_def, i)
                else:
                    record[external_field] = f"mock_value_{i}"
            
            mock_data.append(record)
        
        return mock_data
    
    def _generate_field_value(self, field_def, index: int):
        """Generate a mock value for a specific field."""
        field_type = field_def.field_type
        field_name = field_def.name.lower()
        
        # Type-specific generation
        if field_type == int:
            if 'code' in field_name or 'id' in field_name:
                return 10000 + index
            elif 'days' in field_name:
                return random.randint(0, 365)
            else:
                return random.randint(1, 100000)
        
        elif field_type == float:
            if 'production' in field_name:
                return round(random.uniform(100.0, 5000.0), 2)
            elif 'cost' in field_name or 'value' in field_name:
                return round(random.uniform(1000.0, 100000.0), 2)
            else:
                return round(random.uniform(0.0, 1000.0), 2)
        
        elif field_type == str:
            if 'name' in field_name:
                if 'field' in field_name:
                    return f"Field {chr(65 + (index % 26))}"
                elif 'well' in field_name:
                    return f"Well {index + 1:04d}"
                elif 'equipment' in field_name:
                    return f"Equipment {index + 1:04d}"
                else:
                    return f"Name {index + 1}"
            
            elif 'reference' in field_name:
                return f"REF-{index + 1:06d}"
            
            elif 'period' in field_name:
                base_date = datetime(2024, 1, 1)
                months_offset = index % 12
                period_date = base_date + timedelta(days=months_offset * 30)
                return period_date.strftime("%Y-%m")
            
            elif 'source' in field_name:
                sources = ["Production System", "Manual Entry", "Sensor Data", "External API"]
                return sources[index % len(sources)]
            
            elif 'status' in field_name:
                statuses = ["Active", "Inactive", "Maintenance", "Decommissioned"]
                return statuses[index % len(statuses)]
            
            elif 'type' in field_name:
                if 'equipment' in field_name:
                    types = ["Pump", "Compressor", "Valve", "Tank", "Pipe"]
                else:
                    types = ["Type A", "Type B", "Type C"]
                return types[index % len(types)]
            
            elif 'location' in field_name:
                locations = ["North Platform", "South Platform", "East Platform", "West Platform", "Central"]
                return locations[index % len(locations)]
            
            elif 'supplier' in field_name:
                suppliers = ["Acme Corp", "Tech Solutions", "Industrial Supply", "Equipment Co", "Service Pro"]
                return suppliers[index % len(suppliers)]
            
            elif 'partition' in field_name:
                return str(2024 + (index % 5))
            
            else:
                return f"Mock {field_name} {index + 1}"
        
        elif field_type == datetime:
            base_date = datetime(2024, 1, 1)
            days_offset = index % 365
            return base_date + timedelta(days=days_offset)
        
        elif field_type == bool:
            return index % 2 == 0
        
        else:
            return f"unknown_type_{index}"
    
    def _apply_filters(self, data: List[Dict[str, Any]], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Apply filters to mock data."""
        filtered_data = data.copy()
        
        for field, value in filters.items():
            if isinstance(value, dict):
                # Handle complex filters
                for operator, filter_value in value.items():
                    filtered_data = [
                        record for record in filtered_data
                        if self._matches_filter(record.get(field), operator, filter_value)
                    ]
            else:
                # Simple equality filter
                filtered_data = [
                    record for record in filtered_data
                    if record.get(field) == value
                ]
        
        return filtered_data
    
    def _matches_filter(self, field_value: Any, operator: str, filter_value: Any) -> bool:
        """Check if field value matches filter."""
        if field_value is None:
            return False
        
        try:
            if operator == "eq":
                return field_value == filter_value
            elif operator == "ne":
                return field_value != filter_value
            elif operator == "gt":
                return field_value > filter_value
            elif operator == "gte":
                return field_value >= filter_value
            elif operator == "lt":
                return field_value < filter_value
            elif operator == "lte":
                return field_value <= filter_value
            elif operator == "like":
                return str(filter_value).lower() in str(field_value).lower()
            elif operator == "in":
                return field_value in filter_value
            else:
                return False
        except Exception:
            return False 