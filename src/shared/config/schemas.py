"""
Generic schema configuration system for all datasets.
This file defines the schema structure and behaviors for different datasets in a DRY way.
Follows DDD, SOLID, Clean Architecture, and Hexagonal principles.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Type, Optional, List, Callable
from dataclasses import dataclass, field
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum

# Base interfaces following Interface Segregation Principle
class EntityValidationMixin(ABC):
    """Mixin for entity validation rules."""
    
    @abstractmethod
    def validate(self) -> List[str]:
        """Validate entity and return list of validation errors."""
        pass

class EntityCalculationMixin(ABC):
    """Mixin for entity calculations."""
    
    @abstractmethod
    def calculate_totals(self) -> Dict[str, float]:
        """Calculate aggregate values for the entity."""
        pass

class EntityStatusMixin(ABC):
    """Mixin for entity status checks."""
    
    @abstractmethod
    def is_active(self) -> bool:
        """Check if entity is in active state."""
        pass

# Enums for better type safety
class DatasetType(str, Enum):
    """Dataset type enumeration."""
    PRODUCTION = "production"
    INVENTORY = "inventory"
    FINANCIAL = "financial"
    OPERATIONAL = "operational"

class ImportStrategy(str, Enum):
    """Import strategy enumeration."""
    BATCH = "batch"
    STREAMING = "streaming"
    INCREMENTAL = "incremental"
    FULL_REFRESH = "full_refresh"

# Generic field definitions
@dataclass
class FieldDefinition:
    """Definition of a field in a dataset schema."""
    name: str
    field_type: Type
    description: str
    required: bool = True
    validation_rules: Optional[Dict[str, Any]] = None
    default_value: Any = None
    
    def to_pydantic_field(self) -> Any:
        """Convert to Pydantic field definition."""
        kwargs = {
            "description": self.description,
            "default": ... if self.required else self.default_value
        }
        
        if self.validation_rules:
            kwargs.update(self.validation_rules)
            
        return Field(**kwargs)

# Schema configuration for each dataset
@dataclass
class DatasetSchemaConfig:
    """Configuration for a dataset schema."""
    name: str
    display_name: str
    description: str
    dataset_type: DatasetType
    
    # Schema definitions
    fields: List[FieldDefinition]
    
    # External API configuration
    odata_entity_set: str
    odata_select_fields: List[str]
    
    # Database configuration
    table_name: str
    primary_keys: List[str]
    
    # Fields with default values (must come after fields without defaults)
    odata_filter_template: Optional[str] = None
    indexes: List[List[str]] = field(default_factory=list)
    partitions: List[str] = field(default_factory=list)
    
    # Import/Export configuration
    import_strategy: ImportStrategy = ImportStrategy.BATCH
    batch_size: int = 1000
    export_filename: str = ""
    
    # Business logic configuration
    validation_rules: Dict[str, Any] = field(default_factory=dict)
    calculation_rules: Dict[str, str] = field(default_factory=dict)
    status_rules: Dict[str, str] = field(default_factory=dict)
    
    # API configuration
    api_prefix: str = ""
    api_tags: List[str] = field(default_factory=list)
    enable_stats: bool = True
    enable_download: bool = True
    enable_individual_lookup: bool = True
    
    def __post_init__(self):
        """Post-initialization setup."""
        if not self.export_filename:
            self.export_filename = f"{self.name}.csv"
        if not self.api_prefix:
            self.api_prefix = f"/api/v1/{self.name}"
        if not self.api_tags:
            self.api_tags = [self.name]

# Generic base entity class
class BaseEntity:
    """Base class for all domain entities."""
    
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert entity to dictionary."""
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}

# Dynamic schema builders
class SchemaBuilder:
    """Builder for creating dynamic schemas."""
    
    @staticmethod
    def build_entity_class(config: DatasetSchemaConfig) -> Type[BaseEntity]:
        """Build a dynamic entity class from configuration."""
        
        class DynamicEntity(BaseEntity, EntityValidationMixin, EntityCalculationMixin, EntityStatusMixin):
            """Dynamically created entity class."""
            
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.created_at: Optional[datetime] = kwargs.get('created_at')
                self.updated_at: Optional[datetime] = kwargs.get('updated_at')
            
            def validate(self) -> List[str]:
                """Validate entity based on configuration rules."""
                errors = []
                
                for field_def in config.fields:
                    value = getattr(self, field_def.name, None)
                    
                    if field_def.required and value is None:
                        errors.append(f"{field_def.name} is required")
                    
                    if field_def.validation_rules and value is not None:
                        rules = field_def.validation_rules
                        
                        if 'ge' in rules and value < rules['ge']:
                            errors.append(f"{field_def.name} must be >= {rules['ge']}")
                        if 'le' in rules and value > rules['le']:
                            errors.append(f"{field_def.name} must be <= {rules['le']}")
                        if 'min_length' in rules and len(str(value)) < rules['min_length']:
                            errors.append(f"{field_def.name} must be at least {rules['min_length']} characters")
                
                return errors
            
            def calculate_totals(self) -> Dict[str, float]:
                """Calculate totals based on configuration rules."""
                totals = {}
                
                for calc_name, calc_rule in config.calculation_rules.items():
                    try:
                        # Simple expression evaluation (extend as needed)
                        if '+' in calc_rule:
                            fields = [f.strip() for f in calc_rule.split('+')]
                            total = sum(getattr(self, field, 0) for field in fields)
                            totals[calc_name] = total
                    except Exception as e:
                        totals[calc_name] = 0.0
                
                return totals
            
            def is_active(self) -> bool:
                """Check if entity is active based on status rules."""
                for status_field, status_condition in config.status_rules.items():
                    value = getattr(self, status_field, None)
                    if value is None:
                        return False
                    
                    if status_condition.startswith('>'):
                        threshold = float(status_condition[1:])
                        return value > threshold
                    elif status_condition.startswith('=='):
                        expected = status_condition[2:].strip()
                        return str(value) == expected
                
                return True
        
        DynamicEntity.__name__ = f"{config.name.title()}Entity"
        DynamicEntity.__qualname__ = f"{config.name.title()}Entity"
        
        return DynamicEntity
    
    @staticmethod
    def build_pydantic_schema(config: DatasetSchemaConfig) -> Type[BaseModel]:
        """Build a dynamic Pydantic schema from configuration."""
        
        # Create field definitions
        field_definitions = {}
        for field_def in config.fields:
            field_definitions[field_def.name] = (
                field_def.field_type,
                field_def.to_pydantic_field()
            )
        
        # Add common fields
        field_definitions['created_at'] = (Optional[datetime], Field(None, description="Record creation timestamp"))
        field_definitions['updated_at'] = (Optional[datetime], Field(None, description="Record update timestamp"))
        
        # Create the dynamic class
        schema_class = type(
            f"{config.name.title()}Schema",
            (BaseModel,),
            {
                '__annotations__': {k: v[0] for k, v in field_definitions.items()},
                **{k: v[1] for k, v in field_definitions.items()},
                'Config': type('Config', (), {
                    'from_attributes': True,
                    'json_schema_extra': {
                        "example": SchemaBuilder._generate_example_data(config)
                    }
                })
            }
        )
        
        return schema_class
    
    @staticmethod
    def _generate_example_data(config: DatasetSchemaConfig) -> Dict[str, Any]:
        """Generate example data for schema."""
        example = {}
        
        for field_def in config.fields:
            if field_def.field_type == int:
                example[field_def.name] = 12345
            elif field_def.field_type == float:
                example[field_def.name] = 123.45
            elif field_def.field_type == str:
                example[field_def.name] = f"Example {field_def.name}"
            elif field_def.field_type == datetime:
                example[field_def.name] = "2024-01-01T00:00:00"
            else:
                example[field_def.name] = None
        
        return example

# Dataset configurations
DATASETS_CONFIG: Dict[str, DatasetSchemaConfig] = {
    "wells_production": DatasetSchemaConfig(
        name="wells_production",
        display_name="Well Production Data",
        description="Oil and gas well production data with comprehensive metrics",
        dataset_type=DatasetType.PRODUCTION,
        
        fields=[
            FieldDefinition("field_code", int, "Unique field identifier"),
            FieldDefinition("field_name", str, "Name of the field"),
            FieldDefinition("well_code", int, "Unique well identifier"),
            FieldDefinition("well_reference", str, "Well reference number"),
            FieldDefinition("well_name", str, "Name of the well"),
            FieldDefinition("production_period", str, "Production period identifier"),
            FieldDefinition("days_on_production", int, "Number of days on production", validation_rules={"ge": 0}),
            FieldDefinition("oil_production_kbd", float, "Oil production in KBD", validation_rules={"ge": 0}),
            FieldDefinition("gas_production_mmcfd", float, "Gas production in MMCFD", validation_rules={"ge": 0}),
            FieldDefinition("liquids_production_kbd", float, "Liquids production in KBD", validation_rules={"ge": 0}),
            FieldDefinition("water_production_kbd", float, "Water production in KBD", validation_rules={"ge": 0}),
            FieldDefinition("data_source", str, "Source of the data"),
            FieldDefinition("source_data", str, "Original source data reference"),
            FieldDefinition("partition_0", str, "Partition identifier"),
        ],
        
        odata_entity_set="WellProductionData",
        odata_select_fields=[
            "FieldCode", "FieldName", "WellCode", "WellReference", "WellName",
            "ProductionPeriod", "DaysOnProduction", "OilProductionKBD",
            "GasProductionMMCFD", "LiquidsProductionKBD", "WaterProductionKBD",
            "DataSource", "SourceData", "Partition0"
        ],
        
        table_name="wells_production",
        primary_keys=["well_code", "production_period"],
        indexes=[["field_code"], ["well_code"], ["production_period"]],
        partitions=["partition_0"],
        
        calculation_rules={
            "total_production_kbd": "oil_production_kbd + liquids_production_kbd + water_production_kbd"
        },
        status_rules={
            "days_on_production": ">0"
        },
        
        api_prefix="/api/v1/wells",
        api_tags=["wells"]
    ),
    
    # Example of how to add another dataset - Equipment Inventory
    "equipment_inventory": DatasetSchemaConfig(
        name="equipment_inventory",
        display_name="Equipment Inventory",
        description="Equipment inventory and maintenance tracking",
        dataset_type=DatasetType.INVENTORY,
        
        fields=[
            FieldDefinition("equipment_id", str, "Unique equipment identifier"),
            FieldDefinition("equipment_name", str, "Name of the equipment"),
            FieldDefinition("equipment_type", str, "Type/category of equipment"),
            FieldDefinition("location", str, "Equipment location"),
            FieldDefinition("status", str, "Current status"),
            FieldDefinition("purchase_date", datetime, "Purchase date"),
            FieldDefinition("purchase_cost", float, "Purchase cost", validation_rules={"ge": 0}),
            FieldDefinition("current_value", float, "Current estimated value", validation_rules={"ge": 0}),
            FieldDefinition("maintenance_due_date", datetime, "Next maintenance due date", required=False),
            FieldDefinition("supplier", str, "Equipment supplier"),
        ],
        
        odata_entity_set="EquipmentInventory",
        odata_select_fields=[
            "EquipmentID", "EquipmentName", "EquipmentType", "Location",
            "Status", "PurchaseDate", "PurchaseCost", "CurrentValue",
            "MaintenanceDueDate", "Supplier"
        ],
        
        table_name="equipment_inventory",
        primary_keys=["equipment_id"],
        indexes=[["equipment_type"], ["location"], ["status"]],
        
        status_rules={
            "status": "==Active"
        },
        
        api_prefix="/api/v1/equipment",
        api_tags=["equipment", "inventory"]
    )
}

def get_dataset_config(dataset_name: str) -> DatasetSchemaConfig:
    """Get dataset configuration by name."""
    if dataset_name not in DATASETS_CONFIG:
        raise ValueError(f"Dataset '{dataset_name}' not found in configuration")
    return DATASETS_CONFIG[dataset_name]

def get_available_datasets() -> List[str]:
    """Get list of available dataset names."""
    return list(DATASETS_CONFIG.keys())

# Factory for creating dynamic classes
class DatasetFactory:
    """Factory for creating dataset-specific classes."""
    
    _entity_cache: Dict[str, Type[BaseEntity]] = {}
    _schema_cache: Dict[str, Type[BaseModel]] = {}
    
    @classmethod
    def get_entity_class(cls, dataset_name: str) -> Type[BaseEntity]:
        """Get entity class for dataset."""
        if dataset_name not in cls._entity_cache:
            config = get_dataset_config(dataset_name)
            cls._entity_cache[dataset_name] = SchemaBuilder.build_entity_class(config)
        return cls._entity_cache[dataset_name]
    
    @classmethod
    def get_schema_class(cls, dataset_name: str) -> Type[BaseModel]:
        """Get Pydantic schema class for dataset."""
        if dataset_name not in cls._schema_cache:
            config = get_dataset_config(dataset_name)
            cls._schema_cache[dataset_name] = SchemaBuilder.build_pydantic_schema(config)
        return cls._schema_cache[dataset_name]
    
    @classmethod
    def create_entity(cls, dataset_name: str, **kwargs) -> BaseEntity:
        """Create entity instance for dataset."""
        entity_class = cls.get_entity_class(dataset_name)
        return entity_class(**kwargs) 