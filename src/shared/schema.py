from typing import Dict, Any, Optional, List
from datetime import datetime
import polars as pl
from pydantic import BaseModel, Field, ConfigDict
from .config.settings import get_settings

class WellProduction(BaseModel):
    """Base model for well production data with validation - Single source of truth"""
    field_code: int = Field(description="Unique identifier for the field")
    field_name: str = Field(description="Name of the field", alias="_field_name")
    well_code: int = Field(description="Unique identifier for the well")
    well_reference: str = Field(description="Reference code for the well", alias="_well_reference")
    well_name: str = Field(description="Name of the well")
    production_period: str = Field(description="Production period identifier")
    days_on_production: int = Field(description="Number of days the well was on production")
    oil_production_kbd: Optional[float] = Field(None, description="Oil production in thousand barrels per day")
    gas_production_mmcfd: Optional[float] = Field(None, description="Gas production in million cubic feet per day")
    liquids_production_kbd: Optional[float] = Field(None, description="Liquids production in thousand barrels per day")
    water_production_kbd: Optional[float] = Field(None, description="Water production in thousand barrels per day")
    data_source: Optional[str] = Field(None, description="Source of the production data")
    source_data: Optional[str] = Field(None, description="Raw source data in JSON format")
    partition_0: Optional[str] = Field(None, description="Partition key for data organization")
    created_at: Optional[datetime] = Field(None, description="Timestamp of record creation")
    updated_at: Optional[datetime] = Field(None, description="Timestamp of last record update")

    model_config = ConfigDict(
        populate_by_name=True,
        validate_by_name=True,
        json_schema_extra={
            "example": {
                "field_code": 12345,
                "field_name": "Example Field",
                "well_code": 67890,
                "well_reference": "WELL-001",
                "well_name": "Example Well",
                "production_period": "2024-01",
                "days_on_production": 30,
                "oil_production_kbd": 1.5,
                "gas_production_mmcfd": 2.0,
                "liquids_production_kbd": 1.8,
                "water_production_kbd": 0.5,
                "data_source": "API",
                "source_data": "{}",
                "partition_0": "2024",
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z"
            }
        }
    )

    def get_primary_key(self) -> tuple:
        """Get the primary key components"""
        return (self.well_code, self.field_code, self.production_period)

    def to_dict(self) -> dict:
        """Convert entity to dictionary"""
        return self.model_dump(by_alias=True)  # Use aliases for database fields

    @classmethod
    def from_dict(cls, data: dict) -> 'WellProduction':
        """Create entity from dictionary"""
        return cls.model_validate(data)

# API DTOs
class WellProductionResponse(WellProduction):
    """API response schema for well production data."""
    class Config:
        from_attributes = True  # Allows creation from domain entities

class WellProductionCreate(WellProduction):
    """Schema for creating new well production records."""
    pass

class WellProductionUpdate(BaseModel):
    """Schema for updating well production records."""
    days_on_production: Optional[int] = Field(None, ge=0, description="Number of days on production")
    oil_production_kbd: Optional[float] = Field(None, ge=0, description="Oil production in KBD")
    gas_production_mmcfd: Optional[float] = Field(None, ge=0, description="Gas production in MMCFD")
    liquids_production_kbd: Optional[float] = Field(None, ge=0, description="Liquids production in KBD")
    water_production_kbd: Optional[float] = Field(None, ge=0, description="Water production in KBD")
    data_source: Optional[str] = Field(None, description="Source of the data")
    source_data: Optional[str] = Field(None, description="Original source data reference")

class WellProductionListResponse(BaseModel):
    """Schema for paginated list of well production records."""
    items: List[WellProductionResponse]
    total: int = Field(..., description="Total number of records")
    page: int = Field(..., description="Current page number")
    size: int = Field(..., description="Page size")
    pages: int = Field(..., description="Total number of pages")

class WellProductionStats(BaseModel):
    """Schema for well production statistics."""
    total_wells: int = Field(..., description="Total number of wells")
    total_fields: int = Field(..., description="Total number of fields")
    total_oil_production: float = Field(..., description="Total oil production in KBD")
    total_gas_production: float = Field(..., description="Total gas production in MMCFD")
    total_liquids_production: float = Field(..., description="Total liquids production in KBD")
    total_water_production: float = Field(..., description="Total water production in KBD")
    active_wells: int = Field(..., description="Number of active wells")
    production_periods: List[str] = Field(..., description="Available production periods")

class WellProductionSchema:
    """Schema configuration for well production data - Single source of truth for the application"""
    
    @staticmethod
    def get_polars_schema() -> Dict[str, pl.DataType]:
        """Get Polars schema dictionary for data processing"""
        return {
            "field_code": pl.Int64,
            "_field_name": pl.Utf8,
            "well_code": pl.Int64,
            "_well_reference": pl.Utf8,
            "well_name": pl.Utf8,
            "production_period": pl.Utf8,
            "days_on_production": pl.Int64,
            "oil_production_kbd": pl.Float64,
            "gas_production_mmcfd": pl.Float64,
            "liquids_production_kbd": pl.Float64,
            "water_production_kbd": pl.Float64,
            "data_source": pl.Utf8,
            "source_data": pl.Utf8,
            "partition_0": pl.Utf8,
            "created_at": pl.Datetime,
            "updated_at": pl.Datetime
        }

    @staticmethod
    def get_column_names() -> List[str]:
        """Get ordered list of column names"""
        return list(WellProductionSchema.get_polars_schema().keys())

    @staticmethod
    def get_field_mapping() -> Dict[str, str]:
        """Get field mapping for external API responses"""
        return {
            "field_code": "field_code",
            "_field_name": "field_name",
            "well_code": "well_code",
            "_well_reference": "well_reference",
            "well_name": "well_name",
            "production_period": "production_period",
            "days_on_production": "days_on_production",
            "oil_production_kbd": "oil_production_kbd",
            "gas_production_mmcfd": "gas_production_mmcfd",
            "liquids_production_kbd": "liquids_production_kbd",
            "water_production_kbd": "water_production_kbd",
            "data_source": "data_source",
            "source_data": "source_data",
            "partition_0": "partition_0",
            "created_at": "created_at",
            "updated_at": "updated_at"
        }

    @staticmethod
    def get_primary_key_columns() -> List[str]:
        """Get list of primary key column names"""
        return ["well_code", "field_code", "production_period"]

    @staticmethod
    def get_required_columns() -> List[str]:
        """Get list of non-nullable column names"""
        return [
            "field_code", "_field_name", "well_code", "_well_reference",
            "well_name", "production_period", "days_on_production"
        ]

    @staticmethod
    def get_sql_create_table() -> str:
        """Get SQL for creating the well_production table"""
        columns = []
        for name, dtype in WellProductionSchema.get_polars_schema().items():
            sql_type = {
                pl.Int64: "INTEGER",
                pl.Utf8: "VARCHAR",
                pl.Float64: "DOUBLE",
                pl.Datetime: "TIMESTAMP"
            }.get(dtype, "VARCHAR")
            
            nullable = "NOT NULL" if name in WellProductionSchema.get_required_columns() else ""
            columns.append(f"{name} {sql_type} {nullable}")
        
        pk_columns = ", ".join(WellProductionSchema.get_primary_key_columns())
        return f"""
        CREATE TABLE IF NOT EXISTS well_production (
            {', '.join(columns)},
            PRIMARY KEY ({pk_columns})
        )
        """

    @staticmethod
    def get_sql_indexes() -> List[str]:
        """Get SQL for creating indexes"""
        return [
            f"CREATE INDEX IF NOT EXISTS idx_field_code ON well_production(field_code)",
            f"CREATE INDEX IF NOT EXISTS idx_well_code ON well_production(well_code)",
            f"CREATE INDEX IF NOT EXISTS idx_production_period ON well_production(production_period)",
            f"CREATE INDEX IF NOT EXISTS idx_composite_key ON well_production(well_code, field_code, production_period)"
        ]

    @staticmethod
    def get_validation_rules() -> Dict[str, Any]:
        """Get validation rules for the schema"""
        settings = get_settings()
        return {
            "days_on_production": {
                "min_value": settings.VALIDATION_MIN_DAYS_ON_PRODUCTION,
                "required": True
            },
            "oil_production_kbd": {
                "min_value": 0,
                "required": False
            },
            "gas_production_mmcfd": {
                "min_value": 0,
                "required": False
            },
            "liquids_production_kbd": {
                "min_value": 0,
                "required": False
            },
            "water_production_kbd": {
                "min_value": 0,
                "required": False
            }
        }

    @staticmethod
    def validate_data(data: Dict[str, Any]) -> WellProduction:
        """Validate data against the schema"""
        return WellProduction.model_validate(data)

    @staticmethod
    def to_dict(model: WellProduction) -> Dict[str, Any]:
        """Convert model to dictionary"""
        return model.model_dump(by_alias=True)  # Use aliases for database fields

    @staticmethod
    def get_csv_fieldnames() -> List[str]:
        """Get CSV fieldnames in the correct order"""
        return WellProductionSchema.get_column_names()

    @staticmethod
    def get_business_rules() -> Dict[str, Any]:
        """Get business rules for the schema"""
        settings = get_settings()
        return {
            "min_days_on_production": settings.VALIDATION_MIN_DAYS_ON_PRODUCTION,
            "production_period_format": settings.VALIDATION_PRODUCTION_PERIOD_FORMAT,
            "partition_format": settings.VALIDATION_PARTITION_FORMAT,
            "data_source_required": settings.DATA_SOURCE_REQUIRED,
            "source_data_required": settings.SOURCE_DATA_REQUIRED
        } 