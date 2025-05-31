"""
API schemas for well production endpoints.
These are DTOs (Data Transfer Objects) for the API layer, separate from domain entities.
"""
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List


class WellProductionSchema(BaseModel):
    """API schema for well production data - used for requests and responses."""
    field_code: int = Field(..., description="Unique field identifier")
    field_name: str = Field(..., description="Name of the field")
    well_code: int = Field(..., description="Unique well identifier")
    well_reference: str = Field(..., description="Well reference number")
    well_name: str = Field(..., description="Name of the well")
    production_period: str = Field(..., description="Production period identifier")
    days_on_production: int = Field(..., ge=0, description="Number of days on production")
    oil_production_kbd: float = Field(..., ge=0, description="Oil production in KBD")
    gas_production_mmcfd: float = Field(..., ge=0, description="Gas production in MMCFD")
    liquids_production_kbd: float = Field(..., ge=0, description="Liquids production in KBD")
    water_production_kbd: float = Field(..., ge=0, description="Water production in KBD")
    data_source: str = Field(..., description="Source of the data")
    source_data: str = Field(..., description="Original source data reference")
    partition_0: str = Field(..., description="Partition identifier")
    created_at: Optional[datetime] = Field(None, description="Record creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Record update timestamp")

    class Config:
        """Pydantic configuration."""
        from_attributes = True  # Allows creation from domain entities
        json_schema_extra = {
            "example": {
                "field_code": 12345,
                "field_name": "North Sea Field",
                "well_code": 67890,
                "well_reference": "NS-001",
                "well_name": "North Sea Well 1",
                "production_period": "2024-01",
                "days_on_production": 30,
                "oil_production_kbd": 1500.5,
                "gas_production_mmcfd": 25.3,
                "liquids_production_kbd": 200.1,
                "water_production_kbd": 100.0,
                "data_source": "Production System",
                "source_data": "PS-2024-001",
                "partition_0": "2024"
            }
        }


class WellProductionCreateSchema(BaseModel):
    """Schema for creating new well production records."""
    field_code: int = Field(..., description="Unique field identifier")
    field_name: str = Field(..., description="Name of the field")
    well_code: int = Field(..., description="Unique well identifier")
    well_reference: str = Field(..., description="Well reference number")
    well_name: str = Field(..., description="Name of the well")
    production_period: str = Field(..., description="Production period identifier")
    days_on_production: int = Field(..., ge=0, description="Number of days on production")
    oil_production_kbd: float = Field(..., ge=0, description="Oil production in KBD")
    gas_production_mmcfd: float = Field(..., ge=0, description="Gas production in MMCFD")
    liquids_production_kbd: float = Field(..., ge=0, description="Liquids production in KBD")
    water_production_kbd: float = Field(..., ge=0, description="Water production in KBD")
    data_source: str = Field(..., description="Source of the data")
    source_data: str = Field(..., description="Original source data reference")
    partition_0: str = Field(..., description="Partition identifier")


class WellProductionUpdateSchema(BaseModel):
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
    items: List[WellProductionSchema]
    total: int = Field(..., description="Total number of records")
    page: int = Field(..., description="Current page number")
    size: int = Field(..., description="Page size")
    pages: int = Field(..., description="Total number of pages")


class WellProductionStatsSchema(BaseModel):
    """Schema for well production statistics."""
    total_wells: int = Field(..., description="Total number of wells")
    total_fields: int = Field(..., description="Total number of fields")
    total_oil_production: float = Field(..., description="Total oil production in KBD")
    total_gas_production: float = Field(..., description="Total gas production in MMCFD")
    total_liquids_production: float = Field(..., description="Total liquids production in KBD")
    total_water_production: float = Field(..., description="Total water production in KBD")
    active_wells: int = Field(..., description="Number of active wells")
    production_periods: List[str] = Field(..., description="Available production periods") 