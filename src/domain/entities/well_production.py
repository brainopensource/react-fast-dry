from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

class WellProduction(BaseModel):
    """Domain entity for well production data"""
    field_code: int = Field(description="Unique identifier for the field")
    field_name: str = Field(description="Name of the field")
    well_code: int = Field(description="Unique identifier for the well")
    well_reference: str = Field(description="Reference code for the well")
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

    def get_primary_key(self) -> tuple:
        """Get the primary key components"""
        return (self.well_code, self.field_code, self.production_period)

    def to_dict(self) -> dict:
        """Convert entity to dictionary"""
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: dict) -> 'WellProduction':
        """Create entity from dictionary"""
        return cls.model_validate(data)