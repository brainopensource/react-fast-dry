### domain/models.py
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class WellProduction(BaseModel):
    well_code: str
    well_name: str
    field_code: str
    field_name: str
    prod_oil_kbd: float
    prod_gas_mcf: float
    prod_water_kbd: float
    production_period: datetime

class WellProductionExternal(BaseModel):
    """Model for external OData API response"""
    field_code: int
    field_name: str = Field(alias='_field_name')
    well_code: int
    well_reference: str = Field(alias='_well_reference')
    well_name: str
    production_period: str
    days_on_production: int
    oil_production_kbd: float
    gas_production_mmcfd: float
    liquids_production_kbd: float
    water_production_kbd: float
    data_source: str
    source_data: str
    partition_0: str

    class Config:
        populate_by_name = True

class ODataResponse(BaseModel):
    """Model for OData API response wrapper"""
    context: Optional[str] = Field(alias='@odata.context', default=None)
    value: list[WellProductionExternal]

    class Config:
        populate_by_name = True