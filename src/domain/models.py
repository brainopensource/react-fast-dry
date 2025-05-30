### domain/models.py
from pydantic import BaseModel
from datetime import date

class WellProduction(BaseModel):
    well_code: str
    well_name: str
    field_code: str
    field_name: str
    prod_oil_kbd: float
    prod_gas_mcf: float
    prod_water_kbd: float
    production_period: date