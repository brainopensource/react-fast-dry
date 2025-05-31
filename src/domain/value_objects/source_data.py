from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class SourceData:
    """Value object representing the source data from ANP."""
    year: str
    month_year: str
    state: str
    basin: str
    field: str
    well: str
    type: str
    installation: str
    oil_production_m3: str
    cond_production_m3: str
    ass_gas_prod_mm3: str
    non_ass_gas_prod_mm3: str
    water_production_m3: str
    location: str

    @classmethod
    def from_json_str(cls, json_str: str) -> 'SourceData':
        """Create a SourceData instance from a JSON string."""
        import json
        data = json.loads(json_str)
        return cls(
            year=data.get('Year', ''),
            month_year=data.get('Month_Year', ''),
            state=data.get('State', ''),
            basin=data.get('Basin', ''),
            field=data.get('Field', ''),
            well=data.get('Well', ''),
            type=data.get('Type', ''),
            installation=data.get('Instalation', ''),
            oil_production_m3=data.get('Oil_Production_M3', ''),
            cond_production_m3=data.get('Cond_Production_M3', ''),
            ass_gas_prod_mm3=data.get('Ass_Gas_Prod_MM3', ''),
            non_ass_gas_prod_mm3=data.get('Non_Ass_Gas_Prod_MM3', ''),
            water_production_m3=data.get('Water_Production_M3', ''),
            location=data.get('Location', '')
        ) 