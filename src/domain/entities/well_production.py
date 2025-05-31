from datetime import datetime
from dataclasses import dataclass
from typing import Optional

@dataclass
class WellProduction:
    """Domain entity representing well production data."""
    field_code: int
    field_name: str
    well_code: int
    well_reference: str
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
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def calculate_total_production(self) -> float:
        """Calculate total production in KBD."""
        return self.oil_production_kbd + self.liquids_production_kbd + self.water_production_kbd

    def is_producing(self) -> bool:
        """Check if the well is currently producing."""
        return self.days_on_production > 0 