import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

from ...domain.entities.well_production import WellProduction
from ...domain.repositories.well_production_repository import WellProductionRepository as WellProductionRepositoryPort
from ...domain.ports.external_api_port import ExternalApiPort
from ...shared.exceptions import (
    ValidationException,
    ApplicationException
)

logger = logging.getLogger(__name__)

class WellProductionQueryService:
    """
    Service for querying well production data.
    """

    def __init__(
        self,
        repository: WellProductionRepositoryPort,
        external_api: ExternalApiPort  # Needed for get_production_statistics
    ):
        self.repository = repository
        self.external_api = external_api

    async def get_production_by_well(
        self,
        well_code: int,
        period_start: Optional[datetime] = None,
        period_end: Optional[datetime] = None
    ) -> List[WellProduction]:
        """
        Get production data for a specific well.

        Args:
            well_code: Well identification code
            period_start: Optional start date for filtering
            period_end: Optional end date for filtering

        Returns:
            List of WellProduction entities

        Raises:
            ValidationException: When parameters are invalid
        """
        try:
            if well_code <= 0:
                raise ValidationException(
                    message="Well code must be positive",
                    field="well_code",
                    value=well_code
                )

            # Get all records for the well
            wells = await self.repository.get_by_well_code(well_code)

            # Apply date filtering if provided
            if period_start or period_end:
                filtered_wells = []
                for well in wells:
                    # Parse production_period (assuming format like "2024-01" or "2024-01-01")
                    try:
                        if len(well.production_period) == 7:  # "2024-01" format
                            well_date = datetime.strptime(well.production_period + "-01", "%Y-%m-%d")
                        else:  # Assume full date format
                            well_date = datetime.strptime(well.production_period, "%Y-%m-%d")

                        # Check if within date range
                        if period_start and well_date < period_start:
                            continue
                        if period_end and well_date > period_end:
                            continue

                        filtered_wells.append(well)
                    except ValueError:
                        # If date parsing fails, include the record
                        filtered_wells.append(well)

                wells = filtered_wells

            return wells

        except ApplicationException:
            raise
        except Exception as e:
            logger.error(f"Error retrieving well production for {well_code}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to retrieve well production: {str(e)}",
                cause=e
            )

    async def get_production_by_field(
        self,
        field_code: int,
        limit: Optional[int] = None
    ) -> List[WellProduction]:
        """
        Get production data for all wells in a field.

        Args:
            field_code: Field identification code
            limit: Optional limit for number of records

        Returns:
            List of WellProduction entities
        """
        try:
            if field_code <= 0:
                raise ValidationException(
                    message="Field code must be positive",
                    field="field_code",
                    value=field_code
                )

            # Get all records for the field
            wells = await self.repository.get_by_field_code(field_code)

            # Apply limit if provided
            if limit and limit > 0:
                wells = wells[:limit]

            return wells

        except ApplicationException:
            raise
        except Exception as e:
            logger.error(f"Error retrieving field production for {field_code}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to retrieve field production: {str(e)}",
                cause=e
            )

    async def get_production_statistics(self) -> Dict[str, Any]:
        """
        Get overall production statistics.

        Returns:
            Dictionary with production statistics
        """
        try:
            total_count = await self.repository.count()

            # Get external API status
            api_status = await self.external_api.get_api_status()

            # Batch processor status is not relevant for QueryService, so removed
            # self.batch_processor.get_memory_status()

            return {
                "total_records": total_count,
                "external_api_status": api_status,
                "last_updated": datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Error getting production statistics: {str(e)}")
            raise ApplicationException(
                message=f"Failed to get statistics: {str(e)}",
                cause=e
            )
