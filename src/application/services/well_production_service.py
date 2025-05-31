"""
Service for validating well production data quality.
"""
import logging
from typing import List, Dict, Any

from ...domain.entities.well_production import WellProduction
from ...domain.repositories.well_production_repository import WellProductionRepository as WellProductionRepositoryPort
# Unused imports removed: ExternalApiPort, BatchProcessor, BatchConfig, ResponseBuilder, BatchResult, datetime, Optional, asdict
from ...shared.exceptions import (
    ValidationException, 
    BusinessRuleViolationException,
    # ApplicationException - Re-evaluate if needed for validate_data_quality's own errors
)

logger = logging.getLogger(__name__)


class WellProductionService:
    """
    Service for well production data quality operations.
    Uses dependency injection for external dependencies.
    """
    
    def __init__(
        self,
        repository: WellProductionRepositoryPort
    ):
        self.repository = repository
        # external_api and batch_processor removed
        # _import_stats removed
    
    async def validate_data_quality(
        self,
        well_productions: List[WellProduction]
    ) -> Dict[str, Any]:
        """
        Validate data quality for well production records.
        
        Args:
            well_productions: List of production records to validate
            
        Returns:
            Dictionary with validation results
        """
        validation_results = {
            "total_records": len(well_productions),
            "valid_records": 0,
            "invalid_records": 0,
            "validation_errors": []
        }
        
        for production in well_productions:
            try:
                # Business rule validations
                if not production.is_producing() and production.calculate_total_production() > 0:
                    raise BusinessRuleViolationException(
                        message="Non-producing well has production data",
                        rule="production_consistency",
                        context={"well_code": production.well_code}
                    )
                
                # Additional validations can be added here
                validation_results["valid_records"] += 1
                
            except (ValidationException, BusinessRuleViolationException) as e:
                validation_results["invalid_records"] += 1
                validation_results["validation_errors"].append(e.to_dict())
        
        validation_results["quality_score"] = (
            validation_results["valid_records"] / validation_results["total_records"] * 100
            if validation_results["total_records"] > 0 else 0
        )
        
        return validation_results
    # _validate_production_data and _insert_batch methods removed