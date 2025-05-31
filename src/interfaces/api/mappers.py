"""
Mappers for converting between domain entities and API schemas.
This follows the hexagonal architecture pattern by keeping the domain layer
separate from the API layer while providing clean conversion utilities.
"""
from typing import List
from ...domain.entities.well_production import WellProduction as WellProductionEntity
from .schemas import (
    WellProductionSchema,
    WellProductionCreateSchema,
    WellProductionUpdateSchema,
    WellProductionStatsSchema
)


class WellProductionMapper:
    """Mapper for converting between WellProduction domain entity and API schemas."""
    
    @staticmethod
    def entity_to_schema(entity: WellProductionEntity) -> WellProductionSchema:
        """Convert domain entity to API schema."""
        return WellProductionSchema(
            field_code=entity.field_code,
            field_name=entity.field_name,
            well_code=entity.well_code,
            well_reference=entity.well_reference,
            well_name=entity.well_name,
            production_period=entity.production_period,
            days_on_production=entity.days_on_production,
            oil_production_kbd=entity.oil_production_kbd,
            gas_production_mmcfd=entity.gas_production_mmcfd,
            liquids_production_kbd=entity.liquids_production_kbd,
            water_production_kbd=entity.water_production_kbd,
            data_source=entity.data_source,
            source_data=entity.source_data,
            partition_0=entity.partition_0,
            created_at=entity.created_at,
            updated_at=entity.updated_at
        )
    
    @staticmethod
    def entities_to_schemas(entities: List[WellProductionEntity]) -> List[WellProductionSchema]:
        """Convert list of domain entities to list of API schemas."""
        return [WellProductionMapper.entity_to_schema(entity) for entity in entities]
    
    @staticmethod
    def create_schema_to_entity(schema: WellProductionCreateSchema) -> WellProductionEntity:
        """Convert create schema to domain entity."""
        return WellProductionEntity(
            field_code=schema.field_code,
            field_name=schema.field_name,
            well_code=schema.well_code,
            well_reference=schema.well_reference,
            well_name=schema.well_name,
            production_period=schema.production_period,
            days_on_production=schema.days_on_production,
            oil_production_kbd=schema.oil_production_kbd,
            gas_production_mmcfd=schema.gas_production_mmcfd,
            liquids_production_kbd=schema.liquids_production_kbd,
            water_production_kbd=schema.water_production_kbd,
            data_source=schema.data_source,
            source_data=schema.source_data,
            partition_0=schema.partition_0,
            created_at=None,  # Will be set by the domain service
            updated_at=None   # Will be set by the domain service
        )
    
    @staticmethod
    def update_entity_from_schema(
        entity: WellProductionEntity, 
        schema: WellProductionUpdateSchema
    ) -> WellProductionEntity:
        """Update domain entity with values from update schema."""
        # Create a new entity with updated values (immutable approach)
        return WellProductionEntity(
            field_code=entity.field_code,  # Immutable fields
            field_name=entity.field_name,
            well_code=entity.well_code,
            well_reference=entity.well_reference,
            well_name=entity.well_name,
            production_period=entity.production_period,
            partition_0=entity.partition_0,
            # Updatable fields
            days_on_production=schema.days_on_production if schema.days_on_production is not None else entity.days_on_production,
            oil_production_kbd=schema.oil_production_kbd if schema.oil_production_kbd is not None else entity.oil_production_kbd,
            gas_production_mmcfd=schema.gas_production_mmcfd if schema.gas_production_mmcfd is not None else entity.gas_production_mmcfd,
            liquids_production_kbd=schema.liquids_production_kbd if schema.liquids_production_kbd is not None else entity.liquids_production_kbd,
            water_production_kbd=schema.water_production_kbd if schema.water_production_kbd is not None else entity.water_production_kbd,
            data_source=schema.data_source if schema.data_source is not None else entity.data_source,
            source_data=schema.source_data if schema.source_data is not None else entity.source_data,
            created_at=entity.created_at,
            updated_at=None  # Will be set by the domain service
        ) 