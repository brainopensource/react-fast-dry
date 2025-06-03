"""
Generic DuckDB repository implementation that can handle any dataset.
Follows Repository pattern and SOLID principles with high performance.
"""

import logging
import asyncio
from typing import List, Optional, Dict, Any, AsyncIterator
from datetime import datetime
import duckdb
import pandas as pd
from contextlib import asynccontextmanager

from ...domain.ports.repository import (
    IFullRepository, QueryOptions, QueryFilter, QuerySort, QueryPagination,
    RepositoryResult, BulkOperationResult
)
from ...shared.config.schemas import (
    DatasetSchemaConfig, DatasetFactory, BaseEntity, get_dataset_config
)
from ...shared.exceptions import (
    ApplicationException, ValidationException, ErrorCode
)

logger = logging.getLogger(__name__)

class GenericDuckDBRepository(IFullRepository):
    """
    Generic DuckDB repository that can handle any dataset.
    Provides high-performance data access with async support.
    """
    
    def __init__(
        self,
        dataset_name: str,
        db_path: str,
        config: Optional[DatasetSchemaConfig] = None
    ):
        """Initialize the generic repository."""
        self.dataset_name = dataset_name
        self.db_path = db_path
        self.config = config or get_dataset_config(dataset_name)
        self.entity_class = DatasetFactory.get_entity_class(dataset_name)
        self.table_name = self.config.table_name
        self.primary_keys = self.config.primary_keys
        self.logger = logging.getLogger(f"{__name__}.{dataset_name}")
        
        # Connection pool for async operations
        self._connection_pool: List[duckdb.DuckDBPyConnection] = []
        self._pool_size = 5
        self._initialized = False
    
    async def _ensure_initialized(self):
        """Ensure the repository is initialized."""
        if not self._initialized:
            await self._initialize()
    
    async def _initialize(self):
        """Initialize the repository and create table if needed."""
        try:
            # Create connection pool
            for _ in range(self._pool_size):
                conn = duckdb.connect(self.db_path)
                self._connection_pool.append(conn)
            
            # Create table if it doesn't exist
            await self._create_table_if_not_exists()
            
            # Create indexes
            await self._create_indexes()
            
            self._initialized = True
            self.logger.info(f"Initialized DuckDB repository for {self.dataset_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize repository for {self.dataset_name}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to initialize repository for {self.dataset_name}",
                error_code=ErrorCode.INITIALIZATION_ERROR,
                cause=e
            )
    
    @asynccontextmanager
    async def _get_connection(self):
        """Get a connection from the pool."""
        await self._ensure_initialized()
        
        if not self._connection_pool:
            # Create a new connection if pool is empty
            conn = duckdb.connect(self.db_path)
        else:
            conn = self._connection_pool.pop()
        
        try:
            yield conn
        finally:
            if len(self._connection_pool) < self._pool_size:
                self._connection_pool.append(conn)
            else:
                conn.close()
    
    async def _create_table_if_not_exists(self):
        """Create table based on configuration if it doesn't exist."""
        try:
            # Use a direct connection during initialization to avoid circular dependency
            conn = duckdb.connect(self.db_path)
            
            try:
                # Build CREATE TABLE statement
                columns = []
                
                for field_def in self.config.fields:
                    column_type = self._python_type_to_duckdb_type(field_def.field_type)
                    nullable = "NULL" if not field_def.required else "NOT NULL"
                    columns.append(f"{field_def.name} {column_type} {nullable}")
                
                # Add common fields
                columns.extend([
                    "created_at TIMESTAMP NULL",
                    "updated_at TIMESTAMP NULL"
                ])
                
                # Build primary key constraint
                if self.primary_keys:
                    pk_constraint = f", PRIMARY KEY ({', '.join(self.primary_keys)})"
                else:
                    pk_constraint = ""
                
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    {', '.join(columns)}{pk_constraint}
                )
                """
                
                await asyncio.get_event_loop().run_in_executor(
                    None, conn.execute, create_sql
                )
                
                self.logger.info(f"Table {self.table_name} created/verified")
                
            finally:
                conn.close()
                
        except Exception as e:
            self.logger.error(f"Failed to create table {self.table_name}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to initialize repository for {self.dataset_name}",
                error_code=ErrorCode.INITIALIZATION_ERROR,
                cause=e
            )
    
    async def _create_indexes(self):
        """Create indexes based on configuration."""
        try:
            # Use a direct connection during initialization to avoid circular dependency
            conn = duckdb.connect(self.db_path)
            
            try:
                for i, index_fields in enumerate(self.config.indexes):
                    index_name = f"idx_{self.table_name}_{i}"
                    fields_str = ", ".join(index_fields)
                    
                    index_sql = f"""
                    CREATE INDEX IF NOT EXISTS {index_name}
                    ON {self.table_name} ({fields_str})
                    """
                    
                    await asyncio.get_event_loop().run_in_executor(
                        None, conn.execute, index_sql
                    )
                
                self.logger.info(f"Indexes created for {self.table_name}")
                
            finally:
                conn.close()
                
        except Exception as e:
            self.logger.error(f"Failed to create indexes for {self.table_name}: {str(e)}")
            # Don't raise here as indexes are not critical for functionality
    
    def _python_type_to_duckdb_type(self, python_type: type) -> str:
        """Convert Python type to DuckDB type."""
        type_mapping = {
            str: "VARCHAR",
            int: "BIGINT",
            float: "DOUBLE",
            bool: "BOOLEAN",
            datetime: "TIMESTAMP"
        }
        
        return type_mapping.get(python_type, "VARCHAR")
    
    def _build_where_clause(self, filters: List[QueryFilter]) -> tuple[str, List[Any]]:
        """Build WHERE clause from filters."""
        if not filters:
            return "", []
        
        conditions = []
        params = []
        
        for filter_obj in filters:
            field = filter_obj.field
            operator = filter_obj.operator.lower()
            value = filter_obj.value
            
            if operator == "eq":
                conditions.append(f"{field} = ?")
                params.append(value)
            elif operator == "ne":
                conditions.append(f"{field} != ?")
                params.append(value)
            elif operator == "gt":
                conditions.append(f"{field} > ?")
                params.append(value)
            elif operator == "gte":
                conditions.append(f"{field} >= ?")
                params.append(value)
            elif operator == "lt":
                conditions.append(f"{field} < ?")
                params.append(value)
            elif operator == "lte":
                conditions.append(f"{field} <= ?")
                params.append(value)
            elif operator == "like":
                conditions.append(f"{field} LIKE ?")
                params.append(value)
            elif operator == "in":
                placeholders = ",".join("?" * len(value))
                conditions.append(f"{field} IN ({placeholders})")
                params.extend(value)
        
        where_clause = "WHERE " + " AND ".join(conditions)
        return where_clause, params
    
    def _build_order_clause(self, sorts: List[QuerySort]) -> str:
        """Build ORDER BY clause from sorts."""
        if not sorts:
            return ""
        
        order_parts = []
        for sort_obj in sorts:
            direction = sort_obj.direction.upper()
            order_parts.append(f"{sort_obj.field} {direction}")
        
        return "ORDER BY " + ", ".join(order_parts)
    
    def _row_to_entity(self, row: tuple, columns: List[str]) -> BaseEntity:
        """Convert database row to entity."""
        try:
            data = dict(zip(columns, row))
            return DatasetFactory.create_entity(self.dataset_name, **data)
        except Exception as e:
            self.logger.error(f"Failed to convert row to entity: {str(e)}")
            raise
    
    def _entity_to_dict(self, entity: BaseEntity) -> Dict[str, Any]:
        """Convert entity to dictionary for database operations."""
        data = entity.to_dict()
        
        # Ensure required fields are present
        if 'created_at' not in data:
            data['created_at'] = datetime.now()
        if 'updated_at' not in data:
            data['updated_at'] = datetime.now()
        
        return data
    
    # Repository Interface Implementation
    async def find_by_id(self, entity_id: str) -> Optional[BaseEntity]:
        """Find entity by ID."""
        try:
            if not self.primary_keys:
                raise ApplicationException(
                    message="Cannot find by ID: no primary key defined",
                    error_code=ErrorCode.CONFIGURATION_ERROR
                )
            
            # For composite primary keys, entity_id should be a delimited string
            if len(self.primary_keys) > 1:
                id_parts = entity_id.split(":")
                if len(id_parts) != len(self.primary_keys):
                    return None
                
                conditions = []
                params = []
                for key, value in zip(self.primary_keys, id_parts):
                    conditions.append(f"{key} = ?")
                    params.append(value)
                
                where_clause = "WHERE " + " AND ".join(conditions)
            else:
                where_clause = f"WHERE {self.primary_keys[0]} = ?"
                params = [entity_id]
            
            async with self._get_connection() as conn:
                sql = f"SELECT * FROM {self.table_name} {where_clause}"
                
                result = await asyncio.get_event_loop().run_in_executor(
                    None, conn.execute, sql, params
                )
                
                row = result.fetchone()
                if not row:
                    return None
                
                columns = [desc[0] for desc in result.description]
                return self._row_to_entity(row, columns)
                
        except Exception as e:
            self.logger.error(f"Error finding {self.dataset_name} by ID {entity_id}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to find {self.dataset_name} by ID",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    async def find_all(self, options: Optional[QueryOptions] = None) -> RepositoryResult[BaseEntity]:
        """Find all entities matching criteria."""
        try:
            options = options or QueryOptions()
            
            # Build WHERE clause
            where_clause, where_params = self._build_where_clause(options.filters)
            
            # Build ORDER BY clause
            order_clause = self._build_order_clause(options.sorts)
            
            # Build LIMIT/OFFSET clause
            limit_clause = ""
            limit_params = []
            if options.pagination:
                limit_clause = "LIMIT ? OFFSET ?"
                limit_params = [options.pagination.limit, options.pagination.offset]
            
            async with self._get_connection() as conn:
                # Count total records
                count_sql = f"SELECT COUNT(*) FROM {self.table_name} {where_clause}"
                count_result = await asyncio.get_event_loop().run_in_executor(
                    None, conn.execute, count_sql, where_params
                )
                total_count = count_result.fetchone()[0]
                
                # Get data
                sql = f"""
                SELECT * FROM {self.table_name}
                {where_clause}
                {order_clause}
                {limit_clause}
                """
                
                all_params = where_params + limit_params
                result = await asyncio.get_event_loop().run_in_executor(
                    None, conn.execute, sql, all_params
                )
                
                rows = result.fetchall()
                columns = [desc[0] for desc in result.description]
                
                entities = [self._row_to_entity(row, columns) for row in rows]
                
                # Determine if there are more records
                has_more = False
                if options.pagination:
                    next_offset = options.pagination.offset + options.pagination.limit
                    has_more = next_offset < total_count
                
                return RepositoryResult(
                    items=entities,
                    total_count=total_count,
                    has_more=has_more
                )
                
        except Exception as e:
            self.logger.error(f"Error finding {self.dataset_name}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to find {self.dataset_name} records",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    async def save(self, entity: BaseEntity) -> BaseEntity:
        """Save a single entity."""
        try:
            data = self._entity_to_dict(entity)
            
            # Build INSERT statement
            columns = list(data.keys())
            placeholders = ",".join("?" * len(columns))
            values = [data[col] for col in columns]
            
            async with self._get_connection() as conn:
                sql = f"""
                INSERT INTO {self.table_name} ({",".join(columns)})
                VALUES ({placeholders})
                """
                
                await asyncio.get_event_loop().run_in_executor(
                    None, conn.execute, sql, values
                )
                
                # Return the saved entity (with any generated values)
                return entity
                
        except Exception as e:
            self.logger.error(f"Error saving {self.dataset_name}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to save {self.dataset_name}",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    async def save_many(self, entities: List[BaseEntity]) -> BulkOperationResult:
        """Save multiple entities."""
        try:
            if not entities:
                return BulkOperationResult(0, 0, 0)
            
            successful_count = 0
            failed_count = 0
            errors = []
            
            # Convert entities to data dictionaries
            batch_data = []
            for i, entity in enumerate(entities):
                try:
                    data = self._entity_to_dict(entity)
                    batch_data.append(data)
                except Exception as e:
                    failed_count += 1
                    errors.append(f"Entity {i}: {str(e)}")
            
            if batch_data:
                # Use pandas for efficient bulk insert
                df = pd.DataFrame(batch_data)
                
                async with self._get_connection() as conn:
                    try:
                        await asyncio.get_event_loop().run_in_executor(
                            None, lambda: conn.register('batch_df', df)
                        )
                        
                        sql = f"""
                        INSERT INTO {self.table_name}
                        SELECT * FROM batch_df
                        """
                        
                        await asyncio.get_event_loop().run_in_executor(
                            None, conn.execute, sql
                        )
                        
                        successful_count = len(batch_data)
                        
                    except Exception as e:
                        # Fallback to individual inserts
                        for i, data in enumerate(batch_data):
                            try:
                                columns = list(data.keys())
                                placeholders = ",".join("?" * len(columns))
                                values = [data[col] for col in columns]
                                
                                sql = f"""
                                INSERT INTO {self.table_name} ({",".join(columns)})
                                VALUES ({placeholders})
                                """
                                
                                await asyncio.get_event_loop().run_in_executor(
                                    None, conn.execute, sql, values
                                )
                                
                                successful_count += 1
                                
                            except Exception as insert_error:
                                failed_count += 1
                                errors.append(f"Record {i}: {str(insert_error)}")
            
            return BulkOperationResult(
                successful_count=successful_count,
                failed_count=failed_count,
                total_count=len(entities),
                errors=errors
            )
            
        except Exception as e:
            self.logger.error(f"Error in bulk save for {self.dataset_name}: {str(e)}")
            return BulkOperationResult(
                successful_count=0,
                failed_count=len(entities),
                total_count=len(entities),
                errors=[str(e)]
            )
    
    async def update(self, entity_id: str, updates: Dict[str, Any]) -> Optional[BaseEntity]:
        """Update entity by ID."""
        try:
            if not self.primary_keys:
                raise ApplicationException(
                    message="Cannot update by ID: no primary key defined",
                    error_code=ErrorCode.CONFIGURATION_ERROR
                )
            
            # Add updated_at timestamp
            updates['updated_at'] = datetime.now()
            
            # Build UPDATE statement
            set_clauses = [f"{col} = ?" for col in updates.keys()]
            values = list(updates.values())
            
            # Build WHERE clause for primary key
            if len(self.primary_keys) > 1:
                id_parts = entity_id.split(":")
                if len(id_parts) != len(self.primary_keys):
                    return None
                
                pk_conditions = []
                for key, value in zip(self.primary_keys, id_parts):
                    pk_conditions.append(f"{key} = ?")
                    values.append(value)
                
                where_clause = "WHERE " + " AND ".join(pk_conditions)
            else:
                where_clause = f"WHERE {self.primary_keys[0]} = ?"
                values.append(entity_id)
            
            async with self._get_connection() as conn:
                sql = f"""
                UPDATE {self.table_name}
                SET {", ".join(set_clauses)}
                {where_clause}
                """
                
                await asyncio.get_event_loop().run_in_executor(
                    None, conn.execute, sql, values
                )
                
                # Return updated entity
                return await self.find_by_id(entity_id)
                
        except Exception as e:
            self.logger.error(f"Error updating {self.dataset_name} {entity_id}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to update {self.dataset_name}",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    async def delete(self, entity_id: str) -> bool:
        """Delete entity by ID."""
        try:
            if not self.primary_keys:
                raise ApplicationException(
                    message="Cannot delete by ID: no primary key defined",
                    error_code=ErrorCode.CONFIGURATION_ERROR
                )
            
            # Build WHERE clause for primary key
            if len(self.primary_keys) > 1:
                id_parts = entity_id.split(":")
                if len(id_parts) != len(self.primary_keys):
                    return False
                
                conditions = []
                params = []
                for key, value in zip(self.primary_keys, id_parts):
                    conditions.append(f"{key} = ?")
                    params.append(value)
                
                where_clause = "WHERE " + " AND ".join(conditions)
            else:
                where_clause = f"WHERE {self.primary_keys[0]} = ?"
                params = [entity_id]
            
            async with self._get_connection() as conn:
                sql = f"DELETE FROM {self.table_name} {where_clause}"
                
                result = await asyncio.get_event_loop().run_in_executor(
                    None, conn.execute, sql, params
                )
                
                return result.rowcount > 0
                
        except Exception as e:
            self.logger.error(f"Error deleting {self.dataset_name} {entity_id}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to delete {self.dataset_name}",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    async def delete_many(self, filters: List[QueryFilter]) -> int:
        """Delete multiple entities matching filters."""
        try:
            where_clause, params = self._build_where_clause(filters)
            
            async with self._get_connection() as conn:
                sql = f"DELETE FROM {self.table_name} {where_clause}"
                
                result = await asyncio.get_event_loop().run_in_executor(
                    None, conn.execute, sql, params
                )
                
                return result.rowcount
                
        except Exception as e:
            self.logger.error(f"Error deleting {self.dataset_name} records: {str(e)}")
            raise ApplicationException(
                message=f"Failed to delete {self.dataset_name} records",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    async def count(self, filters: Optional[List[QueryFilter]] = None) -> int:
        """Count entities matching filters."""
        try:
            where_clause, params = self._build_where_clause(filters or [])
            
            async with self._get_connection() as conn:
                sql = f"SELECT COUNT(*) FROM {self.table_name} {where_clause}"
                
                result = await asyncio.get_event_loop().run_in_executor(
                    None, conn.execute, sql, params
                )
                
                return result.fetchone()[0]
                
        except Exception as e:
            self.logger.error(f"Error counting {self.dataset_name}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to count {self.dataset_name}",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    async def exists(self, entity_id: str) -> bool:
        """Check if entity exists."""
        try:
            entity = await self.find_by_id(entity_id)
            return entity is not None
        except Exception as e:
            self.logger.error(f"Error checking existence of {self.dataset_name} {entity_id}: {str(e)}")
            return False
    
    # Analytics methods
    async def aggregate(self, aggregations: Dict[str, str], filters: Optional[List[QueryFilter]] = None) -> Dict[str, Any]:
        """Perform aggregations."""
        try:
            where_clause, params = self._build_where_clause(filters or [])
            
            # Build SELECT clause with aggregations
            select_parts = []
            for alias, expression in aggregations.items():
                select_parts.append(f"{expression} AS {alias}")
            
            async with self._get_connection() as conn:
                sql = f"""
                SELECT {", ".join(select_parts)}
                FROM {self.table_name}
                {where_clause}
                """
                
                result = await asyncio.get_event_loop().run_in_executor(
                    None, conn.execute, sql, params
                )
                
                row = result.fetchone()
                if not row:
                    return {}
                
                columns = [desc[0] for desc in result.description]
                return dict(zip(columns, row))
                
        except Exception as e:
            self.logger.error(f"Error getting aggregates for {self.dataset_name}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to get aggregates for {self.dataset_name}",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    async def group_by(self, group_fields: List[str], aggregations: Dict[str, str], filters: Optional[List[QueryFilter]] = None) -> List[Dict[str, Any]]:
        """Group by fields with aggregations."""
        try:
            where_clause, params = self._build_where_clause(filters or [])
            
            # Build SELECT clause
            select_parts = group_fields.copy()
            for alias, expression in aggregations.items():
                select_parts.append(f"{expression} AS {alias}")
            
            # Build GROUP BY clause
            group_by_clause = f"GROUP BY {', '.join(group_fields)}"
            
            async with self._get_connection() as conn:
                sql = f"""
                SELECT {", ".join(select_parts)}
                FROM {self.table_name}
                {where_clause}
                {group_by_clause}
                """
                
                result = await asyncio.get_event_loop().run_in_executor(
                    None, conn.execute, sql, params
                )
                
                rows = result.fetchall()
                columns = [desc[0] for desc in result.description]
                
                return [dict(zip(columns, row)) for row in rows]
                
        except Exception as e:
            self.logger.error(f"Error getting group by for {self.dataset_name}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to get group by for {self.dataset_name}",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    async def get_distinct_values(self, field: str, filters: Optional[List[QueryFilter]] = None) -> List[Any]:
        """Get distinct values for a field."""
        try:
            where_clause, params = self._build_where_clause(filters or [])
            
            async with self._get_connection() as conn:
                sql = f"""
                SELECT DISTINCT {field}
                FROM {self.table_name}
                {where_clause}
                ORDER BY {field}
                """
                
                result = await asyncio.get_event_loop().run_in_executor(
                    None, conn.execute, sql, params
                )
                
                rows = result.fetchall()
                return [row[0] for row in rows]
                
        except Exception as e:
            self.logger.error(f"Error getting distinct values for {self.dataset_name}.{field}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to get distinct values for {self.dataset_name}",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    # Streaming methods
    async def stream_all(self, options: Optional[QueryOptions] = None) -> AsyncIterator[BaseEntity]:
        """Stream all entities matching criteria."""
        try:
            options = options or QueryOptions()
            
            # Build WHERE clause
            where_clause, where_params = self._build_where_clause(options.filters)
            
            # Build ORDER BY clause
            order_clause = self._build_order_clause(options.sorts)
            
            async with self._get_connection() as conn:
                sql = f"""
                SELECT * FROM {self.table_name}
                {where_clause}
                {order_clause}
                """
                
                result = await asyncio.get_event_loop().run_in_executor(
                    None, conn.execute, sql, where_params
                )
                
                columns = [desc[0] for desc in result.description]
                
                while True:
                    row = result.fetchone()
                    if not row:
                        break
                    
                    entity = self._row_to_entity(row, columns)
                    yield entity
                    
        except Exception as e:
            self.logger.error(f"Error streaming {self.dataset_name}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to stream {self.dataset_name}",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    async def stream_by_batch(self, batch_size: int = 1000, options: Optional[QueryOptions] = None) -> AsyncIterator[List[BaseEntity]]:
        """Stream entities in batches."""
        try:
            batch = []
            async for entity in self.stream_all(options):
                batch.append(entity)
                
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            
            # Yield remaining entities
            if batch:
                yield batch
                
        except Exception as e:
            self.logger.error(f"Error streaming batches for {self.dataset_name}: {str(e)}")
            raise ApplicationException(
                message=f"Failed to stream batches for {self.dataset_name}",
                error_code=ErrorCode.DATA_ACCESS_ERROR,
                cause=e
            )
    
    # Bulk operations
    async def bulk_insert(self, entities: List[BaseEntity], batch_size: int = 1000) -> BulkOperationResult:
        """Bulk insert entities."""
        return await self.save_many(entities)
    
    async def bulk_update(self, updates: List[Dict[str, Any]], batch_size: int = 1000) -> BulkOperationResult:
        """Bulk update entities."""
        # This would require more complex implementation
        # For now, return a placeholder
        return BulkOperationResult(0, len(updates), len(updates), ["Bulk update not yet implemented"])
    
    async def bulk_upsert(self, entities: List[BaseEntity], batch_size: int = 1000) -> BulkOperationResult:
        """Bulk upsert entities."""
        # This would require more complex implementation with conflict resolution
        # For now, return a placeholder
        return BulkOperationResult(0, len(entities), len(entities), ["Bulk upsert not yet implemented"])
    
    def close(self):
        """Close all connections in the pool."""
        for conn in self._connection_pool:
            try:
                conn.close()
            except:
                pass
        self._connection_pool.clear()
        self._initialized = False 