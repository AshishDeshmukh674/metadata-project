"""
Metadata Normalizer.

Converts format-specific metadata (Iceberg, Delta, Hudi) into the unified
TableMetadata model for consistent internal representation.
"""

from typing import Dict, List
from datetime import datetime

from ..models.table_metadata import TableMetadata, ColumnMetadata
from ..utils.logger import setup_logger
from ..utils.exceptions import NormalizationError

logger = setup_logger(__name__)


class MetadataNormalizer:
    """
    Normalizes format-specific metadata into unified TableMetadata model.
    
    Handles:
    - Schema/column normalization
    - Type mapping to SQL-friendly types
    - Partition field extraction
    - Time travel capability detection
    """
    
    # Type mappings for different formats to SQL types
    ICEBERG_TYPE_MAP = {
        "boolean": "BOOLEAN",
        "int": "INTEGER",
        "long": "BIGINT",
        "float": "FLOAT",
        "double": "DOUBLE",
        "decimal": "DECIMAL",
        "date": "DATE",
        "time": "TIME",
        "timestamp": "TIMESTAMP",
        "timestamptz": "TIMESTAMP WITH TIME ZONE",
        "string": "VARCHAR",
        "uuid": "UUID",
        "fixed": "BINARY",
        "binary": "BINARY",
    }
    
    DELTA_TYPE_MAP = {
        "boolean": "BOOLEAN",
        "byte": "TINYINT",
        "short": "SMALLINT",
        "integer": "INTEGER",
        "long": "BIGINT",
        "float": "FLOAT",
        "double": "DOUBLE",
        "decimal": "DECIMAL",
        "string": "VARCHAR",
        "binary": "BINARY",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
    }
    
    HUDI_TYPE_MAP = {
        "boolean": "BOOLEAN",
        "int": "INTEGER",
        "long": "BIGINT",
        "float": "FLOAT",
        "double": "DOUBLE",
        "decimal": "DECIMAL",
        "string": "VARCHAR",
        "binary": "BINARY",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
    }
    
    def normalize(self, raw_metadata: Dict, table_format: str) -> TableMetadata:
        """
        Normalize format-specific metadata to TableMetadata.
        
        Args:
            raw_metadata: Raw metadata dictionary from format-specific reader
            table_format: Format type ("ICEBERG", "DELTA", "HUDI")
            
        Returns:
            Normalized TableMetadata object
            
        Raises:
            NormalizationError: If normalization fails
        """
        logger.info(f"Normalizing {table_format} metadata")
        
        try:
            if table_format == "ICEBERG":
                return self._normalize_iceberg(raw_metadata)
            elif table_format == "DELTA":
                return self._normalize_delta(raw_metadata)
            elif table_format == "HUDI":
                return self._normalize_hudi(raw_metadata)
            else:
                raise NormalizationError(
                    f"Unsupported table format: {table_format}",
                    details={"format": table_format}
                )
        except NormalizationError:
            raise
        except Exception as e:
            raise NormalizationError(
                f"Failed to normalize {table_format} metadata: {str(e)}",
                details={"format": table_format, "error": str(e)}
            )
    
    def _normalize_iceberg(self, raw_metadata: Dict) -> TableMetadata:
        """Normalize Iceberg metadata to TableMetadata."""
        logger.debug("Normalizing Iceberg metadata")
        
        # Extract table name from location
        location = raw_metadata.get("location", "")
        table_name = self._extract_table_name_from_path(location)
        
        # Normalize columns
        columns = self._normalize_iceberg_columns(raw_metadata.get("schema", []))
        
        # Extract partition columns
        partitions = self._extract_iceberg_partition_columns(
            raw_metadata.get("partition_spec", []),
            raw_metadata.get("schema", [])
        )
        
        # Check time travel support (Iceberg always supports it via snapshots)
        supports_time_travel = len(raw_metadata.get("snapshots", [])) > 0
        
        # Build TableMetadata
        metadata = TableMetadata(
            table_name=table_name,
            format="ICEBERG",
            location=location,
            columns=columns,
            partitions=partitions,
            properties=raw_metadata.get("properties", {}),
            supports_time_travel=supports_time_travel,
            updated_at=datetime.now()
        )
        
        # Add format-specific properties
        if raw_metadata.get("current_snapshot_id"):
            metadata.properties["iceberg.current_snapshot_id"] = str(raw_metadata["current_snapshot_id"])
        if raw_metadata.get("format_version"):
            metadata.properties["iceberg.format_version"] = str(raw_metadata["format_version"])
        
        logger.info(f"Normalized Iceberg table: {table_name} with {len(columns)} columns")
        return metadata
    
    def _normalize_delta(self, raw_metadata: Dict) -> TableMetadata:
        """Normalize Delta Lake metadata to TableMetadata."""
        logger.debug("Normalizing Delta metadata")
        
        # Extract table name from location or properties
        location = raw_metadata.get("location", "")
        table_name = raw_metadata.get("properties", {}).get("table.name")
        if not table_name:
            table_name = self._extract_table_name_from_path(location)
        
        # Normalize columns
        columns = self._normalize_delta_columns(raw_metadata.get("schema", []))
        
        # Partition columns are directly listed in Delta
        partitions = raw_metadata.get("partition_columns", [])
        
        # Delta always supports time travel
        supports_time_travel = raw_metadata.get("supports_time_travel", True)
        
        # Build TableMetadata
        metadata = TableMetadata(
            table_name=table_name,
            format="DELTA",
            location=location,
            columns=columns,
            partitions=partitions,
            properties=raw_metadata.get("properties", {}),
            supports_time_travel=supports_time_travel,
            updated_at=datetime.now()
        )
        
        # Add format-specific properties
        if raw_metadata.get("version") is not None:
            metadata.properties["delta.version"] = str(raw_metadata["version"])
        if raw_metadata.get("protocol"):
            protocol = raw_metadata["protocol"]
            metadata.properties["delta.minReaderVersion"] = str(protocol.get("minReaderVersion", ""))
            metadata.properties["delta.minWriterVersion"] = str(protocol.get("minWriterVersion", ""))
        
        logger.info(f"Normalized Delta table: {table_name} with {len(columns)} columns")
        return metadata
    
    def _normalize_hudi(self, raw_metadata: Dict) -> TableMetadata:
        """Normalize Hudi metadata to TableMetadata."""
        logger.debug("Normalizing Hudi metadata")
        
        # Hudi provides table name in properties
        table_name = raw_metadata.get("table_name", "unknown")
        location = raw_metadata.get("location", "")
        
        # Normalize columns
        columns = self._normalize_hudi_columns(raw_metadata.get("schema", []))
        
        # Partition fields
        partitions = raw_metadata.get("partition_fields", [])
        
        # Time travel support based on timeline
        supports_time_travel = raw_metadata.get("supports_time_travel", False)
        
        # Build TableMetadata
        metadata = TableMetadata(
            table_name=table_name,
            format="HUDI",
            location=location,
            columns=columns,
            partitions=partitions,
            properties=raw_metadata.get("properties", {}),
            supports_time_travel=supports_time_travel,
            updated_at=datetime.now()
        )
        
        # Add format-specific properties
        if raw_metadata.get("table_type"):
            metadata.properties["hudi.table.type"] = raw_metadata["table_type"]
        timeline = raw_metadata.get("timeline", [])
        if timeline:
            metadata.properties["hudi.commits.count"] = str(len(timeline))
        
        logger.info(f"Normalized Hudi table: {table_name} with {len(columns)} columns")
        return metadata
    
    def _normalize_iceberg_columns(self, schema_fields: List[Dict]) -> List[ColumnMetadata]:
        """Convert Iceberg schema fields to ColumnMetadata."""
        columns = []
        
        for field in schema_fields:
            name = field.get("name", "")
            field_type = field.get("type", "string")
            
            # Handle complex types (struct, list, map)
            if isinstance(field_type, dict):
                field_type = field_type.get("type", "string")
            
            # Map Iceberg type to SQL type
            sql_type = self._map_iceberg_type(field_type)
            
            # Iceberg uses "required" field (True means NOT NULL)
            nullable = not field.get("required", False)
            
            comment = field.get("doc") or field.get("comment")
            
            columns.append(ColumnMetadata(
                name=name,
                data_type=sql_type,
                nullable=nullable,
                comment=comment
            ))
        
        return columns
    
    def _normalize_delta_columns(self, schema_fields: List[Dict]) -> List[ColumnMetadata]:
        """Convert Delta schema fields to ColumnMetadata."""
        columns = []
        
        for field in schema_fields:
            name = field.get("name", "")
            field_type = field.get("type", "string")
            
            # Handle complex types
            if isinstance(field_type, dict):
                field_type = field_type.get("type", "string")
            
            # Map Delta type to SQL type
            sql_type = self._map_delta_type(field_type)
            
            nullable = field.get("nullable", True)
            
            # Delta stores metadata in a nested dict
            metadata = field.get("metadata", {})
            comment = metadata.get("comment")
            
            columns.append(ColumnMetadata(
                name=name,
                data_type=sql_type,
                nullable=nullable,
                comment=comment
            ))
        
        return columns
    
    def _normalize_hudi_columns(self, schema_fields: List[Dict]) -> List[ColumnMetadata]:
        """Convert Hudi schema fields to ColumnMetadata."""
        columns = []
        
        for field in schema_fields:
            name = field.get("name", "")
            field_type = field.get("type", "string")
            
            # Handle complex types
            if isinstance(field_type, dict):
                field_type = field_type.get("type", "string")
            
            # Map Hudi type to SQL type
            sql_type = self._map_hudi_type(field_type)
            
            # Hudi schema is based on Avro, which has "null" unions for nullable
            nullable = True  # Default to nullable unless specified
            if isinstance(field_type, list):
                nullable = "null" in field_type
            
            comment = field.get("doc")
            
            columns.append(ColumnMetadata(
                name=name,
                data_type=sql_type,
                nullable=nullable,
                comment=comment
            ))
        
        return columns
    
    def _extract_iceberg_partition_columns(self, partition_spec: List[Dict],
                                          schema: List[Dict]) -> List[str]:
        """Extract partition column names from Iceberg partition spec."""
        partition_columns = []
        
        # Build a map of field IDs to names
        field_id_to_name = {field.get("id"): field.get("name") for field in schema}
        
        for partition_field in partition_spec:
            # Partition spec references source column by field ID
            source_id = partition_field.get("source-id") or partition_field.get("sourceId")
            if source_id and source_id in field_id_to_name:
                partition_columns.append(field_id_to_name[source_id])
        
        return partition_columns
    
    def _map_iceberg_type(self, iceberg_type: str) -> str:
        """Map Iceberg type to SQL type."""
        # Handle parameterized types like decimal(10,2)
        base_type = iceberg_type.split("(")[0].lower()
        
        sql_type = self.ICEBERG_TYPE_MAP.get(base_type)
        if sql_type:
            # Preserve parameters for types like decimal
            if "(" in iceberg_type:
                sql_type += iceberg_type[iceberg_type.index("("):]
            return sql_type
        
        # Default to VARCHAR for unknown types
        logger.warning(f"Unknown Iceberg type: {iceberg_type}, defaulting to VARCHAR")
        return "VARCHAR"
    
    def _map_delta_type(self, delta_type: str) -> str:
        """Map Delta type to SQL type."""
        base_type = delta_type.split("(")[0].lower()
        
        sql_type = self.DELTA_TYPE_MAP.get(base_type)
        if sql_type:
            if "(" in delta_type:
                sql_type += delta_type[delta_type.index("("):]
            return sql_type
        
        logger.warning(f"Unknown Delta type: {delta_type}, defaulting to VARCHAR")
        return "VARCHAR"
    
    def _map_hudi_type(self, hudi_type: str) -> str:
        """Map Hudi type to SQL type."""
        # Hudi types can be Avro types
        if isinstance(hudi_type, list):
            # Union type, find non-null type
            non_null_types = [t for t in hudi_type if t != "null"]
            hudi_type = non_null_types[0] if non_null_types else "string"
        
        base_type = str(hudi_type).split("(")[0].lower()
        
        sql_type = self.HUDI_TYPE_MAP.get(base_type)
        if sql_type:
            return sql_type
        
        logger.warning(f"Unknown Hudi type: {hudi_type}, defaulting to VARCHAR")
        return "VARCHAR"
    
    def _extract_table_name_from_path(self, s3_path: str) -> str:
        """Extract table name from S3 path."""
        # Remove s3:// prefix and get last path component
        path = s3_path.replace("s3://", "")
        parts = path.rstrip("/").split("/")
        
        # Return last non-empty part
        for part in reversed(parts):
            if part:
                return part
        
        return "unknown_table"
