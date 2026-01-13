"""
Internal metadata models for unified table representation.

These models are format-agnostic and serve as the normalized schema
for all table formats (Iceberg, Delta, Hudi).
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from datetime import datetime


@dataclass
class ColumnMetadata:
    """Represents a single column in a table."""
    
    name: str
    data_type: str
    nullable: bool = True
    comment: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            "name": self.name,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "comment": self.comment
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "ColumnMetadata":
        """Create ColumnMetadata from dictionary."""
        return cls(
            name=data["name"],
            data_type=data["data_type"],
            nullable=data.get("nullable", True),
            comment=data.get("comment")
        )


@dataclass
class TableMetadata:
    """
    Unified metadata model for lakehouse tables.
    
    This is the core internal representation used across the platform.
    All format-specific metadata is normalized into this schema.
    """
    
    table_name: str
    format: str  # "ICEBERG", "DELTA", "HUDI"
    location: str  # S3 path
    columns: List[ColumnMetadata] = field(default_factory=list)
    partitions: List[str] = field(default_factory=list)  # Partition column names
    properties: Dict[str, str] = field(default_factory=dict)  # Key-value properties
    supports_time_travel: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # Additional metadata
    num_files: Optional[int] = None
    size_bytes: Optional[int] = None
    row_count: Optional[int] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            "table_name": self.table_name,
            "format": self.format,
            "location": self.location,
            "columns": [col.to_dict() for col in self.columns],
            "partitions": self.partitions,
            "properties": self.properties,
            "supports_time_travel": self.supports_time_travel,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "num_files": self.num_files,
            "size_bytes": self.size_bytes,
            "row_count": self.row_count
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "TableMetadata":
        """Create TableMetadata from dictionary."""
        return cls(
            table_name=data["table_name"],
            format=data["format"],
            location=data["location"],
            columns=[ColumnMetadata.from_dict(col) for col in data.get("columns", [])],
            partitions=data.get("partitions", []),
            properties=data.get("properties", {}),
            supports_time_travel=data.get("supports_time_travel", False),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else None,
            updated_at=datetime.fromisoformat(data["updated_at"]) if data.get("updated_at") else None,
            num_files=data.get("num_files"),
            size_bytes=data.get("size_bytes"),
            row_count=data.get("row_count")
        )
    
    def get_column_names(self) -> List[str]:
        """Get list of column names."""
        return [col.name for col in self.columns]
    
    def get_column_by_name(self, name: str) -> Optional[ColumnMetadata]:
        """Get column metadata by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None
    
    def is_partitioned(self) -> bool:
        """Check if table is partitioned."""
        return len(self.partitions) > 0
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"TableMetadata(table_name='{self.table_name}', "
            f"format='{self.format}', "
            f"columns={len(self.columns)}, "
            f"partitions={len(self.partitions)})"
        )
