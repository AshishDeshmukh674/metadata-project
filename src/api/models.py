"""
Pydantic models for API requests and responses.
"""

from typing import List, Optional, Dict
from pydantic import BaseModel, Field
from datetime import datetime


class ColumnResponse(BaseModel):
    """Column metadata response model."""
    name: str
    data_type: str
    nullable: bool
    comment: Optional[str] = None
    
    class Config:
        from_attributes = True


class TableMetadataResponse(BaseModel):
    """Table metadata response model."""
    table_name: str
    format: str
    location: str
    columns: List[ColumnResponse]
    partitions: List[str]
    properties: Dict[str, str]
    supports_time_travel: bool
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    num_files: Optional[int] = None
    size_bytes: Optional[int] = None
    row_count: Optional[int] = None
    
    class Config:
        from_attributes = True


class DiscoverTableRequest(BaseModel):
    """Request model for discovering table metadata."""
    s3_path: str = Field(..., description="S3 path to the table (e.g., s3://bucket/warehouse/table)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "s3_path": "s3://my-bucket/warehouse/sales_data"
            }
        }


class DiscoverTableResponse(BaseModel):
    """Response model for discover operation."""
    success: bool
    message: str
    table_metadata: Optional[TableMetadataResponse] = None


class ListTablesResponse(BaseModel):
    """Response model for listing tables."""
    success: bool
    count: int
    tables: List[str]


class GetTableResponse(BaseModel):
    """Response model for getting table metadata."""
    success: bool
    table_metadata: Optional[TableMetadataResponse] = None
    message: Optional[str] = None


class DeleteTableResponse(BaseModel):
    """Response model for deleting table metadata."""
    success: bool
    message: str


class ErrorResponse(BaseModel):
    """Error response model."""
    success: bool = False
    error: str
    details: Optional[Dict] = None


class HealthResponse(BaseModel):
    """Health check response model."""
    status: str
    version: str
    timestamp: datetime
    database_connected: bool
    tables_count: int
