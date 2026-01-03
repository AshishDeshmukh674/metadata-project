"""
S3 Data Models
==============
Request and response models for S3 operations.
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime


class S3PathRequest(BaseModel):
    """
    Request model for S3 path input.
    
    Example:
        {
            "s3_path": "s3://my-bucket/warehouse/db/table",
            "aws_region": "us-east-1"
        }
    """
    s3_path: str = Field(
        ...,  # Required field
        description="S3 URI (e.g., s3://bucket/path/to/table)",
        examples=["s3://my-bucket/data/iceberg_table"]
    )
    
    aws_region: Optional[str] = Field(
        None,
        description="AWS region (optional, uses default if not provided)"
    )
    
    @field_validator('s3_path')
    @classmethod
    def validate_s3_path(cls, v: str) -> str:
        """Validate that the path starts with s3://"""
        if not v.startswith('s3://'):
            raise ValueError('S3 path must start with s3://')
        if len(v) < 8:  # s3://x/x minimum
            raise ValueError('S3 path is too short')
        return v


class S3ConnectionResponse(BaseModel):
    """
    Response model for S3 connection test.
    """
    success: bool = Field(description="Whether connection was successful")
    message: str = Field(description="Status message")
    region: str = Field(description="AWS region used")
    timestamp: datetime = Field(
        default_factory=datetime.now,
        description="When the test was performed"
    )


class S3ObjectInfo(BaseModel):
    """
    Information about an S3 object (file).
    """
    key: str = Field(description="S3 object key (path)")
    size: int = Field(description="File size in bytes")
    last_modified: datetime = Field(description="Last modification timestamp")
    storage_class: Optional[str] = Field(None, description="S3 storage class")
    
    def size_mb(self) -> float:
        """Return size in megabytes."""
        return round(self.size / (1024 * 1024), 2)


class S3ListResponse(BaseModel):
    """
    Response model for listing S3 objects.
    """
    success: bool
    bucket: str
    prefix: str
    object_count: int
    total_size_bytes: int
    objects: list[S3ObjectInfo]
    
    def total_size_mb(self) -> float:
        """Return total size in megabytes."""
        return round(self.total_size_bytes / (1024 * 1024), 2)
