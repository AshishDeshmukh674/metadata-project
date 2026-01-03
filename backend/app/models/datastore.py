"""
Datastore Models
================
Pydantic models for datastore operations.
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Any, Dict


class DataModificationRequest(BaseModel):
    """Request to modify data using natural language."""
    s3_path: str = Field(
        ...,
        description="S3 path to Parquet file",
        examples=["s3://bucket/data/table.parquet"]
    )
    instruction: str = Field(
        ...,
        description="Natural language instruction for what to do",
        examples=[
            "delete rows where age > 30",
            "update state from California to CA",
            "show me users from New York"
        ]
    )


class SQLPreviewResponse(BaseModel):
    """Response from preview-changes endpoint."""
    success: bool = Field(description="Whether preview was successful")
    sql: str = Field(description="Generated SQL query")
    explanation: str = Field(description="Plain English explanation")
    operation_type: str = Field(description="Type of operation (SELECT, UPDATE, DELETE, INSERT)")
    affected_columns: List[str] = Field(description="Columns affected by the operation")
    requires_backup: bool = Field(description="Whether backup is recommended")
    estimated_impact: str = Field(description="Description of impact")
    safety_check: Dict[str, Any] = Field(description="Safety validation results")
    preview_data: Optional[Dict[str, Any]] = Field(
        None,
        description="Sample of data that would be affected"
    )


class SQLExecutionRequest(BaseModel):
    """Request to execute data modification using natural language."""
    s3_path: str = Field(
        ...,
        description="S3 path to Parquet file",
        examples=["s3://metadataproject/test-data/"]
    )
    instruction: str = Field(
        ...,
        description="Natural language instruction (same as preview)",
        examples=[
            "delete rows where age > 30",
            "update state from California to CA",
            "show me users from New York"
        ]
    )
    create_backup: bool = Field(
        default=True,
        description="Whether to create backup before modification"
    )


class SQLExecutionResponse(BaseModel):
    """Response from execute-changes endpoint."""
    success: bool = Field(description="Whether execution was successful")
    message: str = Field(description="Result message")
    sql: str = Field(description="SQL query that was executed")
    operation_type: str = Field(description="Type of operation (SELECT, UPDATE, DELETE, INSERT)")
    rows_affected: int = Field(description="Number of rows affected")
    execution_time_ms: float = Field(description="Execution time in milliseconds")
    backup_path: Optional[str] = Field(
        None,
        description="Path to backup file (if created)"
    )
    result_data: Optional[Any] = Field(
        None,
        description="Result data for SELECT queries"
    )
