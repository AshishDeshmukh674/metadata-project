"""
API routes for the Unified Data Access Platform.
"""

from fastapi import APIRouter, HTTPException, Query, Path
from typing import Optional, List

from .models import (
    DiscoverTableRequest,
    DiscoverTableResponse,
    ListTablesResponse,
    GetTableResponse,
    DeleteTableResponse,
    TableMetadataResponse,
    ColumnResponse,
)
from ..main import MetadataDiscoveryEngine
from ..utils.exceptions import (
    PlatformException,
    FormatDetectionError,
    MetadataReadError,
    NormalizationError,
    StorageError,
)
from ..utils.logger import setup_logger

logger = setup_logger(__name__)

# Create router
router = APIRouter(prefix="/api/v1", tags=["metadata"])

# Global engine instance (can be dependency injected in production)
_engine: Optional[MetadataDiscoveryEngine] = None


def get_engine() -> MetadataDiscoveryEngine:
    """Get or create the metadata discovery engine."""
    global _engine
    if _engine is None:
        _engine = MetadataDiscoveryEngine(db_path="metadata.db")
    return _engine


@router.post("/discover", response_model=DiscoverTableResponse, summary="Discover table metadata")
async def discover_table(request: DiscoverTableRequest):
    """
    Discover and store table metadata from S3.
    
    This endpoint:
    1. Detects the table format (Iceberg, Delta Lake, or Hudi)
    2. Reads format-specific metadata from S3
    3. Normalizes to unified schema
    4. Stores in the metadata database
    
    **Parameters:**
    - **s3_path**: S3 path to the table (e.g., s3://bucket/warehouse/table_name)
    
    **Returns:**
    - Discovered and normalized table metadata
    """
    try:
        logger.info(f"API: Discovering table at {request.s3_path}")
        
        engine = get_engine()
        metadata = engine.discover_and_store(request.s3_path)
        
        # Convert to response model
        columns_response = [
            ColumnResponse(
                name=col.name,
                data_type=col.data_type,
                nullable=col.nullable,
                comment=col.comment
            )
            for col in metadata.columns
        ]
        
        table_response = TableMetadataResponse(
            table_name=metadata.table_name,
            format=metadata.format,
            location=metadata.location,
            columns=columns_response,
            partitions=metadata.partitions,
            properties=metadata.properties,
            supports_time_travel=metadata.supports_time_travel,
            created_at=metadata.created_at,
            updated_at=metadata.updated_at,
            num_files=metadata.num_files,
            size_bytes=metadata.size_bytes,
            row_count=metadata.row_count
        )
        
        return DiscoverTableResponse(
            success=True,
            message=f"Successfully discovered table: {metadata.table_name}",
            table_metadata=table_response
        )
        
    except FormatDetectionError as e:
        logger.error(f"Format detection failed: {e.message}")
        raise HTTPException(status_code=400, detail={
            "error": "Format detection failed",
            "message": e.message,
            "details": e.details
        })
    except MetadataReadError as e:
        logger.error(f"Metadata read failed: {e.message}")
        raise HTTPException(status_code=500, detail={
            "error": "Metadata read failed",
            "message": e.message,
            "details": e.details
        })
    except NormalizationError as e:
        logger.error(f"Normalization failed: {e.message}")
        raise HTTPException(status_code=500, detail={
            "error": "Normalization failed",
            "message": e.message,
            "details": e.details
        })
    except StorageError as e:
        logger.error(f"Storage failed: {e.message}")
        raise HTTPException(status_code=500, detail={
            "error": "Storage failed",
            "message": e.message,
            "details": e.details
        })
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail={
            "error": "Internal server error",
            "message": str(e)
        })


@router.get("/tables", response_model=ListTablesResponse, summary="List all tables")
async def list_tables(
    format: Optional[str] = Query(None, description="Filter by format (ICEBERG, DELTA, HUDI)")
):
    """
    List all tables in the metadata store.
    
    **Parameters:**
    - **format** (optional): Filter tables by format (ICEBERG, DELTA, or HUDI)
    
    **Returns:**
    - List of table names
    """
    try:
        logger.info(f"API: Listing tables (format={format})")
        
        engine = get_engine()
        tables = engine.list_tables(format_filter=format)
        
        return ListTablesResponse(
            success=True,
            count=len(tables),
            tables=tables
        )
        
    except Exception as e:
        logger.error(f"Failed to list tables: {str(e)}")
        raise HTTPException(status_code=500, detail={
            "error": "Failed to list tables",
            "message": str(e)
        })


@router.get("/tables/{table_name}", response_model=GetTableResponse, summary="Get table metadata")
async def get_table(
    table_name: str = Path(..., description="Name of the table")
):
    """
    Get detailed metadata for a specific table.
    
    **Parameters:**
    - **table_name**: Name of the table to retrieve
    
    **Returns:**
    - Complete table metadata including columns, partitions, and properties
    """
    try:
        logger.info(f"API: Getting table metadata for {table_name}")
        
        engine = get_engine()
        metadata = engine.get_table_metadata(table_name)
        
        if metadata is None:
            raise HTTPException(status_code=404, detail={
                "error": "Table not found",
                "message": f"Table '{table_name}' does not exist in the metadata store"
            })
        
        # Convert to response model
        columns_response = [
            ColumnResponse(
                name=col.name,
                data_type=col.data_type,
                nullable=col.nullable,
                comment=col.comment
            )
            for col in metadata.columns
        ]
        
        table_response = TableMetadataResponse(
            table_name=metadata.table_name,
            format=metadata.format,
            location=metadata.location,
            columns=columns_response,
            partitions=metadata.partitions,
            properties=metadata.properties,
            supports_time_travel=metadata.supports_time_travel,
            created_at=metadata.created_at,
            updated_at=metadata.updated_at,
            num_files=metadata.num_files,
            size_bytes=metadata.size_bytes,
            row_count=metadata.row_count
        )
        
        return GetTableResponse(
            success=True,
            table_metadata=table_response
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get table metadata: {str(e)}")
        raise HTTPException(status_code=500, detail={
            "error": "Failed to retrieve table metadata",
            "message": str(e)
        })


@router.delete("/tables/{table_name}", response_model=DeleteTableResponse, summary="Delete table metadata")
async def delete_table(
    table_name: str = Path(..., description="Name of the table to delete")
):
    """
    Delete table metadata from the store.
    
    **Parameters:**
    - **table_name**: Name of the table to delete
    
    **Returns:**
    - Success confirmation
    """
    try:
        logger.info(f"API: Deleting table metadata for {table_name}")
        
        engine = get_engine()
        deleted = engine.delete_table(table_name)
        
        if not deleted:
            raise HTTPException(status_code=404, detail={
                "error": "Table not found",
                "message": f"Table '{table_name}' does not exist in the metadata store"
            })
        
        return DeleteTableResponse(
            success=True,
            message=f"Successfully deleted table: {table_name}"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete table metadata: {str(e)}")
        raise HTTPException(status_code=500, detail={
            "error": "Failed to delete table metadata",
            "message": str(e)
        })


@router.get("/tables/{table_name}/columns", response_model=List[ColumnResponse], summary="Get table columns")
async def get_table_columns(
    table_name: str = Path(..., description="Name of the table")
):
    """
    Get column definitions for a specific table.
    
    **Parameters:**
    - **table_name**: Name of the table
    
    **Returns:**
    - List of column definitions
    """
    try:
        logger.info(f"API: Getting columns for table {table_name}")
        
        engine = get_engine()
        metadata = engine.get_table_metadata(table_name)
        
        if metadata is None:
            raise HTTPException(status_code=404, detail={
                "error": "Table not found",
                "message": f"Table '{table_name}' does not exist in the metadata store"
            })
        
        columns_response = [
            ColumnResponse(
                name=col.name,
                data_type=col.data_type,
                nullable=col.nullable,
                comment=col.comment
            )
            for col in metadata.columns
        ]
        
        return columns_response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get table columns: {str(e)}")
        raise HTTPException(status_code=500, detail={
            "error": "Failed to retrieve table columns",
            "message": str(e)
        })
