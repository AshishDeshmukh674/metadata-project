"""
Spark API Routes
================
FastAPI endpoints for Spark-based data processing.

These endpoints use Apache Spark for:
- Processing large files efficiently
- ACID transactions with Delta Lake
- Parallel processing
- Advanced transformations
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import logging

from app.models.user_credentials import UserAWSCredentials
from app.services.spark_parquet_service import spark_parquet_service
from app.services.spark_delta_service import spark_delta_service

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/api/spark", tags=["Spark Processing"])


# ============================================================
# REQUEST MODELS
# ============================================================

class SparkParquetQueryRequest(BaseModel):
    """Request model for querying Parquet files with Spark."""
    aws_access_key_id: str = Field(..., description="AWS Access Key ID")
    aws_secret_access_key: str = Field(..., description="AWS Secret Access Key")
    aws_session_token: Optional[str] = Field(None, description="AWS Session Token (optional)")
    aws_region: str = Field(default="us-east-1", description="AWS Region")
    bucket: str = Field(..., description="S3 bucket name")
    key: str = Field(..., description="S3 object key (path to Parquet file)")
    sql_query: Optional[str] = Field(None, description="Optional SQL query (use 'data_table' as table name)")
    filters: Optional[Dict[str, Any]] = Field(None, description="Optional filters")
    columns: Optional[List[str]] = Field(None, description="Optional columns to select")
    limit: Optional[int] = Field(None, description="Optional row limit")


class SparkParquetModifyRequest(BaseModel):
    """Request model for modifying Parquet files with Spark."""
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: Optional[str] = None
    aws_region: str = "us-east-1"
    bucket: str
    key: str = Field(..., description="Input Parquet file path")
    sql_modification: str = Field(..., description="SQL to modify data")
    output_key: Optional[str] = Field(None, description="Output path (overwrites input if not specified)")
    write_mode: str = Field(default="overwrite", description="Write mode: overwrite, append, error")
    partition_by: Optional[List[str]] = Field(None, description="Columns to partition by")


class SparkParquetMergeRequest(BaseModel):
    """Request model for merging multiple Parquet files."""
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: Optional[str] = None
    aws_region: str = "us-east-1"
    bucket: str
    keys: List[str] = Field(..., description="List of Parquet file paths to merge")
    output_key: str = Field(..., description="Output path for merged file")
    deduplicate_on: Optional[List[str]] = Field(None, description="Columns for deduplication")


class SparkDeltaReadRequest(BaseModel):
    """Request model for reading Delta tables with Spark."""
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: Optional[str] = None
    aws_region: str = "us-east-1"
    bucket: str
    table_path: str = Field(..., description="Path to Delta table")
    version: Optional[int] = Field(None, description="Version for time travel")
    timestamp: Optional[str] = Field(None, description="Timestamp for time travel")
    filters: Optional[Dict[str, Any]] = Field(None, description="Optional filters")


class SparkDeltaUpdateRequest(BaseModel):
    """Request model for updating Delta tables."""
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: Optional[str] = None
    aws_region: str = "us-east-1"
    bucket: str
    table_path: str
    update_expr: Dict[str, str] = Field(..., description="Column: expression map for updates")
    condition: Optional[str] = Field(None, description="SQL WHERE condition")


class SparkDeltaDeleteRequest(BaseModel):
    """Request model for deleting from Delta tables."""
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: Optional[str] = None
    aws_region: str = "us-east-1"
    bucket: str
    table_path: str
    condition: str = Field(..., description="SQL WHERE condition for deletion")


class SparkDeltaMergeRequest(BaseModel):
    """Request model for merging into Delta tables."""
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: Optional[str] = None
    aws_region: str = "us-east-1"
    bucket: str
    table_path: str
    source_data: List[Dict[str, Any]] = Field(..., description="Data to merge (upsert)")
    merge_key: str = Field(..., description="Column to match on")
    update_cols: Optional[List[str]] = Field(None, description="Columns to update (all if None)")


class SparkDeltaHistoryRequest(BaseModel):
    """Request model for getting Delta table history."""
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: Optional[str] = None
    aws_region: str = "us-east-1"
    bucket: str
    table_path: str
    limit: int = Field(default=20, description="Number of history entries")


# ============================================================
# PARQUET ENDPOINTS
# ============================================================

@router.post("/parquet/query", summary="Query Parquet file using Spark")
async def spark_query_parquet(request: SparkParquetQueryRequest):
    """
    Query Parquet file using Apache Spark.
    
    **Use this when:**
    - Files are large (> 100 MB)
    - Need parallel processing
    - Complex SQL queries
    
    **Features:**
    - Column pruning (reads only needed columns)
    - Predicate pushdown (filters at read time)
    - Parallel execution
    - Handles files larger than memory
    
    **Example SQL:**
    ```sql
    SELECT department, AVG(salary) as avg_salary
    FROM data_table
    WHERE age > 25
    GROUP BY department
    ORDER BY avg_salary DESC
    ```
    """
    try:
        logger.info(f"🚀 Spark Parquet query: s3://{request.bucket}/{request.key}")
        
        credentials = UserAWSCredentials(
            aws_access_key_id=request.aws_access_key_id,
            aws_secret_access_key=request.aws_secret_access_key,
            aws_session_token=request.aws_session_token,
            aws_region=request.aws_region
        )
        
        result = spark_parquet_service.read_and_query(
            bucket=request.bucket,
            key=request.key,
            credentials=credentials,
            sql_query=request.sql_query,
            filters=request.filters,
            columns=request.columns,
            limit=request.limit
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["message"])
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error in Spark Parquet query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/parquet/modify", summary="Modify Parquet file using Spark")
async def spark_modify_parquet(request: SparkParquetModifyRequest):
    """
    Modify Parquet file data and write back to S3.
    
    **Important:** Parquet files are immutable, so this creates a new file.
    
    **Use cases:**
    - Update values based on conditions
    - Delete rows
    - Add computed columns
    - Transform data structure
    
    **Example SQL:**
    ```sql
    -- Update
    UPDATE data_table SET salary = salary * 1.1 WHERE department = 'Engineering'
    
    -- Delete
    DELETE FROM data_table WHERE age < 18
    
    -- Transform
    SELECT *, salary * 0.7 as tax_amount FROM data_table
    ```
    """
    try:
        logger.info(f"🔧 Spark Parquet modify: s3://{request.bucket}/{request.key}")
        
        credentials = UserAWSCredentials(
            aws_access_key_id=request.aws_access_key_id,
            aws_secret_access_key=request.aws_secret_access_key,
            aws_session_token=request.aws_session_token,
            aws_region=request.aws_region
        )
        
        result = spark_parquet_service.modify_and_write_back(
            bucket=request.bucket,
            key=request.key,
            credentials=credentials,
            sql_modification=request.sql_modification,
            output_key=request.output_key,
            write_mode=request.write_mode,
            partition_by=request.partition_by
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["message"])
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error in Spark Parquet modify: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/parquet/merge", summary="Merge multiple Parquet files")
async def spark_merge_parquet(request: SparkParquetMergeRequest):
    """
    Merge multiple Parquet files into one.
    
    **Use cases:**
    - Combine daily files into monthly file
    - Merge sharded data
    - Consolidate multiple sources
    
    **Features:**
    - Parallel reading of all files
    - Optional deduplication
    - Efficient union operation
    """
    try:
        logger.info(f"🔀 Spark Parquet merge: {len(request.keys)} files")
        
        credentials = UserAWSCredentials(
            aws_access_key_id=request.aws_access_key_id,
            aws_secret_access_key=request.aws_secret_access_key,
            aws_session_token=request.aws_session_token,
            aws_region=request.aws_region
        )
        
        result = spark_parquet_service.merge_multiple_files(
            bucket=request.bucket,
            keys=request.keys,
            credentials=credentials,
            output_key=request.output_key,
            deduplicate_on=request.deduplicate_on
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["message"])
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error in Spark Parquet merge: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# DELTA LAKE ENDPOINTS
# ============================================================

@router.post("/delta/read", summary="Read Delta table using Spark")
async def spark_read_delta(request: SparkDeltaReadRequest):
    """
    Read Delta Lake table using Apache Spark.
    
    **Delta Lake Features:**
    - ACID transactions
    - Time travel (query historical versions)
    - Schema enforcement
    - Metadata-only operations
    
    **Time Travel Examples:**
    ```json
    {"version": 5}                    # Version 5 of the table
    {"timestamp": "2024-01-01"}       # Table as of Jan 1, 2024
    ```
    """
    try:
        logger.info(f"🔺 Spark Delta read: s3://{request.bucket}/{request.table_path}")
        
        credentials = UserAWSCredentials(
            aws_access_key_id=request.aws_access_key_id,
            aws_secret_access_key=request.aws_secret_access_key,
            aws_session_token=request.aws_session_token,
            aws_region=request.aws_region
        )
        
        result = spark_delta_service.read_delta_table(
            bucket=request.bucket,
            table_path=request.table_path,
            credentials=credentials,
            version=request.version,
            timestamp=request.timestamp,
            filters=request.filters
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["message"])
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error in Spark Delta read: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/delta/update", summary="Update Delta table (ACID)")
async def spark_update_delta(request: SparkDeltaUpdateRequest):
    """
    Update rows in Delta table with ACID guarantees.
    
    **This is TRUE update - not creating a new file!**
    
    **Features:**
    - Atomic operation
    - Transaction log maintained
    - Old versions kept for time travel
    - Only modified files rewritten
    
    **Example:**
    ```json
    {
        "update_expr": {"salary": "salary * 1.1", "updated_at": "current_timestamp()"},
        "condition": "department = 'Engineering' AND hire_date < '2020-01-01'"
    }
    ```
    """
    try:
        logger.info(f"✏️ Spark Delta update: s3://{request.bucket}/{request.table_path}")
        
        credentials = UserAWSCredentials(
            aws_access_key_id=request.aws_access_key_id,
            aws_secret_access_key=request.aws_secret_access_key,
            aws_session_token=request.aws_session_token,
            aws_region=request.aws_region
        )
        
        result = spark_delta_service.update_delta_table(
            bucket=request.bucket,
            table_path=request.table_path,
            credentials=credentials,
            update_expr=request.update_expr,
            condition=request.condition
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["message"])
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error in Spark Delta update: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/delta/delete", summary="Delete from Delta table (ACID)")
async def spark_delete_delta(request: SparkDeltaDeleteRequest):
    """
    Delete rows from Delta table with ACID guarantees.
    
    **Safe deletion:**
    - Atomic operation
    - Old data kept for time travel
    - Can be rolled back
    
    **Example:**
    ```json
    {
        "condition": "is_active = false AND last_login < '2023-01-01'"
    }
    ```
    """
    try:
        logger.info(f"🗑️ Spark Delta delete: s3://{request.bucket}/{request.table_path}")
        
        credentials = UserAWSCredentials(
            aws_access_key_id=request.aws_access_key_id,
            aws_secret_access_key=request.aws_secret_access_key,
            aws_session_token=request.aws_session_token,
            aws_region=request.aws_region
        )
        
        result = spark_delta_service.delete_from_delta_table(
            bucket=request.bucket,
            table_path=request.table_path,
            credentials=credentials,
            condition=request.condition
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["message"])
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error in Spark Delta delete: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/delta/merge", summary="Merge into Delta table (Upsert)")
async def spark_merge_delta(request: SparkDeltaMergeRequest):
    """
    Merge (Upsert) data into Delta table.
    
    **MERGE = Update if exists, Insert if not**
    
    **Perfect for:**
    - Daily data updates
    - Syncing data from multiple sources
    - Maintaining slowly changing dimensions
    
    **Example:**
    ```json
    {
        "source_data": [
            {"id": 1, "name": "John", "salary": 120000},
            {"id": 999, "name": "Jane", "salary": 90000}
        ],
        "merge_key": "id"
    }
    ```
    - ID 1 exists → Update name and salary
    - ID 999 is new → Insert new row
    """
    try:
        logger.info(f"🔀 Spark Delta merge: s3://{request.bucket}/{request.table_path}")
        
        credentials = UserAWSCredentials(
            aws_access_key_id=request.aws_access_key_id,
            aws_secret_access_key=request.aws_secret_access_key,
            aws_session_token=request.aws_session_token,
            aws_region=request.aws_region
        )
        
        result = spark_delta_service.merge_into_delta_table(
            bucket=request.bucket,
            table_path=request.table_path,
            credentials=credentials,
            source_data=request.source_data,
            merge_key=request.merge_key,
            update_cols=request.update_cols
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["message"])
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error in Spark Delta merge: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/delta/history", summary="Get Delta table history")
async def spark_delta_history(request: SparkDeltaHistoryRequest):
    """
    Get Delta table transaction history.
    
    **Audit log includes:**
    - All operations (UPDATE, DELETE, MERGE, etc.)
    - Timestamps
    - Metrics (rows affected)
    - User metadata
    
    **Use for:**
    - Auditing changes
    - Debugging issues
    - Understanding table evolution
    """
    try:
        logger.info(f"📜 Spark Delta history: s3://{request.bucket}/{request.table_path}")
        
        credentials = UserAWSCredentials(
            aws_access_key_id=request.aws_access_key_id,
            aws_secret_access_key=request.aws_secret_access_key,
            aws_session_token=request.aws_session_token,
            aws_region=request.aws_region
        )
        
        result = spark_delta_service.get_delta_history(
            bucket=request.bucket,
            table_path=request.table_path,
            credentials=credentials,
            limit=request.limit
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["message"])
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error in Spark Delta history: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# Health check endpoint
@router.get("/health", summary="Check Spark service health")
async def spark_health():
    """Check if Spark services are available."""
    try:
        from app.config.spark_config import SparkConfig
        
        info = SparkConfig.get_session_info()
        
        return {
            "status": "healthy" if info.get("active") else "inactive",
            "spark_info": info
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }
