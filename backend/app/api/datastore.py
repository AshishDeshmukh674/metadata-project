"""
Datastore APIs
==============
APIs for accessing and managing datastore metadata and data.
All endpoints require valid session (created after credential validation).
"""

from fastapi import APIRouter, HTTPException, Header
from typing import Optional, Dict, Any, List
from datetime import datetime
import logging
import pyarrow.parquet as pq
import boto3
from io import BytesIO

from app.models.user_credentials import SessionCredentials
from app.models.datastore import (
    DataModificationRequest,
    SQLPreviewResponse,
    SQLExecutionRequest,
    SQLExecutionResponse
)
from app.services.user_s3_service import user_s3_service
from app.services.groq_service import groq_service
from app.services.duckdb_service import duckdb_service
from app.config import settings
from app.api.auth import get_session_store

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/api/v1/datastore",
    tags=["Datastore Operations"]
)


def get_session(session_id: str) -> SessionCredentials:
    """
    Validate session and return credentials.
    
    Args:
        session_id: Session ID from X-Session-ID header
        
    Returns:
        Session credentials if valid
        
    Raises:
        HTTPException: If session invalid or expired
    """
    _session_store = get_session_store()
    
    if not session_id:
        raise HTTPException(
            status_code=401,
            detail="Missing X-Session-ID header"
        )
    
    session = _session_store.get(session_id)
    if not session:
        raise HTTPException(
            status_code=401,
            detail="Invalid session ID"
        )
    _session_store = get_session_store()
        
    if session.is_expired():
        del _session_store[session_id]
        raise HTTPException(
            status_code=401,
            detail="Session expired. Please create a new session."
        )
    
    return session


@router.get("/info")
async def get_datastore_info(
    s3_path: str,
    session_id: str = Header(None, alias="X-Session-ID")
) -> Dict[str, Any]:
    """
    Get information about a datastore at the given S3 path.
    
    This endpoint:
    - Lists all files in the S3 path
    - Detects table format (Parquet, Iceberg, Delta, Hudi)
    - Returns basic statistics
    
    **Requires**: Valid session (X-Session-ID header)
    
    Args:
        s3_path: S3 path to datastore (e.g., s3://bucket/path/to/table/)
        session_id: Session ID from create-session endpoint
        
    Returns:
        Datastore information including format and file list
        
    Example:
        GET /api/v1/datastore/info?s3_path=s3://my-bucket/data/users/
        Headers: X-Session-ID: your-session-id
    """
    try:
        # Validate session
        session = get_session(session_id)
        
        logger.info(f"📂 Getting datastore info for: {s3_path}")
        
        # Parse S3 path
        bucket, prefix = user_s3_service.parse_s3_path(s3_path)
        
        # List objects
        objects = user_s3_service.list_objects(
            credentials=session.credentials,
            s3_path=s3_path,
            max_keys=1000
        )
        
        # Detect table format
        table_format = detect_table_format(objects)
        
        # Calculate statistics
        total_size = sum(obj.size for obj in objects)
        
        # Group by file extension
        file_types = {}
        for obj in objects:
            ext = obj.key.split('.')[-1] if '.' in obj.key else 'unknown'
            file_types[ext] = file_types.get(ext, 0) + 1
        
        return {
            "success": True,
            "s3_path": s3_path,
            "bucket": bucket,
            "prefix": prefix,
            "table_format": table_format,
            "file_count": len(objects),
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "file_types": file_types,
            "last_modified": max([obj.last_modified for obj in objects]).isoformat() if objects else None
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting datastore info: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get datastore info: {str(e)}"
        )


@router.get("/metadata")
async def get_metadata(
    s3_path: str,
    session_id: str = Header(None, alias="X-Session-ID")
) -> Dict[str, Any]:
    """
    Get detailed metadata including schema, columns, and data types.
    
    This endpoint:
    - Reads table schema
    - Returns column names and types
    - Provides format-specific metadata (partitions, statistics, etc.)
    
    **Requires**: Valid session (X-Session-ID header)
    
    Args:
        s3_path: S3 path to datastore
        session_id: Session ID from create-session endpoint
        
    Returns:
        Detailed metadata including schema and columns
        
    Example:
        GET /api/v1/datastore/metadata?s3_path=s3://my-bucket/data/users/
        Headers: X-Session-ID: your-session-id
    """
    try:
        # Validate session
        session = get_session(session_id)
        
        logger.info(f"📋 Getting metadata for: {s3_path}")
        
        # Parse S3 path
        bucket, prefix = user_s3_service.parse_s3_path(s3_path)
        
        # List objects to detect format
        objects = user_s3_service.list_objects(
            credentials=session.credentials,
            s3_path=s3_path,
            max_keys=100
        )
        
        # Detect format
        table_format = detect_table_format(objects)
        
        if table_format == "unknown":
            raise HTTPException(
                status_code=400,
                detail="Unable to detect table format. Supported formats: Parquet, Iceberg, Delta, Hudi"
            )
        
        # Get schema based on format
        if table_format == "parquet":
            schema_info = read_parquet_schema(
                bucket=bucket,
                prefix=prefix,
                objects=objects,
                credentials=session.credentials
            )
        else:
            # TODO: Implement Iceberg, Delta, Hudi readers
            schema_info = {
                "message": f"{table_format} metadata reader not yet implemented",
                "columns": [],
                "partition_columns": []
            }
        
        return {
            "success": True,
            "s3_path": s3_path,
            "table_format": table_format,
            "schema": schema_info
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting metadata: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get metadata: {str(e)}"
        )


def detect_table_format(objects: List) -> str:
    """
    Detect table format from file list.
    
    Args:
        objects: List of S3 objects
        
    Returns:
        Table format: 'iceberg', 'delta', 'hudi', 'parquet', or 'unknown'
    """
    file_keys = [obj.key.lower() for obj in objects]
    
    # Check for Iceberg
    if any('metadata' in key and key.endswith('.json') for key in file_keys):
        if any('metadata.json' in key or 'version-hint.text' in key for key in file_keys):
            return "iceberg"
    
    # Check for Delta Lake
    if any('_delta_log' in key for key in file_keys):
        return "delta"
    
    # Check for Hudi
    if any('.hoodie' in key for key in file_keys):
        return "hudi"
    
    # Check for plain Parquet
    if any(key.endswith('.parquet') for key in file_keys):
        return "parquet"
    
    return "unknown"


def read_parquet_schema(bucket: str, prefix: str, objects: List, credentials) -> Dict[str, Any]:
    """
    Read schema from Parquet files.
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix/path
        objects: List of S3 objects
        credentials: User AWS credentials
        
    Returns:
        Schema information with columns and statistics
    """
    try:
        # Find first parquet file
        parquet_files = [obj for obj in objects if obj.key.endswith('.parquet')]
        
        if not parquet_files:
            return {
                "error": "No Parquet files found",
                "columns": [],
                "partition_columns": []
            }
        
        # Use first parquet file to read schema
        first_file = parquet_files[0]
        
        logger.info(f"📖 Reading Parquet schema from: s3://{bucket}/{first_file.key}")
        
        # Create S3 client with user credentials
        s3_client = boto3.client(
            's3',
            aws_access_key_id=credentials.aws_access_key_id,
            aws_secret_access_key=credentials.aws_secret_access_key,
            aws_session_token=credentials.aws_session_token,
            region_name=credentials.aws_region
        )
        
        # Download the Parquet file
        response = s3_client.get_object(Bucket=bucket, Key=first_file.key)
        parquet_data = response['Body'].read()
        
        # Read Parquet metadata from bytes
        parquet_file = pq.ParquetFile(BytesIO(parquet_data))
        schema = parquet_file.schema_arrow
        
        # Extract column information
        columns = []
        for i in range(len(schema)):
            field = schema.field(i)
            column_info = {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable
            }
            columns.append(column_info)
        
        # Get statistics
        metadata = parquet_file.metadata
        num_rows = metadata.num_rows
        num_row_groups = metadata.num_row_groups
        
        # Check for partition columns (common naming patterns)
        partition_columns = []
        all_keys = [obj.key for obj in objects]
        
        # Detect Hive-style partitions (e.g., year=2024/month=01/)
        for key in all_keys:
            parts = key.split('/')
            for part in parts:
                if '=' in part and part not in partition_columns:
                    partition_col = part.split('=')[0]
                    if partition_col not in partition_columns:
                        partition_columns.append(partition_col)
        
        logger.info(f"✅ Schema read: {len(columns)} columns, {num_rows:,} rows")
        
        return {
            "columns": columns,
            "partition_columns": partition_columns,
            "statistics": {
                "num_rows": num_rows,
                "num_row_groups": num_row_groups,
                "num_columns": len(columns),
                "num_files": len(parquet_files)
            }
        }
        
    except Exception as e:
        logger.error(f"Error reading Parquet schema: {str(e)}")
        return {
            "error": f"Failed to read Parquet schema: {str(e)}",
            "columns": [],
            "partition_columns": []
        }


@router.post("/preview-changes", response_model=SQLPreviewResponse)
async def preview_changes(
    request: DataModificationRequest,
    x_session_id: str = Header(..., alias="X-Session-ID")
) -> SQLPreviewResponse:
    """
    **Preview Data Modifications**
    
    Convert natural language instruction to SQL and show what would change.
    This is a safe, read-only operation that doesn't modify any data.
    
    Flow:
    1. Convert natural language to SQL using Groq LLM
    2. Validate SQL for safety
    3. Preview affected rows
    4. Return SQL + explanation + safety check
    
    Example Request:
    ```json
    {
        "s3_path": "s3://metadataproject/test-data/test_users.parquet",
        "instruction": "delete users older than 30"
    }
    ```
    
    Returns:
        Preview with generated SQL, explanation, and sample affected rows
    """
    try:
        # Validate session
        session = get_session(x_session_id)
        credentials = session.credentials
        
        logger.info(f"🔍 Preview request: '{request.instruction}' on {request.s3_path}")
        
        # Check if Groq is configured
        if not groq_service.is_available():
            raise HTTPException(
                status_code=503,
                detail="LLM service not configured. Set GROQ_API_KEY in .env file"
            )
        
        # Parse S3 path and list objects
        logger.info("📋 Listing S3 objects...")
        objects = user_s3_service.list_objects(
            credentials=credentials,
            s3_path=request.s3_path
        )
        
        if not objects:
            raise HTTPException(
                status_code=404,
                detail=f"No objects found at {request.s3_path}"
            )
        
        # Parse bucket and prefix for schema reading
        bucket, prefix = user_s3_service.parse_s3_path(request.s3_path)
        
        # Get table schema for context
        logger.info("📋 Reading table schema...")
        schema = read_parquet_schema(bucket, prefix, objects, credentials)
        
        if "error" in schema:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to read table schema: {schema['error']}"
            )
        
        # Convert natural language to SQL
        logger.info("🤖 Generating SQL from instruction...")
        sql_result = await groq_service.natural_language_to_sql(
            instruction=request.instruction,
            schema=schema,
            table_format="parquet"
        )
        
        # Validate SQL safety
        logger.info("🔒 Validating SQL safety...")
        safety_check = await groq_service.validate_sql_safety(sql_result["sql"])
        
        # Preview affected rows (only for modifying operations)
        preview_data = None
        if sql_result["operation_type"] != "SELECT":
            logger.info("👀 Previewing affected rows...")
            preview_result = duckdb_service.preview_changes(
                s3_path=request.s3_path,
                sql_query=sql_result["sql"],
                credentials=credentials,
                sample_size=10
            )
            
            if preview_result["success"]:
                # Convert DataFrame to dict for JSON response
                preview_data = {
                    "sample_rows": preview_result["sample_rows"].to_dict(orient="records"),
                    "total_rows": preview_result["total_rows"],
                    "preview_size": preview_result["preview_size"]
                }
        
        logger.info(f"✅ Preview complete: {sql_result['operation_type']} operation")
        
        return SQLPreviewResponse(
            success=True,
            sql=sql_result["sql"],
            explanation=sql_result["explanation"],
            operation_type=sql_result["operation_type"],
            affected_columns=sql_result["affected_columns"],
            requires_backup=sql_result["requires_backup"],
            estimated_impact=sql_result["estimated_impact"],
            safety_check=safety_check,
            preview_data=preview_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Preview failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Preview failed: {str(e)}"
        )


@router.post("/execute-changes", response_model=SQLExecutionResponse)
async def execute_changes(
    request: SQLExecutionRequest,
    x_session_id: str = Header(..., alias="X-Session-ID")
) -> SQLExecutionResponse:
    """
    **Execute Data Modifications**
    
    Execute data modification using natural language instruction.
    
    ⚠️  This modifies data! Use /preview-changes first to see what will happen.
    
    Flow:
    1. Convert natural language to SQL using Groq LLM
    2. Validate SQL for safety
    3. Create backup (optional but recommended)
    4. Execute SQL on Parquet file
    5. Write modified data back to S3
    6. Return results
    
    Example Request:
    ```json
    {
        "s3_path": "s3://metadataproject/test-data/",
        "instruction": "delete rows where age > 30",
        "create_backup": true
    }
    ```
    
    Security:
        - Requires valid session
        - Backup created by default
        - SQL safety checks before execution
    
    Returns:
        Execution results with rows affected and backup info
    """
    try:
        # Validate session
        session = get_session(x_session_id)
        credentials = session.credentials
        
        logger.info(f"⚡ Execute request: '{request.instruction}' on {request.s3_path}")
        
        # Check if Groq is configured
        if not groq_service.is_available():
            raise HTTPException(
                status_code=503,
                detail="LLM service not configured. Set GROQ_API_KEY in .env file"
            )
        
        # Parse S3 path and list objects
        logger.info("📋 Listing S3 objects...")
        objects = user_s3_service.list_objects(
            credentials=credentials,
            s3_path=request.s3_path
        )
        
        if not objects:
            raise HTTPException(
                status_code=404,
                detail=f"No objects found at {request.s3_path}"
            )
        
        # Parse bucket and prefix for schema reading
        bucket, prefix = user_s3_service.parse_s3_path(request.s3_path)
        
        # Get table schema for context
        logger.info("📋 Reading table schema...")
        schema = read_parquet_schema(bucket, prefix, objects, credentials)
        
        if "error" in schema:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to read table schema: {schema['error']}"
            )
        
        # Convert natural language to SQL
        logger.info("🤖 Generating SQL from instruction...")
        sql_result = await groq_service.natural_language_to_sql(
            instruction=request.instruction,
            schema=schema,
            table_format="parquet"
        )
        
        # Validate SQL safety
        logger.info("🔒 Validating SQL safety...")
        safety_check = await groq_service.validate_sql_safety(sql_result["sql"])
        
        if not safety_check["is_safe"]:
            raise HTTPException(
                status_code=400,
                detail=f"SQL failed safety check: {', '.join(safety_check['warnings'])}"
            )
        
        operation_type = sql_result["operation_type"]
        sql_query = sql_result["sql"]
        
        logger.info(f"📝 Generated SQL ({operation_type}): {sql_query[:100]}...")
        
        # Find first Parquet file for execution
        parquet_files = [obj for obj in objects if obj.key.endswith('.parquet')]
        if not parquet_files:
            raise HTTPException(
                status_code=404,
                detail="No Parquet files found in the specified path"
            )
        
        # Use the first parquet file path for DuckDB
        first_parquet_path = f"s3://{bucket}/{parquet_files[0].key}"
        logger.info(f"📊 Using Parquet file: {first_parquet_path}")
        
        backup_path = None
        
        # Create backup if requested
        if request.create_backup and operation_type != "SELECT":
            logger.info("💾 Creating backup...")
            backup_result = duckdb_service.create_backup(
                s3_path=first_parquet_path,
                credentials=credentials
            )
            
            if backup_result["success"]:
                backup_path = backup_result["backup_path"]
                logger.info(f"✅ Backup created: {backup_path}")
            else:
                logger.warning(f"⚠️  Backup failed: {backup_result['message']}")
                # Continue anyway but warn user
        
        # Execute SQL
        logger.info("⚡ Executing SQL...")
        execution_result = duckdb_service.execute_sql_on_s3_parquet(
            s3_path=first_parquet_path,
            sql_query=sql_query,
            credentials=credentials,
            operation_type=operation_type
        )
        
        if not execution_result["success"]:
            raise HTTPException(
                status_code=500,
                detail=execution_result["message"]
            )
        
        # Write back to S3 if data was modified
        if operation_type != "SELECT" and execution_result["result_data"] is not None:
            logger.info("📤 Writing modified data back to S3...")
            write_result = duckdb_service.write_dataframe_to_s3(
                df=execution_result["result_data"],
                s3_path=first_parquet_path,
                credentials=credentials
            )
            
            if not write_result["success"]:
                raise HTTPException(
                    status_code=500,
                    detail=f"SQL executed but failed to write back: {write_result['message']}"
                )
            
            message = f"{execution_result['message']} Data written back to S3. Generated SQL: {sql_query}"
        else:
            message = f"{execution_result['message']} Generated SQL: {sql_query}"
        
        # For SELECT queries, return data
        result_data = None
        if operation_type == "SELECT" and execution_result["result_data"] is not None:
            result_data = execution_result["result_data"].to_dict(orient="records")
        
        logger.info(f"✅ Execution complete: {execution_result['rows_affected']} rows affected")
        
        return SQLExecutionResponse(
            success=True,
            message=message,
            sql=sql_query,
            operation_type=operation_type,
            rows_affected=execution_result["rows_affected"],
            execution_time_ms=execution_result["execution_time_ms"],
            backup_path=backup_path,
            result_data=result_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Execution failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Execution failed: {str(e)}"
        )

