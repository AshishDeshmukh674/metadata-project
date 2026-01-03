"""
Parquet API
===========
Endpoints for Parquet file operations using natural language.
"""

from fastapi import APIRouter, HTTPException, Header
from typing import Dict, Any
import logging

from app.models.user_credentials import SessionCredentials
from app.models.datastore import (
    DataModificationRequest,
    SQLPreviewResponse,
    SQLExecutionRequest,
    SQLExecutionResponse
)
from app.services.user_s3_service import user_s3_service
from app.services.groq_service import groq_service
from app.services.parquet_service import parquet_service
from app.api.auth import get_session_store
from app.api.datastore import get_session, read_parquet_schema

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/api/v1/parquet",
    tags=["Parquet Operations"]
)


@router.post("/preview-changes", response_model=SQLPreviewResponse)
async def preview_parquet_changes(
    request: DataModificationRequest,
    x_session_id: str = Header(..., alias="X-Session-ID")
) -> SQLPreviewResponse:
    """
    **Preview Parquet Data Modifications**
    
    Convert natural language instruction to SQL for Parquet files.
    
    Example:
    ```json
    {
        "s3_path": "s3://metadataproject/test-data/",
        "instruction": "show users where age > 30"
    }
    ```
    """
    try:
        session = get_session(x_session_id)
        credentials = session.credentials
        
        logger.info(f"🔍 [PARQUET] Preview: '{request.instruction}' on {request.s3_path}")
        
        if not groq_service.is_available():
            raise HTTPException(status_code=503, detail="LLM service not configured")
        
        # List objects
        objects = user_s3_service.list_objects(credentials=credentials, s3_path=request.s3_path)
        if not objects:
            raise HTTPException(status_code=404, detail="No objects found")
        
        bucket, prefix = user_s3_service.parse_s3_path(request.s3_path)
        schema = read_parquet_schema(bucket, prefix, objects, credentials)
        
        if "error" in schema:
            raise HTTPException(status_code=400, detail=schema["error"])
        
        # Generate SQL
        sql_result = await groq_service.natural_language_to_sql(
            instruction=request.instruction,
            schema=schema,
            table_format="parquet"
        )
        
        safety_check = await groq_service.validate_sql_safety(sql_result["sql"])
        
        # Preview affected rows
        preview_data = None
        if sql_result["operation_type"] != "SELECT":
            preview_result = parquet_service.preview_changes(
                s3_path=request.s3_path,
                sql_query=sql_result["sql"],
                credentials=credentials,
                sample_size=10
            )
            if preview_result["success"]:
                preview_data = {
                    "sample_rows": preview_result["sample_rows"].to_dict(orient="records"),
                    "total_rows": preview_result["total_rows"],
                    "preview_size": preview_result["preview_size"]
                }
        
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
        logger.error(f"❌ [PARQUET] Preview failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Preview failed: {str(e)}")


@router.post("/execute-changes", response_model=SQLExecutionResponse)
async def execute_parquet_changes(
    request: SQLExecutionRequest,
    x_session_id: str = Header(..., alias="X-Session-ID")
) -> SQLExecutionResponse:
    """
    **Execute Parquet Data Modifications**
    
    Execute data modification on Parquet files using natural language.
    
    Example:
    ```json
    {
        "s3_path": "s3://metadataproject/test-data/",
        "instruction": "delete rows where age > 60",
        "create_backup": true
    }
    ```
    """
    try:
        session = get_session(x_session_id)
        credentials = session.credentials
        
        logger.info(f"⚡ [PARQUET] Execute: '{request.instruction}' on {request.s3_path}")
        
        if not groq_service.is_available():
            raise HTTPException(status_code=503, detail="LLM service not configured")
        
        # List and parse objects
        objects = user_s3_service.list_objects(credentials=credentials, s3_path=request.s3_path)
        if not objects:
            raise HTTPException(status_code=404, detail="No objects found")
        
        bucket, prefix = user_s3_service.parse_s3_path(request.s3_path)
        schema = read_parquet_schema(bucket, prefix, objects, credentials)
        
        if "error" in schema:
            raise HTTPException(status_code=400, detail=schema["error"])
        
        # Generate SQL
        sql_result = await groq_service.natural_language_to_sql(
            instruction=request.instruction,
            schema=schema,
            table_format="parquet"
        )
        
        safety_check = await groq_service.validate_sql_safety(sql_result["sql"])
        if not safety_check["is_safe"]:
            raise HTTPException(status_code=400, detail=f"Unsafe SQL: {', '.join(safety_check['warnings'])}")
        
        operation_type = sql_result["operation_type"]
        sql_query = sql_result["sql"]
        
        # Find first Parquet file
        parquet_files = [obj for obj in objects if obj.key.endswith('.parquet')]
        if not parquet_files:
            raise HTTPException(status_code=404, detail="No Parquet files found")
        
        first_parquet_path = f"s3://{bucket}/{parquet_files[0].key}"
        
        # Create backup if requested
        backup_path = None
        if request.create_backup and operation_type != "SELECT":
            backup_result = parquet_service.create_backup(first_parquet_path, credentials)
            if backup_result["success"]:
                backup_path = backup_result["backup_path"]
        
        # Execute SQL
        execution_result = parquet_service.execute_sql_on_s3_parquet(
            s3_path=first_parquet_path,
            sql_query=sql_query,
            credentials=credentials,
            operation_type=operation_type
        )
        
        if not execution_result["success"]:
            raise HTTPException(status_code=500, detail=execution_result["message"])
        
        # Write back for modifications
        if operation_type != "SELECT" and execution_result["result_data"] is not None:
            write_result = parquet_service.write_dataframe_to_s3(
                df=execution_result["result_data"],
                s3_path=first_parquet_path,
                credentials=credentials
            )
            if not write_result["success"]:
                raise HTTPException(status_code=500, detail=f"Failed to write back: {write_result['message']}")
            message = f"{execution_result['message']} Data written back to S3."
        else:
            message = execution_result["message"]
        
        # Return data for SELECT
        result_data = None
        if operation_type == "SELECT" and execution_result["result_data"] is not None:
            result_data = execution_result["result_data"].to_dict(orient="records")
        
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
        logger.error(f"❌ [PARQUET] Execution failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Execution failed: {str(e)}")
