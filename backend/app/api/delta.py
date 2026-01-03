"""
Delta Lake API
==============
Endpoints for Delta Lake table operations.
"""

from fastapi import APIRouter, HTTPException, Header
import logging

from app.models.datastore import (
    DataModificationRequest,
    SQLExecutionRequest,
    SQLExecutionResponse
)
from app.services.delta_service import delta_service
from app.services.groq_service import groq_service
from app.api.auth import get_session_store

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/delta",
    tags=["Delta Lake Operations"]
)


def get_session(session_id: str):
    """Get session from store."""
    _session_store = get_session_store()
    if not session_id or session_id not in _session_store:
        raise HTTPException(status_code=401, detail="Invalid or missing session")
    session = _session_store[session_id]
    if session.is_expired():
        del _session_store[session_id]
        raise HTTPException(status_code=401, detail="Session expired")
    return session


@router.post("/preview-changes")
async def preview_delta_changes(
    request: DataModificationRequest,
    x_session_id: str = Header(..., alias="X-Session-ID")
):
    """
    **Preview Delta Lake Table Modifications**
    
    Read Delta table and show what would change.
    
    Example:
    ```json
    {
        "s3_path": "s3://bucket/delta_table/",
        "instruction": "show users where department = IT"
    }
    ```
    """
    try:
        session = get_session(x_session_id)
        credentials = session.credentials
        
        logger.info(f"🔍 [DELTA] Preview: '{request.instruction}' on {request.s3_path}")
        
        if not groq_service.is_available():
            raise HTTPException(status_code=503, detail="LLM service not configured")
        
        # Read Delta table
        read_result = delta_service.read_delta_table(request.s3_path, credentials)
        
        if not read_result["success"]:
            raise HTTPException(status_code=400, detail=read_result["message"])
        
        df = read_result["data"]
        metadata = read_result["metadata"]
        
        # Generate SQL using schema
        sql_result = await groq_service.natural_language_to_sql(
            instruction=request.instruction,
            schema=metadata,
            table_format="delta"
        )
        
        safety_check = await groq_service.validate_sql_safety(sql_result["sql"])
        
        return {
            "success": True,
            "sql": sql_result["sql"],
            "explanation": sql_result["explanation"],
            "operation_type": sql_result["operation_type"],
            "affected_columns": sql_result["affected_columns"],
            "requires_backup": sql_result["requires_backup"],
            "estimated_impact": sql_result["estimated_impact"],
            "safety_check": safety_check,
            "table_info": {
                "version": metadata["version"],
                "num_rows": metadata["num_rows"],
                "num_columns": metadata["num_columns"]
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [DELTA] Preview failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/execute-changes", response_model=SQLExecutionResponse)
async def execute_delta_changes(
    request: SQLExecutionRequest,
    x_session_id: str = Header(..., alias="X-Session-ID")
) -> SQLExecutionResponse:
    """
    **Execute Delta Lake Table Modifications**
    
    Execute modifications on Delta table using natural language.
    
    Example:
    ```json
    {
        "s3_path": "s3://bucket/delta_table/",
        "instruction": "delete rows where age > 60",
        "create_backup": true
    }
    ```
    """
    try:
        session = get_session(x_session_id)
        credentials = session.credentials
        
        logger.info(f"⚡ [DELTA] Execute: '{request.instruction}' on {request.s3_path}")
        
        # Read Delta table
        read_result = delta_service.read_delta_table(request.s3_path, credentials)
        
        if not read_result["success"]:
            raise HTTPException(status_code=400, detail=read_result["message"])
        
        df = read_result["data"]
        
        # For now, return limited support message
        # Full Delta operations require proper SQL engine integration
        return SQLExecutionResponse(
            success=False,
            message="Delta Lake modification is in development. Currently supports read-only operations.",
            sql="",
            operation_type="",
            rows_affected=0,
            execution_time_ms=0.0,
            backup_path=None,
            result_data=df.head(100).to_dict(orient="records")
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [DELTA] Execution failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
