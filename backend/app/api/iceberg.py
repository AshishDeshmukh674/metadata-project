"""
Iceberg API
===========
Endpoints for Apache Iceberg table operations.
"""

from fastapi import APIRouter, HTTPException, Header
import logging

from app.models.datastore import DataModificationRequest, SQLExecutionRequest
from app.services.iceberg_service import iceberg_service
from app.api.auth import get_session_store

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/iceberg",
    tags=["Iceberg Operations"]
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
async def preview_iceberg_changes(
    request: DataModificationRequest,
    x_session_id: str = Header(..., alias="X-Session-ID")
):
    """
    **Preview Iceberg Table Modifications** (Coming Soon)
    
    Apache Iceberg support requires REST catalog configuration.
    """
    try:
        session = get_session(x_session_id)
        credentials = session.credentials
        
        return {
            "success": False,
            "message": "Iceberg support coming soon - requires Iceberg REST catalog setup",
            "documentation": "https://iceberg.apache.org/docs/latest/"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [ICEBERG] Preview failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/execute-changes")
async def execute_iceberg_changes(
    request: SQLExecutionRequest,
    x_session_id: str = Header(..., alias="X-Session-ID")
):
    """
    **Execute Iceberg Table Modifications** (Coming Soon)
    
    Apache Iceberg modifications require Spark or Trino integration.
    """
    try:
        session = get_session(x_session_id)
        credentials = session.credentials
        
        return {
            "success": False,
            "message": "Iceberg modification requires Spark/Trino integration",
            "documentation": "https://iceberg.apache.org/docs/latest/spark-writes/"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [ICEBERG] Execution failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
