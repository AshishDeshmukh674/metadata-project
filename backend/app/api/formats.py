"""
Table Formats API
=================
Endpoint to list available table formats and their capabilities.
"""

from fastapi import APIRouter
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/formats",
    tags=["Table Formats"]
)


@router.get("/list")
async def list_table_formats() -> Dict:
    """
    **List Available Table Formats**
    
    Returns all supported table formats with their current implementation status.
    Use this endpoint to show format options in your UI.
    
    **Response:**
    - format: Format identifier (parquet, iceberg, delta, hudi)
    - name: Display name
    - status: ready, read_only, in_development
    - description: What this format supports
    - preview_endpoint: API endpoint for previewing changes
    - execute_endpoint: API endpoint for executing changes
    """
    
    formats = [
        {
            "format": "parquet",
            "name": "Apache Parquet",
            "status": "ready",
            "description": "Full support for reading and modifying Parquet files using natural language",
            "features": [
                "Natural language to SQL conversion",
                "Preview before execution",
                "Backup creation",
                "SELECT, UPDATE, DELETE, INSERT operations"
            ],
            "preview_endpoint": "/api/v1/parquet/preview-changes",
            "execute_endpoint": "/api/v1/parquet/execute-changes",
            "icon": "📄"
        },
        {
            "format": "delta",
            "name": "Delta Lake",
            "status": "read_only",
            "description": "Read support with preview capabilities. Write operations in development.",
            "features": [
                "Table schema reading",
                "Version information",
                "Preview with natural language",
                "SELECT queries"
            ],
            "preview_endpoint": "/api/v1/delta/preview-changes",
            "execute_endpoint": "/api/v1/delta/execute-changes",
            "icon": "🔺",
            "note": "Write operations coming soon"
        },
        {
            "format": "iceberg",
            "name": "Apache Iceberg",
            "status": "in_development",
            "description": "Requires Iceberg REST catalog configuration. Implementation in progress.",
            "features": [
                "Planned: Full table operations",
                "Planned: Time travel queries",
                "Planned: Schema evolution"
            ],
            "preview_endpoint": "/api/v1/iceberg/preview-changes",
            "execute_endpoint": "/api/v1/iceberg/execute-changes",
            "icon": "🧊",
            "note": "Requires REST catalog setup",
            "documentation": "https://iceberg.apache.org"
        },
        {
            "format": "hudi",
            "name": "Apache Hudi",
            "status": "in_development",
            "description": "Requires Apache Spark integration. Implementation planned.",
            "features": [
                "Planned: Upsert operations",
                "Planned: Incremental processing",
                "Planned: Record-level operations"
            ],
            "preview_endpoint": "/api/v1/hudi/preview-changes",
            "execute_endpoint": "/api/v1/hudi/execute-changes",
            "icon": "🚀",
            "note": "Requires Spark integration",
            "documentation": "https://hudi.apache.org"
        }
    ]
    
    return {
        "formats": formats,
        "recommendation": "Use Parquet for immediate full functionality. Delta, Iceberg, and Hudi are under development."
    }


@router.get("/supported")
async def get_supported_formats() -> Dict:
    """
    **Get Fully Supported Formats**
    
    Returns only the formats that are fully operational.
    """
    return {
        "ready": ["parquet"],
        "read_only": ["delta"],
        "in_development": ["iceberg", "hudi"]
    }
