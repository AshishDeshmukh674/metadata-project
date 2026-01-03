"""
Health Check Endpoint
=====================
Simple endpoint for checking API status.
"""

from fastapi import APIRouter
from datetime import datetime
from typing import Dict, Any

from app.config import settings

# Create router
router = APIRouter(
    prefix="/api/v1",
    tags=["Health"]
)


@router.get("/")
async def root() -> Dict[str, str]:
    """
    Root endpoint - Basic API information.
    
    Returns:
        API name and version
    """
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "status": "running",
        "environment": settings.environment
    }


@router.get("/health")
async def health_check() -> Dict[str, Any]:
    """
    Health check endpoint.
    
    Use this to verify the API is running.
    
    Returns:
        Health status with timestamp
    """
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "app_name": settings.app_name,
        "version": settings.app_version,
        "environment": settings.environment
    }
