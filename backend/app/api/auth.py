"""
Authentication API
==================
Endpoints for user credential validation and session management.
"""

from fastapi import APIRouter, HTTPException, Request
import uuid
# UUID = Universally Unique IDentifier. It's a random 128-bit number that's guaranteed to be unique (extremely low chance of duplicates).
import logging
from typing import Dict, Any

from app.config import settings
from app.models.user_credentials import (
    UserAWSCredentials,
    CredentialTestResponse,
    SessionCredentials
)
from app.services.user_s3_service import user_s3_service

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/api/v1/auth",
    tags=["Authentication"]
)

# In-memory session storage (use Redis in production!)
_session_store: Dict[str, SessionCredentials] = {}


@router.post("/validate-credentials")
async def validate_credentials(
    request: Request,
    credentials: UserAWSCredentials
) -> CredentialTestResponse:
    """
    **Step 1: Validate AWS Credentials**
    
    Validates user's AWS credentials without storing them.
    
    This endpoint:
    - ✅ Tests if credentials are valid
    - ✅ Gets AWS account ID
    - ✅ Checks S3 access permissions
    - ❌ Does NOT store credentials
    
    **Required for**: Creating a session
    
    Request Body:
    ```json
    {
        "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
        "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/...",
        "aws_region": "us-east-1"
    }
    ```
    
    Returns:
        Validation result with account info
    """
    try:
        # Check HTTPS requirement in production
        if settings.require_https and not settings.debug:
            if request.url.scheme != "https":
                raise HTTPException(
                    status_code=403,
                    detail="HTTPS required for credential submission"
                )
        
        logger.info("🔐 Validating credentials...")
        
        # Validate credentials
        result = user_s3_service.validate_credentials(credentials)
        
        if not result["success"]:
            raise HTTPException(
                status_code=401,
                detail=result["message"]
            )
        
        logger.info(f"✅ Credentials valid for account: {result.get('account_id')}")
        
        return CredentialTestResponse(
            success=True,
            message=result["message"],
            user_account_id=result.get("account_id"),
            accessible_buckets=result.get("accessible_buckets"),
            region=result["region"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error validating credentials: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to validate credentials"
        )


@router.post("/create-session")
async def create_session(
    request: Request,
    credentials: UserAWSCredentials
) -> Dict[str, Any]:
    """
    **Step 2: Create Session**
    
    Creates a session after validating credentials.
    Returns a session ID to use for all subsequent API calls.
    
    **Prerequisites**: Credentials must be valid (test with /validate-credentials first)
    
    Request Body:
    ```json
    {
        "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
        "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/...",
        "aws_region": "us-east-1"
    }
    ```
    
    Returns:
    ```json
    {
        "session_id": "uuid-here",
        "expires_in_minutes": 30,
        "message": "Session created successfully"
    }
    ```
    
    **Usage**: Include `X-Session-ID: <session_id>` header in all datastore API calls
    
    Security:
        - Session expires after 30 minutes (configurable)
        - Credentials stored in memory only
        - Use Redis with encryption in production
    """
    try:
        # Check HTTPS requirement
        if settings.require_https and not settings.debug:
            if request.url.scheme != "https":
                raise HTTPException(
                    status_code=403,
                    detail="HTTPS required for credential submission"
                )
        
        # Validate credentials first
        validation = user_s3_service.validate_credentials(credentials)
        if not validation["success"]:
            raise HTTPException(
                status_code=401,
                detail=f"Invalid credentials: {validation['message']}"
            )
        
        # Create session
        session_id = str(uuid.uuid4())
        session = SessionCredentials.create_session(
            session_id=session_id,
            credentials=credentials,
            ttl_minutes=settings.session_ttl_minutes
        )
        
        # Store in memory (use Redis in production!)
        _session_store[session_id] = session
        
        logger.info(f"✅ Created session {session_id} for account {validation.get('account_id')}")
        
        return {
            "session_id": session_id,
            "expires_in_minutes": settings.session_ttl_minutes,
            "message": "Session created successfully. Use this session_id in X-Session-ID header for all API calls."
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating session: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to create session"
        )


@router.delete("/session/{session_id}")
async def delete_session(session_id: str) -> Dict[str, str]:
    """
    Delete/logout a session.
    
    Args:
        session_id: Session ID to delete
        
    Returns:
        Success message
    """
    if session_id in _session_store:
        del _session_store[session_id]
        logger.info(f"🗑️ Deleted session {session_id}")
        return {"message": "Session deleted successfully"}
    else:
        raise HTTPException(
            status_code=404,
            detail="Session not found"
        )


@router.get("/session-status")
async def get_session_status(session_id: str) -> Dict[str, Any]:
    """
    Check if a session is still valid.
    
    Args:
        session_id: Session ID to check
        
    Returns:
        Session status information
    """
    session = _session_store.get(session_id)
    
    if not session:
        return {
            "valid": False,
            "message": "Session not found"
        }
    
    if session.is_expired():
        del _session_store[session_id]
        return {
            "valid": False,
            "message": "Session expired"
        }
    
    return {
        "valid": True,
        "created_at": session.created_at.isoformat(),
        "expires_at": session.expires_at.isoformat(),
        "region": session.credentials.aws_region
    }


# Export session store for datastore.py to use
def get_session_store() -> Dict[str, SessionCredentials]:
    """Get reference to session store for other modules."""
    return _session_store
