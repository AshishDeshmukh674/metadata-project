"""
User Credentials Models
=======================
Models for handling user-provided AWS credentials.

Security Notes:
- Credentials are NEVER stored in database
- Only kept in memory for request duration
- Transmitted over HTTPS only
- Optionally encrypted in session storage
"""

from pydantic import BaseModel, Field, field_validator
#BaseModel is from Pydantic - it adds powerful features to your class:
from typing import Optional
#Optional means the field can be either the specified type OR None (null).
# success: bool  # Must provide, cannot be None
#success: Optional[bool]  # Can be True, False, or None
#optional is shorthand for success:bool | None
from datetime import datetime, timedelta


class UserAWSCredentials(BaseModel):
    """
    User-provided AWS credentials for accessing their S3 data.
    
    ⚠️ SECURITY: These should NEVER be persisted to database!
    Only use for the duration of the request or session.
    """
    aws_access_key_id: str = Field(
        ...,
        description="User's AWS Access Key ID",
        min_length=16,
        max_length=128,
        examples=["AKIAIOSFODNN7EXAMPLE"]
    )
    
    aws_secret_access_key: str = Field(
        ...,
        description="User's AWS Secret Access Key",
        min_length=20,
        examples=["wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"]
    )
    
    aws_session_token: Optional[str] = Field(
        None,
        description="Session token (for temporary credentials)"
    )
    
    aws_region: str = Field(
        default="us-east-1",
        description="AWS region to use"
    )
    
    @field_validator('aws_access_key_id')
    @classmethod
    def validate_access_key(cls, v: str) -> str:
        """Basic validation for access key format."""
        if not v.startswith('AKIA') and not v.startswith('ASIA'):
            raise ValueError(
                'AWS Access Key should start with AKIA (long-term) or ASIA (temporary)'
            )
        return v
    
    class Config:
        # Don't include in logs
        str_strip_whitespace = True
        json_schema_extra = {
            "example": {
                "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
                "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "aws_region": "us-east-1"
            }
        }


class UserS3Request(BaseModel):
    """
    Complete request with user credentials and S3 path.
    
    Users provide:
    1. Their AWS credentials
    2. The S3 path they want to explore
    """
    credentials: UserAWSCredentials = Field(
        ...,
        description="User's AWS credentials"
    )
    
    s3_path: str = Field(
        ...,
        description="S3 URI to explore",
        examples=["s3://my-bucket/warehouse/table"]
    )
    
    table_format: Optional[str] = Field(
        None,
        description="Table format hint (auto-detect if not provided)",
        examples=["auto", "iceberg", "delta", "hudi", "parquet"]
    )
    
    @field_validator('s3_path')
    @classmethod
    def validate_s3_path(cls, v: str) -> str:
        """Validate S3 path format."""
        if not v.startswith('s3://'):
            raise ValueError('S3 path must start with s3://')
        return v


class SessionCredentials(BaseModel):
    """
    Session-based credentials stored temporarily.
    
    Use this to avoid asking user for credentials on every request.
    Credentials expire after timeout.
    """
    session_id: str = Field(
        ...,
        description="Unique session identifier"
    )
    
    credentials: UserAWSCredentials = Field(
        ...,
        description="User's credentials"
    )
    
    created_at: datetime = Field(
        default_factory=datetime.now,
        description="When session was created"
    )
    
    expires_at: datetime = Field(
        ...,
        description="When session expires"
    )
    
    @classmethod
    def create_session(
        cls, 
        session_id: str, 
        credentials: UserAWSCredentials,
        ttl_minutes: int = 60
    ) -> "SessionCredentials":
        """
        Create a new session with expiration.
        
        Args:
            session_id: Unique session ID (use UUID)
            credentials: User's AWS credentials
            ttl_minutes: Time to live in minutes (default 1 hour)
        """
        now = datetime.now()
        expires_at = now + timedelta(minutes=ttl_minutes)
        
        return cls(
            session_id=session_id,
            credentials=credentials,
            created_at=now,
            expires_at=expires_at
        )
    
    def is_expired(self) -> bool:
        """Check if session has expired."""
        return datetime.now() > self.expires_at
    
    def remaining_minutes(self) -> int:
        """Get remaining minutes before expiration."""
        if self.is_expired():
            return 0
        delta = self.expires_at - datetime.now()
        return int(delta.total_seconds() / 60)




#This is a class definition, not a function - it defines the structure of a response object.
#if validation is successful, an instance of this class can be created to represent the result.
#eg.
# {
#     "success": true,
#     "message": "Credentials are valid",
#     "user_account_id": "123456789012",
#     "accessible_buckets": 5,
#     "region": "us-east-1"
# }
class CredentialTestResponse(BaseModel):
    """Response for credential validation."""
    success: bool = Field(description="Whether credentials are valid")
    message: str = Field(description="Status message")
    user_account_id: Optional[str] = Field(
        None, #default 
        description="AWS account ID (if successful)"
    )
    accessible_buckets: Optional[int] = Field(
        None,
        description="Number of accessible buckets"
    )
    region: str = Field(description="AWS region used")
