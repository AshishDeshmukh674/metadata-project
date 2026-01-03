"""
User-Specific S3 Service
=========================
Handles S3 operations using user-provided credentials.

Key Differences from s3_service.py:
- Uses credentials provided by user (not from .env)
- Validates credentials before use
- Does NOT store credentials
- Creates temporary boto3 clients per request

Security Features:
- Credentials only in memory
- Never logged or persisted
- Client objects garbage collected after use
- Can add encryption for session storage
"""

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from typing import Optional, List, Dict, Any
import logging

from app.models.user_credentials import UserAWSCredentials
from app.models.s3_models import S3ObjectInfo
from app.config import settings

logger = logging.getLogger(__name__)


class UserS3Service:
    """
    S3 service that uses user-provided credentials.
    
    Unlike the main S3Service, this creates clients
    with user-specific credentials for each request.
    
    Security: Credentials are NEVER stored, only used
    to create boto3 client and then discarded.
    """
    
    def create_client(self, credentials: UserAWSCredentials):
        """
        Create boto3 S3 client with user's credentials.
        
        ⚠️ SECURITY: Never log or store these credentials!
        
        Args:
            credentials: User's AWS credentials
            
        Returns:
            boto3 S3 client configured with user's credentials
            
        Raises:
            ValueError: If credentials are invalid
        """
        try:
            logger.info(f"Creating S3 client for region: {credentials.aws_region}")
            # Note: We log region but NEVER log the actual credentials
            
            # Create client with user credentials
            client = boto3.client(
                's3',
                aws_access_key_id=credentials.aws_access_key_id,
                aws_secret_access_key=credentials.aws_secret_access_key,
                aws_session_token=credentials.aws_session_token,  # For temp credentials
                region_name=credentials.aws_region
            )
            
            return client
            
        except Exception as e:
            logger.error(f"Failed to create S3 client: {str(e)}")
            # Don't expose the actual error (might contain credential info)
            raise ValueError("Failed to create S3 client with provided credentials")
    
    def validate_credentials(
        self, 
        credentials: UserAWSCredentials
    ) -> Dict[str, Any]:
        """
        Validate user's AWS credentials.
        
        This does a simple test by:
        1. Creating a client with their credentials
        2. Calling get_caller_identity to verify they work
        3. Optionally listing buckets they can access
        
        Args:
            credentials: User's AWS credentials to validate
            
        Returns:
            Dict with validation results
        """
        try:
            # Create STS client to get caller identity
            sts_client = boto3.client(
                'sts',
                aws_access_key_id=credentials.aws_access_key_id,
                aws_secret_access_key=credentials.aws_secret_access_key,
                aws_session_token=credentials.aws_session_token,
                region_name=credentials.aws_region
            )
            
            # Get caller identity (works with any valid credentials)
            identity = sts_client.get_caller_identity()
            account_id = identity['Account']
            user_arn = identity['Arn']
            
            logger.info(f"✅ Credentials validated for account: {account_id}")
            
            # Try to list buckets (might fail if no permissions, but that's ok)
            s3_client = self.create_client(credentials)
            try:
                response = s3_client.list_buckets()
                bucket_count = len(response.get('Buckets', []))
                logger.info(f"   User can access {bucket_count} buckets")
            except ClientError:
                bucket_count = None
                logger.info("   User cannot list all buckets (limited permissions)")
            
            return {
                "success": True,
                "message": "Credentials are valid",
                "account_id": account_id,
                "user_arn": user_arn,
                "accessible_buckets": bucket_count,
                "region": credentials.aws_region
            }
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"❌ Credential validation failed: {error_code}")
            
            if error_code == 'InvalidClientTokenId':
                message = "Invalid Access Key ID"
            elif error_code == 'SignatureDoesNotMatch':
                message = "Invalid Secret Access Key"
            elif error_code == 'ExpiredToken':
                message = "Session token has expired"
            else:
                message = f"AWS Error: {error_code}"
            
            return {
                "success": False,
                "message": message,
                "error_code": error_code,
                "region": credentials.aws_region
            }
            
        except Exception as e:
            logger.error(f"❌ Unexpected error validating credentials: {str(e)}")
            return {
                "success": False,
                "message": "Failed to validate credentials",
                "region": credentials.aws_region
            }
    
    def parse_s3_path(self, s3_path: str) -> tuple[str, str]:
        """Parse S3 URI into bucket and prefix."""
        path = s3_path.replace('s3://', '')
        parts = path.split('/', 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''
        return bucket, prefix
    
    def list_objects(
        self,
        credentials: UserAWSCredentials,
        s3_path: str,
        max_keys: int = 100
    ) -> List[S3ObjectInfo]:
        """
        List objects using user's credentials.
        
        Args:
            credentials: User's AWS credentials
            s3_path: S3 URI to list
            max_keys: Maximum objects to return
            
        Returns:
            List of S3ObjectInfo
            
        Raises:
            ValueError: If access denied or bucket doesn't exist
        """
        bucket, prefix = self.parse_s3_path(s3_path)
        client = self.create_client(credentials)
        
        try:
            logger.info(
                f"Listing objects in s3://{bucket}/{prefix} "
                f"(using user credentials)"
            )
            
            response = client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            objects = []
            for obj in response.get('Contents', []):
                objects.append(S3ObjectInfo(
                    key=obj['Key'],
                    size=obj['Size'],
                    last_modified=obj['LastModified'],
                    storage_class=obj.get('StorageClass', 'STANDARD')
                ))
            
            logger.info(f"✅ Found {len(objects)} objects")
            return objects
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"❌ Error listing objects: {error_code}")
            
            if error_code == 'NoSuchBucket':
                raise ValueError(f"Bucket '{bucket}' does not exist")
            elif error_code == 'AccessDenied':
                raise ValueError(
                    f"Access denied to bucket '{bucket}'. "
                    "Check your AWS credentials have s3:ListBucket permission."
                )
            else:
                raise ValueError(f"S3 Error: {error_code}")
    
    def check_bucket_access(
        self,
        credentials: UserAWSCredentials,
        bucket: str
    ) -> Dict[str, Any]:
        """
        Check if user has access to a specific bucket.
        
        Tests different permission levels:
        - Can list bucket?
        - Can read objects?
        - Can write objects?
        
        Args:
            credentials: User's credentials
            bucket: Bucket name (without s3://)
            
        Returns:
            Dict with permission details
        """
        client = self.create_client(credentials)
        permissions = {
            "bucket": bucket,
            "can_list": False,
            "can_read": False,
            "can_write": False
        }
        
        # Test ListBucket permission
        try:
            client.list_objects_v2(Bucket=bucket, MaxKeys=1)
            permissions["can_list"] = True
            logger.info(f"✅ User can LIST bucket: {bucket}")
        except ClientError as e:
            logger.info(f"❌ User cannot LIST bucket: {bucket} ({e.response['Error']['Code']})")
        
        # Test GetObject permission (try to read a small object)
        try:
            # List one object first
            response = client.list_objects_v2(Bucket=bucket, MaxKeys=1)
            if response.get('Contents'):
                key = response['Contents'][0]['Key']
                client.head_object(Bucket=bucket, Key=key)
                permissions["can_read"] = True
                logger.info(f"✅ User can READ from bucket: {bucket}")
        except ClientError:
            logger.info(f"❌ User cannot READ from bucket: {bucket}")
        
        # Note: We don't test write permissions to avoid creating objects
        
        return permissions


# ===================================
# Global Service Instance
# ===================================
user_s3_service = UserS3Service()
