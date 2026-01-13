"""
Format Detector for Lakehouse Table Formats.

Detects whether an S3 path contains Iceberg, Delta Lake, or Hudi tables
by examining directory structure and metadata files.
"""

import boto3
from enum import Enum
from typing import Optional
from botocore.exceptions import ClientError

from ..utils.logger import setup_logger
from ..utils.exceptions import FormatDetectionError

logger = setup_logger(__name__)


class TableFormat(str, Enum):
    """Supported table formats."""
    ICEBERG = "ICEBERG"
    DELTA = "DELTA"
    HUDI = "HUDI"
    UNKNOWN = "UNKNOWN"


class FormatDetector:
    """
    Detects table format by analyzing S3 bucket structure.
    
    Detection logic:
    - Iceberg: presence of "metadata/" directory
    - Delta Lake: presence of "_delta_log/" directory
    - Hudi: presence of ".hoodie/" directory
    """
    
    def __init__(self, aws_access_key_id: Optional[str] = None, 
                 aws_secret_access_key: Optional[str] = None,
                 region_name: str = "us-east-1"):
        """
        Initialize the format detector.
        
        Args:
            aws_access_key_id: AWS access key (optional, uses env vars if not provided)
            aws_secret_access_key: AWS secret key (optional, uses env vars if not provided)
            region_name: AWS region name
        """
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        logger.info("FormatDetector initialized")
    
    def detect_format(self, s3_path: str) -> TableFormat:
        """
        Detect the table format at the given S3 path.
        
        Args:
            s3_path: S3 path in format s3://bucket/prefix/table_name
            
        Returns:
            TableFormat enum value
            
        Raises:
            FormatDetectionError: If format cannot be detected or S3 access fails
        """
        logger.info(f"Detecting format for: {s3_path}")
        
        try:
            bucket, prefix = self._parse_s3_path(s3_path)
            
            # Check for format-specific directories
            if self._check_directory_exists(bucket, prefix, "metadata/"):
                logger.info(f"Detected ICEBERG format at {s3_path}")
                return TableFormat.ICEBERG
            
            if self._check_directory_exists(bucket, prefix, "_delta_log/"):
                logger.info(f"Detected DELTA format at {s3_path}")
                return TableFormat.DELTA
            
            if self._check_directory_exists(bucket, prefix, ".hoodie/"):
                logger.info(f"Detected HUDI format at {s3_path}")
                return TableFormat.HUDI
            
            raise FormatDetectionError(
                f"No recognized table format found at {s3_path}",
                details={
                    "path": s3_path,
                    "bucket": bucket,
                    "prefix": prefix
                }
            )
            
        except FormatDetectionError:
            raise
        except Exception as e:
            raise FormatDetectionError(
                f"Failed to detect format at {s3_path}: {str(e)}",
                details={"path": s3_path, "error": str(e)}
            )
    
    def _parse_s3_path(self, s3_path: str) -> tuple[str, str]:
        """
        Parse S3 path into bucket and prefix.
        
        Args:
            s3_path: S3 path like s3://bucket/prefix/table
            
        Returns:
            Tuple of (bucket, prefix)
            
        Raises:
            FormatDetectionError: If path format is invalid
        """
        if not s3_path.startswith("s3://"):
            raise FormatDetectionError(
                f"Invalid S3 path format: {s3_path}. Expected format: s3://bucket/prefix"
            )
        
        path_parts = s3_path.replace("s3://", "").split("/", 1)
        bucket = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ""
        
        # Ensure prefix ends with / for consistent directory checking
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        
        return bucket, prefix
    
    def _check_directory_exists(self, bucket: str, prefix: str, 
                               directory: str) -> bool:
        """
        Check if a specific directory exists under the given S3 prefix.
        
        Args:
            bucket: S3 bucket name
            prefix: S3 prefix (table path)
            directory: Directory name to check (e.g., "metadata/")
            
        Returns:
            True if directory exists, False otherwise
        """
        try:
            full_prefix = f"{prefix}{directory}"
            
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=full_prefix,
                MaxKeys=1
            )
            
            # If any objects exist with this prefix, directory exists
            exists = 'Contents' in response and len(response['Contents']) > 0
            
            logger.debug(
                f"Directory check: {directory} at s3://{bucket}/{prefix} = {exists}"
            )
            
            return exists
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                raise FormatDetectionError(
                    f"S3 bucket does not exist: {bucket}",
                    details={"bucket": bucket, "error_code": error_code}
                )
            elif error_code == 'AccessDenied':
                raise FormatDetectionError(
                    f"Access denied to S3 bucket: {bucket}",
                    details={"bucket": bucket, "error_code": error_code}
                )
            else:
                raise FormatDetectionError(
                    f"S3 error while checking directory: {str(e)}",
                    details={"bucket": bucket, "prefix": prefix, "error": str(e)}
                )
        except Exception as e:
            logger.error(f"Unexpected error checking directory: {str(e)}")
            return False
