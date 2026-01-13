"""
S3 utilities for working with table paths and objects.
"""

from typing import Tuple
from ..utils.exceptions import PlatformException


def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    """
    Parse S3 URI into bucket and key.
    
    Args:
        s3_uri: S3 URI like s3://bucket/path/to/object
        
    Returns:
        Tuple of (bucket, key)
        
    Raises:
        PlatformException: If URI format is invalid
    """
    if not s3_uri.startswith("s3://"):
        raise PlatformException(f"Invalid S3 URI format: {s3_uri}")
    
    path = s3_uri[5:]  # Remove s3://
    parts = path.split("/", 1)
    
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    
    return bucket, key


def build_s3_uri(bucket: str, key: str) -> str:
    """
    Build S3 URI from bucket and key.
    
    Args:
        bucket: S3 bucket name
        key: S3 key/path
        
    Returns:
        S3 URI like s3://bucket/path/to/object
    """
    key = key.lstrip("/")
    return f"s3://{bucket}/{key}"


def ensure_trailing_slash(path: str) -> str:
    """
    Ensure path ends with trailing slash.
    
    Args:
        path: Path string
        
    Returns:
        Path with trailing slash
    """
    return path if path.endswith("/") else path + "/"


def remove_trailing_slash(path: str) -> str:
    """
    Remove trailing slash from path.
    
    Args:
        path: Path string
        
    Returns:
        Path without trailing slash
    """
    return path.rstrip("/")
