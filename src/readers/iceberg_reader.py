"""
Iceberg Metadata Reader.

Reads and parses Apache Iceberg table metadata from S3.
Returns raw metadata without normalization.
"""

import json
import boto3
from typing import Dict, Optional, List
from botocore.exceptions import ClientError

from ..utils.logger import setup_logger
from ..utils.exceptions import MetadataReadError

logger = setup_logger(__name__)


class IcebergReader:
    """
    Reads Apache Iceberg table metadata.
    
    Iceberg stores metadata in JSON files under the metadata/ directory.
    The version-hint.text file contains the name of the latest metadata file.
    """
    
    def __init__(self, aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 region_name: str = "us-east-1"):
        """
        Initialize the Iceberg metadata reader.
        
        Args:
            aws_access_key_id: AWS access key (optional)
            aws_secret_access_key: AWS secret key (optional)
            region_name: AWS region name
        """
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        logger.info("IcebergReader initialized")
    
    def read_metadata(self, s3_path: str) -> Dict:
        """
        Read Iceberg metadata from S3 path.
        
        Args:
            s3_path: S3 path to the Iceberg table (e.g., s3://bucket/warehouse/table)
            
        Returns:
            Dictionary containing raw Iceberg metadata with keys:
                - schema: List of fields with name, type, required, etc.
                - partition_spec: Partition specification
                - snapshots: List of snapshots (for time travel)
                - current_snapshot_id: Current snapshot ID
                - properties: Table properties
                - location: Table location
                
        Raises:
            MetadataReadError: If metadata cannot be read
        """
        logger.info(f"Reading Iceberg metadata from: {s3_path}")
        
        try:
            bucket, prefix = self._parse_s3_path(s3_path)
            
            # Get the latest metadata file
            metadata_file = self._get_latest_metadata_file(bucket, prefix)
            
            # Read and parse metadata JSON
            metadata_content = self._read_s3_object(bucket, metadata_file)
            metadata = json.loads(metadata_content)
            
            # Extract key information
            raw_metadata = {
                "format": "ICEBERG",
                "location": s3_path,
                "schema": self._extract_schema(metadata),
                "partition_spec": self._extract_partition_spec(metadata),
                "snapshots": metadata.get("snapshots", []),
                "current_snapshot_id": metadata.get("current-snapshot-id"),
                "properties": metadata.get("properties", {}),
                "format_version": metadata.get("format-version", 1),
                "last_updated_ms": metadata.get("last-updated-ms"),
                "metadata_file": metadata_file
            }
            
            logger.info(f"Successfully read Iceberg metadata: {len(raw_metadata['schema'])} columns, "
                       f"{len(raw_metadata['snapshots'])} snapshots")
            
            return raw_metadata
            
        except MetadataReadError:
            raise
        except Exception as e:
            raise MetadataReadError(
                f"Failed to read Iceberg metadata from {s3_path}: {str(e)}",
                details={"path": s3_path, "error": str(e)}
            )
    
    def _parse_s3_path(self, s3_path: str) -> tuple[str, str]:
        """Parse S3 path into bucket and prefix."""
        if not s3_path.startswith("s3://"):
            raise MetadataReadError(f"Invalid S3 path format: {s3_path}")
        
        path_parts = s3_path.replace("s3://", "").split("/", 1)
        bucket = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ""
        
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        
        return bucket, prefix
    
    def _get_latest_metadata_file(self, bucket: str, prefix: str) -> str:
        """
        Get the path to the latest Iceberg metadata file.
        
        Tries multiple approaches:
        1. Read version-hint.text
        2. Find latest metadata JSON by timestamp
        
        Args:
            bucket: S3 bucket name
            prefix: Table prefix
            
        Returns:
            S3 key to the latest metadata file
        """
        metadata_prefix = f"{prefix}metadata/"
        
        # Try version-hint.text first
        try:
            version_hint_key = f"{metadata_prefix}version-hint.text"
            version_hint = self._read_s3_object(bucket, version_hint_key).strip()
            metadata_file = f"{metadata_prefix}{version_hint}"
            
            # Verify the file exists
            self.s3_client.head_object(Bucket=bucket, Key=metadata_file)
            logger.debug(f"Found metadata file via version-hint: {metadata_file}")
            return metadata_file
            
        except ClientError:
            logger.debug("version-hint.text not found, searching for latest metadata file")
        
        # Fallback: Find latest metadata JSON by listing
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=metadata_prefix
            )
            
            if 'Contents' not in response:
                raise MetadataReadError(
                    f"No metadata files found in {metadata_prefix}",
                    details={"bucket": bucket, "prefix": metadata_prefix}
                )
            
            # Filter for .metadata.json files and get the latest
            metadata_files = [
                obj for obj in response['Contents']
                if obj['Key'].endswith('.metadata.json')
            ]
            
            if not metadata_files:
                raise MetadataReadError(
                    f"No .metadata.json files found in {metadata_prefix}",
                    details={"bucket": bucket, "prefix": metadata_prefix}
                )
            
            # Sort by last modified and get the latest
            latest_file = sorted(metadata_files, key=lambda x: x['LastModified'], reverse=True)[0]
            logger.debug(f"Found latest metadata file: {latest_file['Key']}")
            
            return latest_file['Key']
            
        except ClientError as e:
            raise MetadataReadError(
                f"Failed to list metadata files: {str(e)}",
                details={"bucket": bucket, "prefix": metadata_prefix, "error": str(e)}
            )
    
    def _read_s3_object(self, bucket: str, key: str) -> str:
        """Read content from S3 object."""
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            return content
        except ClientError as e:
            raise MetadataReadError(
                f"Failed to read S3 object s3://{bucket}/{key}: {str(e)}",
                details={"bucket": bucket, "key": key, "error": str(e)}
            )
    
    def _extract_schema(self, metadata: Dict) -> List[Dict]:
        """
        Extract schema from Iceberg metadata.
        
        Args:
            metadata: Parsed Iceberg metadata JSON
            
        Returns:
            List of field dictionaries with name, type, required, etc.
        """
        schema = metadata.get("schema", {})
        
        # Handle both current-schema-id and schema formats
        if "fields" in schema:
            return schema["fields"]
        elif "schemas" in metadata:
            # Multiple schemas, get current one
            current_schema_id = metadata.get("current-schema-id", 0)
            for schema_entry in metadata["schemas"]:
                if schema_entry.get("schema-id") == current_schema_id:
                    return schema_entry.get("fields", [])
        
        return []
    
    def _extract_partition_spec(self, metadata: Dict) -> List[Dict]:
        """
        Extract partition specification from Iceberg metadata.
        
        Args:
            metadata: Parsed Iceberg metadata JSON
            
        Returns:
            List of partition field specifications
        """
        partition_spec = metadata.get("partition-spec", [])
        
        # Handle both direct partition-spec and partition-specs array
        if isinstance(partition_spec, list):
            return partition_spec
        elif "partition-specs" in metadata:
            # Multiple specs, get the default one
            default_spec_id = metadata.get("default-spec-id", 0)
            for spec in metadata["partition-specs"]:
                if spec.get("spec-id") == default_spec_id:
                    return spec.get("fields", [])
        
        return []
