"""
Hudi Metadata Reader.

Reads and parses Apache Hudi table metadata from S3.
Returns raw metadata without normalization.
"""

import json
import boto3
from typing import Dict, Optional, List
from botocore.exceptions import ClientError
from configparser import ConfigParser
from io import StringIO

from ..utils.logger import setup_logger
from ..utils.exceptions import MetadataReadError

logger = setup_logger(__name__)


class HudiReader:
    """
    Reads Apache Hudi table metadata.
    
    Hudi stores metadata in .hoodie/ directory:
    - hoodie.properties: Table configuration and properties
    - .hoodie/metadata/: Timeline metadata
    - .commit files: Commit transaction logs
    """
    
    def __init__(self, aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 region_name: str = "us-east-1"):
        """
        Initialize the Hudi metadata reader.
        
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
        logger.info("HudiReader initialized")
    
    def read_metadata(self, s3_path: str) -> Dict:
        """
        Read Hudi metadata from S3 path.
        
        Args:
            s3_path: S3 path to the Hudi table (e.g., s3://bucket/warehouse/table)
            
        Returns:
            Dictionary containing raw Hudi metadata with keys:
                - schema: Table schema
                - partition_path: Partition path configuration
                - table_name: Table name from properties
                - table_type: COPY_ON_WRITE or MERGE_ON_READ
                - timeline: Commit timeline information
                - properties: All table properties
                - supports_time_travel: Boolean (based on timeline)
                
        Raises:
            MetadataReadError: If metadata cannot be read
        """
        logger.info(f"Reading Hudi metadata from: {s3_path}")
        
        try:
            bucket, prefix = self._parse_s3_path(s3_path)
            
            # Read hoodie.properties
            properties = self._read_hoodie_properties(bucket, prefix)
            
            # Read commit timeline
            timeline = self._read_commit_timeline(bucket, prefix)
            
            # Try to read schema from latest commit
            schema = self._extract_schema_from_commit(bucket, prefix, timeline)
            
            # Build raw metadata
            raw_metadata = {
                "format": "HUDI",
                "location": s3_path,
                "table_name": properties.get("hoodie.table.name", "unknown"),
                "table_type": properties.get("hoodie.table.type", "COPY_ON_WRITE"),
                "schema": schema,
                "partition_fields": self._extract_partition_fields(properties),
                "properties": properties,
                "timeline": timeline,
                "supports_time_travel": len(timeline) > 1,  # Multiple commits enable time travel
                "base_path": properties.get("hoodie.table.base.path", s3_path)
            }
            
            logger.info(f"Successfully read Hudi metadata: {len(schema)} columns, "
                       f"{len(timeline)} commits")
            
            return raw_metadata
            
        except MetadataReadError:
            raise
        except Exception as e:
            raise MetadataReadError(
                f"Failed to read Hudi metadata from {s3_path}: {str(e)}",
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
    
    def _read_hoodie_properties(self, bucket: str, prefix: str) -> Dict[str, str]:
        """
        Read and parse hoodie.properties file.
        
        Args:
            bucket: S3 bucket name
            prefix: Table prefix
            
        Returns:
            Dictionary of properties
        """
        properties_key = f"{prefix}.hoodie/hoodie.properties"
        
        try:
            content = self._read_s3_object(bucket, properties_key)
            
            # Parse properties file (Java properties format)
            config = ConfigParser()
            config.read_string("[DEFAULT]\n" + content)
            
            properties = dict(config.items("DEFAULT"))
            logger.debug(f"Read {len(properties)} properties from hoodie.properties")
            
            return properties
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                raise MetadataReadError(
                    f"hoodie.properties not found at s3://{bucket}/{properties_key}",
                    details={"bucket": bucket, "key": properties_key}
                )
            else:
                raise MetadataReadError(
                    f"Failed to read hoodie.properties: {str(e)}",
                    details={"bucket": bucket, "key": properties_key, "error": str(e)}
                )
        except Exception as e:
            logger.warning(f"Error parsing hoodie.properties: {e}")
            # Return empty dict if parsing fails
            return {}
    
    def _read_commit_timeline(self, bucket: str, prefix: str) -> List[Dict]:
        """
        Read commit timeline from .hoodie directory.
        
        Commit files have extensions like: .commit, .deltacommit, .replacecommit
        
        Args:
            bucket: S3 bucket name
            prefix: Table prefix
            
        Returns:
            List of commit information dictionaries
        """
        hoodie_prefix = f"{prefix}.hoodie/"
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=hoodie_prefix
            )
            
            if 'Contents' not in response:
                logger.warning(f"No files found in {hoodie_prefix}")
                return []
            
            # Filter for commit files
            commit_extensions = ['.commit', '.deltacommit', '.replacecommit', '.inflight']
            commit_files = [
                obj for obj in response['Contents']
                if any(obj['Key'].endswith(ext) for ext in commit_extensions)
            ]
            
            timeline = []
            for commit_file in sorted(commit_files, key=lambda x: x['LastModified']):
                filename = commit_file['Key'].split('/')[-1]
                # Extract timestamp from filename (format: timestamp.commit)
                commit_time = filename.split('.')[0]
                commit_type = filename.split('.')[-1]
                
                timeline.append({
                    "commit_time": commit_time,
                    "commit_type": commit_type,
                    "file_key": commit_file['Key'],
                    "last_modified": commit_file['LastModified'].isoformat()
                })
            
            logger.debug(f"Found {len(timeline)} commits in timeline")
            return timeline
            
        except ClientError as e:
            logger.warning(f"Failed to read commit timeline: {e}")
            return []
    
    def _extract_schema_from_commit(self, bucket: str, prefix: str, 
                                    timeline: List[Dict]) -> List[Dict]:
        """
        Extract schema from the latest commit file.
        
        Hudi commit files can contain schema information in JSON format.
        
        Args:
            bucket: S3 bucket name
            prefix: Table prefix
            timeline: Commit timeline
            
        Returns:
            List of field dictionaries
        """
        if not timeline:
            logger.warning("No commits found in timeline, schema extraction skipped")
            return []
        
        # Try to read schema from the latest commit
        for commit in reversed(timeline):  # Start from latest
            try:
                commit_key = commit['file_key']
                content = self._read_s3_object(bucket, commit_key)
                
                # Try to parse as JSON
                commit_data = json.loads(content)
                
                # Look for schema in various possible locations
                if "metadata" in commit_data and "schema" in commit_data["metadata"]:
                    schema_str = commit_data["metadata"]["schema"]
                    schema = json.loads(schema_str) if isinstance(schema_str, str) else schema_str
                    
                    if "fields" in schema:
                        return schema["fields"]
                
            except (json.JSONDecodeError, KeyError, ClientError) as e:
                logger.debug(f"Could not extract schema from {commit['file_key']}: {e}")
                continue
        
        logger.warning("Could not extract schema from any commit file")
        return []
    
    def _extract_partition_fields(self, properties: Dict[str, str]) -> List[str]:
        """
        Extract partition field names from Hudi properties.
        
        Args:
            properties: Hudi table properties
            
        Returns:
            List of partition column names
        """
        # Hudi partition path field
        partition_path = properties.get("hoodie.table.partition.fields", "")
        
        if partition_path:
            # Can be comma-separated
            return [field.strip() for field in partition_path.split(",") if field.strip()]
        
        return []
    
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
