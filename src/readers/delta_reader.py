"""
Delta Lake Metadata Reader.

Reads and parses Delta Lake table metadata from S3.
Returns raw metadata without normalization.
"""

import json
import boto3
from typing import Dict, Optional, List
from botocore.exceptions import ClientError

from ..utils.logger import setup_logger
from ..utils.exceptions import MetadataReadError

logger = setup_logger(__name__)


class DeltaReader:
    """
    Reads Delta Lake table metadata.
    
    Delta Lake stores transaction logs in _delta_log/ directory.
    Each transaction is a JSON file numbered sequentially (00000.json, 00001.json, etc.).
    The _last_checkpoint file points to the latest checkpoint.
    """
    
    def __init__(self, aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 region_name: str = "us-east-1"):
        """
        Initialize the Delta Lake metadata reader.
        
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
        logger.info("DeltaReader initialized")
    
    def read_metadata(self, s3_path: str) -> Dict:
        """
        Read Delta Lake metadata from S3 path.
        
        Args:
            s3_path: S3 path to the Delta table (e.g., s3://bucket/warehouse/table)
            
        Returns:
            Dictionary containing raw Delta metadata with keys:
                - schema: Table schema (parsed from schemaString)
                - partition_columns: List of partition column names
                - version: Current table version
                - properties: Configuration properties
                - created_time: Table creation timestamp
                - supports_time_travel: Boolean indicating version support
                
        Raises:
            MetadataReadError: If metadata cannot be read
        """
        logger.info(f"Reading Delta metadata from: {s3_path}")
        
        try:
            bucket, prefix = self._parse_s3_path(s3_path)
            
            # Get the latest transaction log
            latest_version, log_entry = self._get_latest_log_entry(bucket, prefix)
            
            # Parse metadata from log entry
            raw_metadata = {
                "format": "DELTA",
                "location": s3_path,
                "version": latest_version,
                "schema": self._extract_schema(log_entry),
                "partition_columns": self._extract_partition_columns(log_entry),
                "properties": self._extract_properties(log_entry),
                "created_time": self._extract_created_time(log_entry),
                "supports_time_travel": True,  # Delta always supports time travel
                "protocol": self._extract_protocol(log_entry)
            }
            
            logger.info(f"Successfully read Delta metadata: version {latest_version}, "
                       f"{len(raw_metadata['schema'])} columns")
            
            return raw_metadata
            
        except MetadataReadError:
            raise
        except Exception as e:
            raise MetadataReadError(
                f"Failed to read Delta metadata from {s3_path}: {str(e)}",
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
    
    def _get_latest_log_entry(self, bucket: str, prefix: str) -> tuple[int, Dict]:
        """
        Get the latest Delta transaction log entry.
        
        Args:
            bucket: S3 bucket name
            prefix: Table prefix
            
        Returns:
            Tuple of (version_number, log_entry_dict)
        """
        delta_log_prefix = f"{prefix}_delta_log/"
        
        try:
            # List all JSON log files
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=delta_log_prefix
            )
            
            if 'Contents' not in response:
                raise MetadataReadError(
                    f"No transaction log files found in {delta_log_prefix}",
                    details={"bucket": bucket, "prefix": delta_log_prefix}
                )
            
            # Filter for transaction log JSON files (format: 00000000000000000000.json)
            log_files = [
                obj for obj in response['Contents']
                if obj['Key'].endswith('.json') and not obj['Key'].endswith('.checkpoint.json')
            ]
            
            if not log_files:
                raise MetadataReadError(
                    f"No valid transaction log files found in {delta_log_prefix}",
                    details={"bucket": bucket, "prefix": delta_log_prefix}
                )
            
            # Sort by file name (which contains version number) and get the latest
            log_files.sort(key=lambda x: x['Key'])
            latest_log = log_files[-1]
            
            # Extract version number from filename
            filename = latest_log['Key'].split('/')[-1]
            version = int(filename.replace('.json', ''))
            
            logger.debug(f"Found latest log file: {latest_log['Key']}, version: {version}")
            
            # Read and parse the log file
            log_content = self._read_s3_object(bucket, latest_log['Key'])
            
            # Delta log files contain one JSON object per line
            log_entries = [json.loads(line) for line in log_content.strip().split('\n') if line.strip()]
            
            # Merge all entries to get complete metadata
            merged_metadata = self._merge_log_entries(log_entries)
            
            return version, merged_metadata
            
        except ClientError as e:
            raise MetadataReadError(
                f"Failed to read Delta log files: {str(e)}",
                details={"bucket": bucket, "prefix": delta_log_prefix, "error": str(e)}
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
    
    def _merge_log_entries(self, log_entries: List[Dict]) -> Dict:
        """
        Merge multiple log entries into a single metadata dictionary.
        
        Delta log files can contain multiple action types: metaData, protocol, add, remove, etc.
        We're primarily interested in metaData action.
        
        Args:
            log_entries: List of parsed JSON log entries
            
        Returns:
            Merged metadata dictionary
        """
        merged = {}
        
        for entry in log_entries:
            if "metaData" in entry:
                merged.update(entry["metaData"])
            if "protocol" in entry:
                merged["protocol"] = entry["protocol"]
        
        return merged
    
    def _extract_schema(self, log_entry: Dict) -> List[Dict]:
        """
        Extract schema from Delta log entry.
        
        Schema is stored as a JSON string in the schemaString field.
        
        Args:
            log_entry: Parsed Delta log entry
            
        Returns:
            List of field dictionaries
        """
        schema_string = log_entry.get("schemaString", "{}")
        
        try:
            schema = json.loads(schema_string)
            fields = schema.get("fields", [])
            return fields
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse schema string: {e}")
            return []
    
    def _extract_partition_columns(self, log_entry: Dict) -> List[str]:
        """
        Extract partition column names from Delta log entry.
        
        Args:
            log_entry: Parsed Delta log entry
            
        Returns:
            List of partition column names
        """
        return log_entry.get("partitionColumns", [])
    
    def _extract_properties(self, log_entry: Dict) -> Dict[str, str]:
        """
        Extract configuration properties from Delta log entry.
        
        Args:
            log_entry: Parsed Delta log entry
            
        Returns:
            Dictionary of configuration properties
        """
        configuration = log_entry.get("configuration", {})
        
        # Add other metadata fields as properties
        properties = {**configuration}
        
        if "name" in log_entry:
            properties["table.name"] = log_entry["name"]
        if "description" in log_entry:
            properties["table.description"] = log_entry["description"]
        
        return properties
    
    def _extract_created_time(self, log_entry: Dict) -> Optional[int]:
        """
        Extract table creation time from Delta log entry.
        
        Args:
            log_entry: Parsed Delta log entry
            
        Returns:
            Creation timestamp in milliseconds, or None
        """
        return log_entry.get("createdTime")
    
    def _extract_protocol(self, log_entry: Dict) -> Optional[Dict]:
        """
        Extract protocol version information.
        
        Args:
            log_entry: Parsed Delta log entry
            
        Returns:
            Protocol dictionary or None
        """
        return log_entry.get("protocol")
