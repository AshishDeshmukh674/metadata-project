"""
Main entry point for the Unified Data Access Platform.

This module provides the core MetadataDiscoveryEngine that orchestrates
format detection, metadata reading, normalization, and storage.
"""

import sys
from typing import Optional

from .detectors.format_detector import FormatDetector, TableFormat
from .readers.iceberg_reader import IcebergReader
from .readers.delta_reader import DeltaReader
from .readers.hudi_reader import HudiReader
from .normalizer.metadata_normalizer import MetadataNormalizer
from .storage.metadata_store import MetadataStore
from .models.table_metadata import TableMetadata
from .utils.logger import setup_logger
from .utils.exceptions import (
    PlatformException,
    FormatDetectionError,
    MetadataReadError,
    NormalizationError,
    StorageError
)

logger = setup_logger(__name__)


class MetadataDiscoveryEngine:
    """
    Core engine for discovering and processing table metadata.
    
    Workflow:
    1. Detect table format (Iceberg, Delta, Hudi)
    2. Read format-specific metadata
    3. Normalize to unified schema
    4. Store in metadata database
    """
    
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1",
        db_path: str = "metadata.db"
    ):
        """
        Initialize the metadata discovery engine.
        
        Args:
            aws_access_key_id: AWS access key (optional)
            aws_secret_access_key: AWS secret key (optional)
            region_name: AWS region
            db_path: Path to SQLite database
        """
        logger.info("Initializing MetadataDiscoveryEngine")
        
        # Initialize components
        self.format_detector = FormatDetector(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        
        self.iceberg_reader = IcebergReader(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        
        self.delta_reader = DeltaReader(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        
        self.hudi_reader = HudiReader(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        
        self.normalizer = MetadataNormalizer()
        self.metadata_store = MetadataStore(db_path=db_path)
        
        logger.info("MetadataDiscoveryEngine initialized successfully")
    
    def discover_and_store(self, s3_path: str) -> TableMetadata:
        """
        Complete metadata discovery workflow.
        
        Args:
            s3_path: S3 path to the table
            
        Returns:
            Normalized and stored TableMetadata
            
        Raises:
            PlatformException: If any step fails
        """
        logger.info(f"Starting metadata discovery for: {s3_path}")
        
        try:
            # Step 1: Detect format
            table_format = self.format_detector.detect_format(s3_path)
            logger.info(f"Detected format: {table_format}")
            
            # Step 2: Read format-specific metadata
            raw_metadata = self._read_metadata(s3_path, table_format)
            logger.info(f"Read raw metadata from {table_format} table")
            
            # Step 3: Normalize to unified schema
            normalized_metadata = self.normalizer.normalize(raw_metadata, table_format.value)
            logger.info(f"Normalized metadata to unified schema")
            
            # Step 4: Store in database
            table_id = self.metadata_store.save_table_metadata(normalized_metadata)
            logger.info(f"Stored metadata in database (table_id={table_id})")
            
            logger.info(f"Successfully completed metadata discovery for: {normalized_metadata.table_name}")
            return normalized_metadata
            
        except (FormatDetectionError, MetadataReadError, NormalizationError, StorageError) as e:
            logger.error(f"Metadata discovery failed: {e.message}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during metadata discovery: {str(e)}")
            raise PlatformException(
                f"Metadata discovery failed: {str(e)}",
                details={"s3_path": s3_path, "error": str(e)}
            )
    
    def _read_metadata(self, s3_path: str, table_format: TableFormat) -> dict:
        """
        Read metadata using appropriate reader based on format.
        
        Args:
            s3_path: S3 path to the table
            table_format: Detected table format
            
        Returns:
            Raw metadata dictionary
        """
        if table_format == TableFormat.ICEBERG:
            return self.iceberg_reader.read_metadata(s3_path)
        elif table_format == TableFormat.DELTA:
            return self.delta_reader.read_metadata(s3_path)
        elif table_format == TableFormat.HUDI:
            return self.hudi_reader.read_metadata(s3_path)
        else:
            raise MetadataReadError(
                f"Unsupported table format: {table_format}",
                details={"format": table_format}
            )
    
    def get_table_metadata(self, table_name: str) -> Optional[TableMetadata]:
        """
        Retrieve stored table metadata by name.
        
        Args:
            table_name: Name of the table
            
        Returns:
            TableMetadata or None if not found
        """
        return self.metadata_store.get_table_metadata(table_name)
    
    def list_tables(self, format_filter: Optional[str] = None) -> list[str]:
        """
        List all stored tables.
        
        Args:
            format_filter: Optional format filter (ICEBERG, DELTA, HUDI)
            
        Returns:
            List of table names
        """
        return self.metadata_store.list_tables(format_filter=format_filter)
    
    def delete_table(self, table_name: str) -> bool:
        """
        Delete stored table metadata.
        
        Args:
            table_name: Name of the table
            
        Returns:
            True if deleted, False if not found
        """
        return self.metadata_store.delete_table_metadata(table_name)


def main():
    """
    CLI entry point for the platform.
    
    Usage examples:
        python -m src.main discover s3://bucket/warehouse/table
        python -m src.main list
        python -m src.main get table_name
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Unified Data Access Platform - Metadata Discovery Engine"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Discover command
    discover_parser = subparsers.add_parser("discover", help="Discover and store table metadata")
    discover_parser.add_argument("s3_path", help="S3 path to the table")
    discover_parser.add_argument("--db-path", default="metadata.db", help="Database path")
    
    # List command
    list_parser = subparsers.add_parser("list", help="List all tables")
    list_parser.add_argument("--format", choices=["ICEBERG", "DELTA", "HUDI"], help="Filter by format")
    list_parser.add_argument("--db-path", default="metadata.db", help="Database path")
    
    # Get command
    get_parser = subparsers.add_parser("get", help="Get table metadata")
    get_parser.add_argument("table_name", help="Name of the table")
    get_parser.add_argument("--db-path", default="metadata.db", help="Database path")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    try:
        engine = MetadataDiscoveryEngine(db_path=args.db_path)
        
        if args.command == "discover":
            metadata = engine.discover_and_store(args.s3_path)
            print(f"\n✓ Successfully discovered metadata for: {metadata.table_name}")
            print(f"  Format: {metadata.format}")
            print(f"  Columns: {len(metadata.columns)}")
            print(f"  Partitions: {len(metadata.partitions)}")
            print(f"  Time Travel: {metadata.supports_time_travel}")
            
        elif args.command == "list":
            tables = engine.list_tables(format_filter=args.format)
            print(f"\nFound {len(tables)} table(s):")
            for table in tables:
                print(f"  - {table}")
            
        elif args.command == "get":
            metadata = engine.get_table_metadata(args.table_name)
            if metadata:
                print(f"\nTable: {metadata.table_name}")
                print(f"Format: {metadata.format}")
                print(f"Location: {metadata.location}")
                print(f"\nColumns ({len(metadata.columns)}):")
                for col in metadata.columns:
                    nullable = "NULL" if col.nullable else "NOT NULL"
                    print(f"  - {col.name}: {col.data_type} {nullable}")
                if metadata.partitions:
                    print(f"\nPartitions: {', '.join(metadata.partitions)}")
                print(f"\nTime Travel: {metadata.supports_time_travel}")
            else:
                print(f"Table not found: {args.table_name}")
                sys.exit(1)
        
    except PlatformException as e:
        print(f"\n✗ Error: {e.message}", file=sys.stderr)
        if e.details:
            print(f"  Details: {e.details}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
