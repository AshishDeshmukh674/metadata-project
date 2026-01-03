"""
Delta Lake Service
==================
Handle Delta Lake table operations.
"""

import logging
from typing import Dict, Any
import pandas as pd
from deltalake import DeltaTable, write_deltalake
from datetime import datetime

from app.models.user_credentials import UserAWSCredentials

logger = logging.getLogger(__name__)


class DeltaService:
    """Service for Delta Lake table operations."""
    
    def __init__(self):
        """Initialize Delta service."""
        logger.info("✅ Delta Lake service initialized")
    
    def read_delta_table(
        self,
        s3_path: str,
        credentials: UserAWSCredentials
    ) -> Dict[str, Any]:
        """
        Read Delta Lake table and return as DataFrame.
        
        Args:
            s3_path: S3 path to Delta table (e.g., s3://bucket/path/delta_table)
            credentials: User AWS credentials
            
        Returns:
            Dict with DataFrame and metadata
        """
        try:
            logger.info(f"📊 Reading Delta table from {s3_path}")
            
            # Configure S3 storage options
            storage_options = {
                "AWS_ACCESS_KEY_ID": credentials.aws_access_key_id,
                "AWS_SECRET_ACCESS_KEY": credentials.aws_secret_access_key,
                "AWS_REGION": credentials.aws_region,
            }
            
            if credentials.aws_session_token:
                storage_options["AWS_SESSION_TOKEN"] = credentials.aws_session_token
            
            # Load Delta table
            dt = DeltaTable(s3_path, storage_options=storage_options)
            
            # Get table data
            df = dt.to_pandas()
            
            # Get metadata
            metadata = {
                "version": dt.version(),
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "schema": [{"name": col, "type": str(df[col].dtype)} for col in df.columns]
            }
            
            logger.info(f"✅ Loaded Delta table: {len(df)} rows, version {dt.version()}")
            
            return {
                "success": True,
                "data": df,
                "metadata": metadata
            }
            
        except Exception as e:
            logger.error(f"❌ Error reading Delta table: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to read Delta table: {str(e)}"
            }
    
    def execute_on_delta(
        self,
        s3_path: str,
        df: pd.DataFrame,
        credentials: UserAWSCredentials,
        mode: str = "overwrite"
    ) -> Dict[str, Any]:
        """
        Write DataFrame back to Delta table.
        
        Args:
            s3_path: S3 path to Delta table
            df: Modified DataFrame to write
            credentials: User AWS credentials
            mode: Write mode ('overwrite', 'append', 'error', 'ignore')
            
        Returns:
            Dict with success status
        """
        start_time = datetime.now()
        
        try:
            logger.info(f"📤 Writing {len(df)} rows to Delta table at {s3_path}")
            
            # Configure S3 storage options
            storage_options = {
                "AWS_ACCESS_KEY_ID": credentials.aws_access_key_id,
                "AWS_SECRET_ACCESS_KEY": credentials.aws_secret_access_key,
                "AWS_REGION": credentials.aws_region,
            }
            
            if credentials.aws_session_token:
                storage_options["AWS_SESSION_TOKEN"] = credentials.aws_session_token
            
            # Write to Delta table
            write_deltalake(
                s3_path,
                df,
                mode=mode,
                storage_options=storage_options
            )
            
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            logger.info(f"✅ Successfully wrote to Delta table")
            
            return {
                "success": True,
                "message": f"Successfully wrote {len(df)} rows to Delta table",
                "rows_affected": len(df),
                "execution_time_ms": round(execution_time, 2)
            }
            
        except Exception as e:
            logger.error(f"❌ Error writing to Delta table: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to write to Delta table: {str(e)}"
            }


# Global service instance
delta_service = DeltaService()
