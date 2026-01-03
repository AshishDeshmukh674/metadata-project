"""
Parquet Service
===============
Execute SQL queries on Parquet files using DuckDB.
"""

import duckdb
import pandas as pd
import logging
from typing import Dict, Any, Optional
from io import BytesIO
import boto3
from datetime import datetime

from app.models.user_credentials import UserAWSCredentials

logger = logging.getLogger(__name__)


class ParquetService:
    """Service for executing SQL on Parquet files using DuckDB."""
    
    def __init__(self):
        """Initialize Parquet service."""
        logger.info("✅ Parquet service initialized")
    
    def execute_sql_on_s3_parquet(
        self,
        s3_path: str,
        sql_query: str,
        credentials: UserAWSCredentials,
        operation_type: str = "SELECT"
    ) -> Dict[str, Any]:
        """
        Execute SQL query on S3 Parquet file.
        
        Args:
            s3_path: S3 URI to Parquet file (e.g., s3://bucket/path/file.parquet)
            sql_query: SQL query to execute (table is referenced as 'data_table')
            credentials: User AWS credentials
            operation_type: Type of SQL operation (SELECT, UPDATE, DELETE, INSERT)
            
        Returns:
            Dict with:
                - success: bool
                - result_data: DataFrame or None (for SELECT queries)
                - rows_affected: int
                - execution_time_ms: float
                - message: str
        """
        start_time = datetime.now()
        
        try:
            logger.info(f"📊 Executing {operation_type} query on {s3_path}")
            
            # Create DuckDB connection
            conn = duckdb.connect(database=':memory:')
            
            # Configure AWS credentials for DuckDB
            self._configure_s3_credentials(conn, credentials)
            
            # Load Parquet file from S3 into DuckDB
            logger.info(f"📥 Loading Parquet from S3...")
            conn.execute(f"""
                CREATE TABLE data_table AS 
                SELECT * FROM read_parquet('{s3_path}')
            """)
            
            # Get initial row count
            initial_count = conn.execute("SELECT COUNT(*) FROM data_table").fetchone()[0]
            logger.info(f"📋 Loaded {initial_count:,} rows")
            
            # Execute the user's SQL query
            logger.info(f"⚡ Executing SQL: {sql_query[:100]}...")
            
            # Get results based on operation type
            if operation_type.upper() == "SELECT":
                # For SELECT, execute query and fetch results directly
                result_df = conn.execute(sql_query).fetchdf()
                rows_affected = len(result_df)
                logger.info(f"✅ Query returned {rows_affected:,} rows")
                
                result = {
                    "success": True,
                    "result_data": result_df,
                    "rows_affected": rows_affected,
                    "message": f"Query executed successfully. Returned {rows_affected:,} rows."
                }
            else:
                # For UPDATE/DELETE/INSERT, execute the modifying query
                conn.execute(sql_query)
                
                # Check modified data
                final_count = conn.execute("SELECT COUNT(*) FROM data_table").fetchone()[0]
                rows_affected = abs(final_count - initial_count)
                
                # Get the modified data
                result_df = conn.execute("SELECT * FROM data_table").fetchdf()
                
                logger.info(f"✅ {operation_type} affected {rows_affected:,} rows")
                
                result = {
                    "success": True,
                    "result_data": result_df,
                    "rows_affected": rows_affected,
                    "initial_rows": initial_count,
                    "final_rows": final_count,
                    "message": f"{operation_type} executed successfully. {rows_affected:,} rows affected."
                }
            
            # Calculate execution time
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            result["execution_time_ms"] = round(execution_time, 2)
            
            conn.close()
            return result
            
        except Exception as e:
            logger.error(f"❌ SQL execution failed: {str(e)}")
            return {
                "success": False,
                "result_data": None,
                "rows_affected": 0,
                "execution_time_ms": (datetime.now() - start_time).total_seconds() * 1000,
                "message": f"SQL execution failed: {str(e)}"
            }
    
    def _configure_s3_credentials(
        self,
        conn: duckdb.DuckDBPyConnection,
        credentials: UserAWSCredentials
    ):
        """Configure S3 credentials for DuckDB."""
        try:
            conn.execute(f"""
                SET s3_access_key_id='{credentials.aws_access_key_id}';
            """)
            conn.execute(f"""
                SET s3_secret_access_key='{credentials.aws_secret_access_key}';
            """)
            conn.execute(f"""
                SET s3_region='{credentials.aws_region}';
            """)
            
            if credentials.aws_session_token:
                conn.execute(f"""
                    SET s3_session_token='{credentials.aws_session_token}';
                """)
            
            logger.debug("✅ S3 credentials configured in DuckDB")
            
        except Exception as e:
            logger.error(f"❌ Failed to configure S3 credentials: {str(e)}")
            raise
    
    def write_dataframe_to_s3(
        self,
        df: pd.DataFrame,
        s3_path: str,
        credentials: UserAWSCredentials
    ) -> Dict[str, Any]:
        """
        Write DataFrame back to S3 as Parquet.
        
        Args:
            df: DataFrame to write
            s3_path: S3 URI (e.g., s3://bucket/path/file.parquet)
            credentials: User AWS credentials
            
        Returns:
            Dict with success status and message
        """
        try:
            logger.info(f"📤 Writing {len(df):,} rows to {s3_path}")
            
            # Parse S3 path
            bucket, key = self._parse_s3_path(s3_path)
            
            # Create S3 client
            s3_client = boto3.client(
                's3',
                aws_access_key_id=credentials.aws_access_key_id,
                aws_secret_access_key=credentials.aws_secret_access_key,
                aws_session_token=credentials.aws_session_token,
                region_name=credentials.aws_region
            )
            
            # Write DataFrame to Parquet in memory
            buffer = BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow')
            buffer.seek(0)
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            file_size_mb = len(buffer.getvalue()) / (1024 * 1024)
            logger.info(f"✅ Successfully wrote {file_size_mb:.2f} MB to S3")
            
            return {
                "success": True,
                "message": f"Successfully wrote {len(df):,} rows ({file_size_mb:.2f} MB) to {s3_path}",
                "rows_written": len(df),
                "file_size_mb": round(file_size_mb, 2)
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to write to S3: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to write to S3: {str(e)}"
            }
    
    def create_backup(
        self,
        s3_path: str,
        credentials: UserAWSCredentials
    ) -> Dict[str, Any]:
        """
        Create a backup of the Parquet file before modification.
        
        Args:
            s3_path: Original S3 path
            credentials: User AWS credentials
            
        Returns:
            Dict with backup path and status
        """
        try:
            # Parse S3 path
            bucket, key = self._parse_s3_path(s3_path)
            
            # Generate backup key with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_key = key.replace(".parquet", f"_backup_{timestamp}.parquet")
            
            # If key doesn't end with .parquet, just append
            if not key.endswith(".parquet"):
                backup_key = f"{key}_backup_{timestamp}.parquet"
            
            backup_path = f"s3://{bucket}/{backup_key}"
            
            logger.info(f"💾 Creating backup: {backup_path}")
            
            # Create S3 client
            s3_client = boto3.client(
                's3',
                aws_access_key_id=credentials.aws_access_key_id,
                aws_secret_access_key=credentials.aws_secret_access_key,
                aws_session_token=credentials.aws_session_token,
                region_name=credentials.aws_region
            )
            
            # Copy object
            s3_client.copy_object(
                CopySource={'Bucket': bucket, 'Key': key},
                Bucket=bucket,
                Key=backup_key
            )
            
            logger.info(f"✅ Backup created successfully")
            
            return {
                "success": True,
                "backup_path": backup_path,
                "message": f"Backup created at {backup_path}"
            }
            
        except Exception as e:
            logger.error(f"❌ Backup failed: {str(e)}")
            return {
                "success": False,
                "backup_path": None,
                "message": f"Backup failed: {str(e)}"
            }
    
    def _parse_s3_path(self, s3_path: str) -> tuple[str, str]:
        """
        Parse S3 URI into bucket and key.
        
        Args:
            s3_path: S3 URI (e.g., s3://bucket/path/to/file.parquet)
            
        Returns:
            Tuple of (bucket, key)
        """
        if not s3_path.startswith("s3://"):
            raise ValueError("S3 path must start with s3://")
        
        path_parts = s3_path[5:].split("/", 1)
        bucket = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ""
        
        return bucket, key
    
    def preview_changes(
        self,
        s3_path: str,
        sql_query: str,
        credentials: UserAWSCredentials,
        sample_size: int = 10
    ) -> Dict[str, Any]:
        """
        Preview what changes a query would make without executing it.
        
        Args:
            s3_path: S3 URI to Parquet file
            sql_query: SQL query to preview
            credentials: User AWS credentials
            sample_size: Number of sample rows to show
            
        Returns:
            Dict with preview information
        """
        try:
            conn = duckdb.connect(database=':memory:')
            self._configure_s3_credentials(conn, credentials)
            
            # Load data
            conn.execute(f"CREATE TABLE data_table AS SELECT * FROM read_parquet('{s3_path}')")
            
            # Get sample of rows that would be affected
            if "WHERE" in sql_query.upper():
                # Extract WHERE clause
                where_clause = sql_query.upper().split("WHERE")[1].strip()
                preview_query = f"SELECT * FROM data_table WHERE {where_clause} LIMIT {sample_size}"
                affected_rows = conn.execute(preview_query).fetchdf()
            else:
                # No WHERE clause - show random sample
                affected_rows = conn.execute(f"SELECT * FROM data_table LIMIT {sample_size}").fetchdf()
            
            # Count total affected
            total_count = conn.execute("SELECT COUNT(*) FROM data_table").fetchone()[0]
            
            conn.close()
            
            return {
                "success": True,
                "sample_rows": affected_rows,
                "total_rows": total_count,
                "preview_size": len(affected_rows),
                "message": f"Preview shows {len(affected_rows)} of approximately {total_count:,} rows"
            }
            
        except Exception as e:
            logger.error(f"❌ Preview failed: {str(e)}")
            return {
                "success": False,
                "message": f"Preview failed: {str(e)}"
            }


# Global service instance
parquet_service = ParquetService()
