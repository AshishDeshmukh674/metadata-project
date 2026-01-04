"""
Spark Parquet Service
=====================

PARQUET + SPARK = PERFECT MATCH
--------------------------------
Parquet is a columnar format designed for Spark/Hadoop ecosystems.
Spark can:
1. Read only needed columns (column pruning)
2. Skip irrelevant row groups (predicate pushdown)
3. Read multiple files in parallel
4. Handle files larger than memory

WHEN TO USE SPARK FOR PARQUET:
-------------------------------
✅ Files > 100 MB
✅ Need to process multiple files at once
✅ Complex transformations (joins, aggregations)
✅ Want to write back modified data
❌ Simple queries on small files → Use DuckDB instead
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from app.services.spark_base import SparkServiceBase
from app.models.user_credentials import UserAWSCredentials

logger = logging.getLogger(__name__)


class SparkParquetService(SparkServiceBase):
    """
    Service for processing Parquet files using Apache Spark.
    
    Provides:
    - Read/Write Parquet files from/to S3
    - Execute SQL queries with Spark SQL
    - Modify data and persist changes
    - Handle large files efficiently
    """
    
    def __init__(self):
        """Initialize Spark Parquet service."""
        super().__init__(service_name="SparkParquetService")
    
    def read_and_query(
        self,
        bucket: str,
        key: str,
        credentials: UserAWSCredentials,
        sql_query: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Read Parquet file and optionally query it.
        
        PIPELINE:
        ---------
        1. Configure AWS credentials
        2. Read Parquet from S3 (lazy - no data loaded yet)
        3. Apply column selection if specified
        4. Apply filters if specified
        5. Execute SQL if provided
        6. Collect results (action - triggers execution)
        
        Args:
            bucket: S3 bucket name
            key: S3 object key (path to Parquet file or directory)
            credentials: AWS credentials
            sql_query: Optional SQL query (use 'data_table' as table name)
            filters: Optional filters {"column": value}
            columns: Optional columns to select
            limit: Optional row limit
            
        Returns:
            Dict with:
                - success: bool
                - data: List of row dicts
                - schema: Table schema
                - stats: Statistics
                - execution_time_ms: Execution time
        """
        start_time = time.time()
        
        try:
            logger.info(f"📦 Processing Parquet: s3://{bucket}/{key}")
            
            # Configure credentials
            self._configure_credentials(credentials)
            
            # Build S3 path
            s3_path = self._build_s3_path(bucket, key)
            
            # STEP 1: READ (Lazy - builds plan only)
            df = self.read_parquet(s3_path, columns=columns)
            
            # STEP 2: APPLY FILTERS (Lazy - adds to plan)
            if filters:
                logger.info(f"🔍 Applying filters: {filters}")
                df = self.filter_data(df, filters)
            
            # STEP 3: EXECUTE SQL IF PROVIDED (Lazy - modifies plan)
            if sql_query:
                logger.info(f"⚡ Executing SQL query")
                df = self.execute_sql(df, sql_query, table_name="data_table")
            
            # STEP 4: APPLY LIMIT (Lazy)
            if limit:
                logger.info(f"📊 Limiting to {limit} rows")
                df = df.limit(limit)
            
            # STEP 5: GET SCHEMA (Quick - no data loaded)
            schema = [
                {"name": field.name, "type": str(field.dataType)}
                for field in df.schema.fields
            ]
            
            # STEP 6: COLLECT SAMPLE DATA (ACTION - triggers execution!)
            logger.info("🚀 Executing Spark job...")
            sample_limit = limit if limit else 1000
            sample_data = self.get_sample_data(df, num_rows=sample_limit)
            
            # STEP 7: GET FULL COUNT (another ACTION)
            if not limit:
                total_rows = df.count()
            else:
                total_rows = len(sample_data)
            
            execution_time = (time.time() - start_time) * 1000
            
            result = {
                "success": True,
                "data": sample_data,
                "schema": schema,
                "stats": {
                    "total_rows": total_rows,
                    "rows_returned": len(sample_data),
                    "num_columns": len(schema),
                    "file_path": s3_path
                },
                "execution_time_ms": execution_time,
                "message": f"Query executed successfully in {execution_time:.0f}ms"
            }
            
            logger.info(f"✅ Query completed: {total_rows:,} rows, {len(schema)} columns")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error processing Parquet: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to process Parquet file: {str(e)}",
                "execution_time_ms": (time.time() - start_time) * 1000
            }
    
    def modify_and_write_back(
        self,
        bucket: str,
        key: str,
        credentials: UserAWSCredentials,
        sql_modification: str,
        output_key: Optional[str] = None,
        write_mode: str = "overwrite",
        partition_by: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Read Parquet, modify data, and write back to S3.
        
        THIS IS THE KEY DIFFERENCE FROM DUCKDB:
        ----------------------------------------
        With Spark, we can:
        1. Modify data (UPDATE/DELETE/INSERT)
        2. Write changes back to S3
        3. Persist the modifications
        
        IMPORTANT: This creates a NEW Parquet file!
        Parquet files are immutable, so we write to a new location.
        
        PIPELINE:
        ---------
        1. Read original Parquet
        2. Apply SQL modifications
        3. Write modified data as new Parquet file
        4. Optionally overwrite original
        
        Args:
            bucket: S3 bucket
            key: Input Parquet path
            credentials: AWS credentials
            sql_modification: SQL to modify data (e.g., UPDATE, DELETE)
            output_key: Output path (default: overwrites input)
            write_mode: Write mode (overwrite, append, error)
            partition_by: Columns to partition by
            
        Returns:
            Dict with success status and statistics
            
        Example SQL:
        ------------
        # UPDATE
        "UPDATE data_table SET salary = salary * 1.1 WHERE department = 'Engineering'"
        
        # DELETE
        "DELETE FROM data_table WHERE age < 18"
        
        # Complex transformation
        "SELECT *, CASE WHEN salary > 100000 THEN 'HIGH' ELSE 'LOW' END as salary_band FROM data_table"
        """
        start_time = time.time()
        
        try:
            logger.info(f"🔧 Modifying Parquet: s3://{bucket}/{key}")
            logger.info(f"   SQL: {sql_modification}")
            
            # Configure credentials
            self._configure_credentials(credentials)
            
            # Build paths
            input_path = self._build_s3_path(bucket, key)
            output_path = self._build_s3_path(
                bucket, 
                output_key if output_key else f"{key}.modified"
            )
            
            # STEP 1: READ
            df = self.read_parquet(input_path)
            initial_count = df.count()
            logger.info(f"📊 Initial row count: {initial_count:,}")
            
            # STEP 2: APPLY MODIFICATIONS
            logger.info(f"⚡ Applying modifications...")
            modified_df = self.execute_sql(df, sql_modification, table_name="data_table")
            
            # STEP 3: CHECK RESULTS
            final_count = modified_df.count()
            rows_affected = abs(final_count - initial_count)
            logger.info(f"📊 Final row count: {final_count:,} ({rows_affected:,} affected)")
            
            # STEP 4: WRITE BACK TO S3
            logger.info(f"💾 Writing to {output_path}...")
            write_result = self.write_parquet(
                df=modified_df,
                s3_path=output_path,
                mode=write_mode,
                partition_by=partition_by,
                compression="snappy"
            )
            
            execution_time = (time.time() - start_time) * 1000
            
            result = {
                "success": True,
                "initial_rows": initial_count,
                "final_rows": final_count,
                "rows_affected": rows_affected,
                "output_path": output_path,
                "write_mode": write_mode,
                "execution_time_ms": execution_time,
                "message": f"Successfully modified and wrote {final_count:,} rows"
            }
            
            logger.info(f"✅ Modification completed in {execution_time:.0f}ms")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error modifying Parquet: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to modify Parquet file: {str(e)}",
                "execution_time_ms": (time.time() - start_time) * 1000
            }
    
    def get_metadata(
        self,
        bucket: str,
        key: str,
        credentials: UserAWSCredentials,
        include_stats: bool = True
    ) -> Dict[str, Any]:
        """
        Get metadata about Parquet file without reading all data.
        
        FAST METADATA EXTRACTION:
        -------------------------
        Spark can read Parquet metadata without loading data:
        - Schema (column names and types)
        - Row count (from footer)
        - Partition information
        - File sizes
        
        Args:
            bucket: S3 bucket
            key: Parquet file path
            credentials: AWS credentials
            include_stats: Whether to compute statistics
            
        Returns:
            Dict with metadata
        """
        try:
            logger.info(f"📋 Getting metadata for: s3://{bucket}/{key}")
            
            self._configure_credentials(credentials)
            s3_path = self._build_s3_path(bucket, key)
            
            # Read schema only (fast!)
            df = self.read_parquet(s3_path)
            
            # Get schema
            schema = [
                {
                    "name": field.name,
                    "type": str(field.dataType),
                    "nullable": field.nullable
                }
                for field in df.schema.fields
            ]
            
            metadata = {
                "success": True,
                "file_path": s3_path,
                "num_columns": len(schema),
                "schema": schema
            }
            
            # Get statistics if requested (requires reading data)
            if include_stats:
                logger.info("📊 Computing statistics...")
                stats = self.get_dataframe_stats(df)
                metadata.update(stats)
            
            logger.info(f"✅ Metadata retrieved: {len(schema)} columns")
            return metadata
            
        except Exception as e:
            logger.error(f"❌ Error getting metadata: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to get metadata: {str(e)}"
            }
    
    def merge_multiple_files(
        self,
        bucket: str,
        keys: List[str],
        credentials: UserAWSCredentials,
        output_key: str,
        deduplicate_on: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Merge multiple Parquet files into one.
        
        USE CASE:
        ---------
        You have multiple Parquet files and want to combine them:
        - Daily files → Monthly file
        - Sharded data → Single file
        - Multiple sources → Unified dataset
        
        SPARK ADVANTAGE:
        ----------------
        Spark reads all files in parallel and combines them efficiently.
        
        Args:
            bucket: S3 bucket
            keys: List of Parquet file paths to merge
            credentials: AWS credentials
            output_key: Output path for merged file
            deduplicate_on: Optional columns for deduplication
            
        Returns:
            Dict with merge results
        """
        start_time = time.time()
        
        try:
            logger.info(f"🔀 Merging {len(keys)} Parquet files")
            
            self._configure_credentials(credentials)
            
            # Read all files (Spark does this in parallel!)
            dfs = []
            for key in keys:
                s3_path = self._build_s3_path(bucket, key)
                logger.info(f"   Reading: {s3_path}")
                df = self.read_parquet(s3_path)
                dfs.append(df)
            
            # Union all DataFrames
            merged_df = dfs[0]
            for df in dfs[1:]:
                merged_df = merged_df.union(df)
            
            # Deduplicate if requested
            if deduplicate_on:
                logger.info(f"🔍 Deduplicating on columns: {deduplicate_on}")
                merged_df = merged_df.dropDuplicates(deduplicate_on)
            
            # Count results
            total_rows = merged_df.count()
            logger.info(f"📊 Merged result: {total_rows:,} rows")
            
            # Write merged file
            output_path = self._build_s3_path(bucket, output_key)
            logger.info(f"💾 Writing merged file to {output_path}")
            
            write_result = self.write_parquet(
                df=merged_df,
                s3_path=output_path,
                mode="overwrite",
                compression="snappy"
            )
            
            execution_time = (time.time() - start_time) * 1000
            
            result = {
                "success": True,
                "files_merged": len(keys),
                "total_rows": total_rows,
                "output_path": output_path,
                "execution_time_ms": execution_time,
                "message": f"Successfully merged {len(keys)} files into {total_rows:,} rows"
            }
            
            logger.info(f"✅ Merge completed in {execution_time:.0f}ms")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error merging files: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to merge files: {str(e)}",
                "execution_time_ms": (time.time() - start_time) * 1000
            }


# Global service instance
spark_parquet_service = SparkParquetService()
