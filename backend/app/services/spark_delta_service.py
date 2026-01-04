"""
Spark Delta Lake Service
========================

DELTA LAKE + SPARK = ACID TRANSACTIONS
---------------------------------------
Delta Lake was built for Spark and provides:
1. ACID transactions (Atomicity, Consistency, Isolation, Durability)
2. Time travel (query historical versions)
3. Schema evolution (safely add/modify columns)
4. Upserts and deletes (MERGE operations)
5. Metadata-only operations (fast!)

This is what you SHOULD use for data that changes frequently!
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta import DeltaTable

from app.services.spark_base import SparkServiceBase
from app.models.user_credentials import UserAWSCredentials

logger = logging.getLogger(__name__)


class SparkDeltaService(SparkServiceBase):
    """
    Service for Delta Lake operations using Apache Spark.
    
    Delta Lake Features:
    - ACID transactions (guaranteed consistency)
    - Time travel (access historical data)
    - Schema enforcement and evolution
    - Efficient upserts/deletes
    - Audit history
    """
    
    def __init__(self):
        """Initialize Spark Delta service."""
        super().__init__(service_name="SparkDeltaService")
    
    def read_delta_table(
        self,
        bucket: str,
        table_path: str,
        credentials: UserAWSCredentials,
        version: Optional[int] = None,
        timestamp: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Read Delta table from S3.
        
        TIME TRAVEL:
        ------------
        Delta Lake keeps history of all changes!
        - version=5: Read table at version 5
        - timestamp="2024-01-01": Read table as it was on that date
        
        PIPELINE:
        ---------
        1. Read Delta table (optionally at specific version)
        2. Apply filters
        3. Return data and metadata
        
        Args:
            bucket: S3 bucket
            table_path: Path to Delta table
            credentials: AWS credentials
            version: Optional version number for time travel
            timestamp: Optional timestamp for time travel
            filters: Optional filters
            
        Returns:
            Dict with data and metadata
        """
        start_time = time.time()
        
        try:
            logger.info(f"🔺 Reading Delta table: s3://{bucket}/{table_path}")
            
            self._configure_credentials(credentials)
            s3_path = self._build_s3_path(bucket, table_path)
            
            spark = self._get_spark_session()
            
            # Read Delta table with time travel
            reader = spark.read.format("delta")
            
            if version is not None:
                logger.info(f"⏱️ Time travel: version {version}")
                reader = reader.option("versionAsOf", version)
            elif timestamp:
                logger.info(f"⏱️ Time travel: timestamp {timestamp}")
                reader = reader.option("timestampAsOf", timestamp)
            
            df = reader.load(s3_path)
            
            # Apply filters if specified
            if filters:
                logger.info(f"🔍 Applying filters: {filters}")
                df = self.filter_data(df, filters)
            
            # Get Delta table metadata
            delta_table = DeltaTable.forPath(spark, s3_path)
            history = delta_table.history(1).collect()[0]
            
            # Get schema
            schema = [
                {
                    "name": field.name,
                    "type": str(field.dataType),
                    "nullable": field.nullable
                }
                for field in df.schema.fields
            ]
            
            # Get sample data
            sample_data = self.get_sample_data(df, num_rows=100)
            total_rows = df.count()
            
            execution_time = (time.time() - start_time) * 1000
            
            result = {
                "success": True,
                "data": sample_data,
                "schema": schema,
                "stats": {
                    "total_rows": total_rows,
                    "rows_returned": len(sample_data),
                    "num_columns": len(schema),
                    "current_version": history["version"],
                    "last_modified": str(history["timestamp"]),
                    "operation": history["operation"]
                },
                "execution_time_ms": execution_time,
                "message": f"Read Delta table successfully: {total_rows:,} rows"
            }
            
            logger.info(f"✅ Delta read completed in {execution_time:.0f}ms")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error reading Delta table: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to read Delta table: {str(e)}",
                "execution_time_ms": (time.time() - start_time) * 1000
            }
    
    def update_delta_table(
        self,
        bucket: str,
        table_path: str,
        credentials: UserAWSCredentials,
        update_expr: Dict[str, str],
        condition: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update rows in Delta table (ACID transaction).
        
        THIS IS THE MAGIC OF DELTA LAKE:
        ---------------------------------
        Unlike Parquet, Delta can do TRUE updates:
        1. Transaction log tracks changes
        2. Only modified data is rewritten
        3. Old versions are kept for time travel
        4. Atomic - either all succeed or all fail
        
        Args:
            bucket: S3 bucket
            table_path: Path to Delta table
            credentials: AWS credentials
            update_expr: Dict of column: new_value_expression
            condition: SQL WHERE condition for which rows to update
            
        Returns:
            Dict with update statistics
            
        Example:
        --------
        # Increase all Engineering salaries by 10%
        update_delta_table(
            bucket="my-bucket",
            table_path="employees",
            update_expr={"salary": "salary * 1.1"},
            condition="department = 'Engineering'"
        )
        """
        start_time = time.time()
        
        try:
            logger.info(f"✏️ Updating Delta table: s3://{bucket}/{table_path}")
            logger.info(f"   Update: {update_expr}")
            logger.info(f"   Condition: {condition}")
            
            self._configure_credentials(credentials)
            s3_path = self._build_s3_path(bucket, table_path)
            
            spark = self._get_spark_session()
            
            # Load Delta table
            delta_table = DeltaTable.forPath(spark, s3_path)
            
            # Count before update
            before_count = delta_table.toDF().count()
            
            # Build update operation
            update_builder = delta_table.update()
            
            if condition:
                update_builder = update_builder.where(condition)
            
            # Execute update (ACID transaction!)
            update_builder.set(update_expr).execute()
            
            # Get update metrics
            history = delta_table.history(1).collect()[0]
            operation_metrics = history["operationMetrics"]
            
            rows_updated = int(operation_metrics.get("numUpdatedRows", 0))
            
            execution_time = (time.time() - start_time) * 1000
            
            result = {
                "success": True,
                "rows_updated": rows_updated,
                "total_rows": before_count,
                "new_version": history["version"],
                "execution_time_ms": execution_time,
                "message": f"Updated {rows_updated:,} rows successfully"
            }
            
            logger.info(f"✅ Update completed: {rows_updated:,} rows in {execution_time:.0f}ms")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error updating Delta table: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to update Delta table: {str(e)}",
                "execution_time_ms": (time.time() - start_time) * 1000
            }
    
    def delete_from_delta_table(
        self,
        bucket: str,
        table_path: str,
        credentials: UserAWSCredentials,
        condition: str
    ) -> Dict[str, Any]:
        """
        Delete rows from Delta table (ACID transaction).
        
        SAFE DELETES:
        -------------
        - Atomic: All deletes happen or none
        - Versioned: Old data kept for time travel
        - Efficient: Only affected files rewritten
        
        Args:
            bucket: S3 bucket
            table_path: Path to Delta table
            credentials: AWS credentials
            condition: SQL WHERE condition for which rows to delete
            
        Returns:
            Dict with delete statistics
            
        Example:
        --------
        # Delete inactive employees
        delete_from_delta_table(
            bucket="my-bucket",
            table_path="employees",
            condition="is_active = false"
        )
        """
        start_time = time.time()
        
        try:
            logger.info(f"🗑️ Deleting from Delta table: s3://{bucket}/{table_path}")
            logger.info(f"   Condition: {condition}")
            
            self._configure_credentials(credentials)
            s3_path = self._build_s3_path(bucket, table_path)
            
            spark = self._get_spark_session()
            
            # Load Delta table
            delta_table = DeltaTable.forPath(spark, s3_path)
            
            # Count before delete
            before_count = delta_table.toDF().count()
            
            # Execute delete (ACID transaction!)
            delta_table.delete(condition)
            
            # Get delete metrics
            history = delta_table.history(1).collect()[0]
            operation_metrics = history["operationMetrics"]
            
            rows_deleted = int(operation_metrics.get("numDeletedRows", 0))
            after_count = before_count - rows_deleted
            
            execution_time = (time.time() - start_time) * 1000
            
            result = {
                "success": True,
                "rows_deleted": rows_deleted,
                "rows_remaining": after_count,
                "new_version": history["version"],
                "execution_time_ms": execution_time,
                "message": f"Deleted {rows_deleted:,} rows successfully"
            }
            
            logger.info(f"✅ Delete completed: {rows_deleted:,} rows in {execution_time:.0f}ms")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error deleting from Delta table: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to delete from Delta table: {str(e)}",
                "execution_time_ms": (time.time() - start_time) * 1000
            }
    
    def merge_into_delta_table(
        self,
        bucket: str,
        table_path: str,
        credentials: UserAWSCredentials,
        source_data: List[Dict[str, Any]],
        merge_key: str,
        update_cols: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        UPSERT into Delta table (Insert or Update).
        
        THE MOST POWERFUL FEATURE:
        --------------------------
        MERGE = "Upsert" = Update if exists, Insert if not
        
        Use case: Daily data updates
        - New records → Insert
        - Existing records → Update
        - All in one atomic operation
        
        Args:
            bucket: S3 bucket
            table_path: Path to Delta table
            credentials: AWS credentials
            source_data: New data to merge (list of dicts)
            merge_key: Column to match on (e.g., "employee_id")
            update_cols: Columns to update (all if None)
            
        Returns:
            Dict with merge statistics
            
        Example:
        --------
        # Update employee records (upsert)
        merge_into_delta_table(
            bucket="my-bucket",
            table_path="employees",
            source_data=[
                {"employee_id": 1, "salary": 120000, "department": "IT"},
                {"employee_id": 999, "salary": 80000, "department": "HR"}
            ],
            merge_key="employee_id"
        )
        # ID 1 exists → Update salary and department
        # ID 999 is new → Insert new row
        """
        start_time = time.time()
        
        try:
            logger.info(f"🔀 Merging into Delta table: s3://{bucket}/{table_path}")
            logger.info(f"   Merge key: {merge_key}")
            logger.info(f"   Source records: {len(source_data)}")
            
            self._configure_credentials(credentials)
            s3_path = self._build_s3_path(bucket, table_path)
            
            spark = self._get_spark_session()
            
            # Convert source data to DataFrame
            source_df = spark.createDataFrame(source_data)
            
            # Load Delta table
            delta_table = DeltaTable.forPath(spark, s3_path)
            
            # Build merge expression
            merge_condition = f"target.{merge_key} = source.{merge_key}"
            
            # Determine which columns to update
            if update_cols is None:
                update_cols = [col for col in source_df.columns if col != merge_key]
            
            update_dict = {col: f"source.{col}" for col in update_cols}
            insert_dict = {col: f"source.{col}" for col in source_df.columns}
            
            # Execute merge (UPSERT!)
            (delta_table.alias("target")
             .merge(source_df.alias("source"), merge_condition)
             .whenMatchedUpdate(set=update_dict)
             .whenNotMatchedInsert(values=insert_dict)
             .execute())
            
            # Get merge metrics
            history = delta_table.history(1).collect()[0]
            operation_metrics = history["operationMetrics"]
            
            rows_inserted = int(operation_metrics.get("numTargetRowsInserted", 0))
            rows_updated = int(operation_metrics.get("numTargetRowsUpdated", 0))
            
            execution_time = (time.time() - start_time) * 1000
            
            result = {
                "success": True,
                "rows_inserted": rows_inserted,
                "rows_updated": rows_updated,
                "total_source_rows": len(source_data),
                "new_version": history["version"],
                "execution_time_ms": execution_time,
                "message": f"Merged successfully: {rows_inserted} inserted, {rows_updated} updated"
            }
            
            logger.info(f"✅ Merge completed in {execution_time:.0f}ms")
            logger.info(f"   Inserted: {rows_inserted}, Updated: {rows_updated}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error merging into Delta table: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to merge into Delta table: {str(e)}",
                "execution_time_ms": (time.time() - start_time) * 1000
            }
    
    def get_delta_history(
        self,
        bucket: str,
        table_path: str,
        credentials: UserAWSCredentials,
        limit: int = 20
    ) -> Dict[str, Any]:
        """
        Get Delta table history (audit log).
        
        DELTA'S TRANSACTION LOG:
        ------------------------
        Every operation is logged:
        - What changed
        - When it changed
        - Who changed it
        - How many rows affected
        
        Perfect for:
        - Auditing
        - Debugging
        - Time travel
        
        Args:
            bucket: S3 bucket
            table_path: Path to Delta table
            credentials: AWS credentials
            limit: Number of history entries to return
            
        Returns:
            Dict with history entries
        """
        try:
            logger.info(f"📜 Getting Delta history: s3://{bucket}/{table_path}")
            
            self._configure_credentials(credentials)
            s3_path = self._build_s3_path(bucket, table_path)
            
            spark = self._get_spark_session()
            
            # Load Delta table
            delta_table = DeltaTable.forPath(spark, s3_path)
            
            # Get history
            history_df = delta_table.history(limit)
            history_rows = history_df.collect()
            
            # Convert to list of dicts
            history = []
            for row in history_rows:
                history.append({
                    "version": row["version"],
                    "timestamp": str(row["timestamp"]),
                    "operation": row["operation"],
                    "operationMetrics": dict(row["operationMetrics"]) if row["operationMetrics"] else {},
                    "userMetadata": row.get("userMetadata"),
                })
            
            result = {
                "success": True,
                "history": history,
                "total_versions": len(history),
                "current_version": history[0]["version"] if history else 0,
                "message": f"Retrieved {len(history)} history entries"
            }
            
            logger.info(f"✅ Retrieved {len(history)} history entries")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error getting Delta history: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to get Delta history: {str(e)}"
            }


# Global service instance
spark_delta_service = SparkDeltaService()
