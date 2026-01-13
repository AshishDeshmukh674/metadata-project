"""
Metadata Storage.

Provides persistence layer for TableMetadata using SQLite.
Designed to be easily migrated to PostgreSQL in production.
"""

import sqlite3
import json
from typing import Optional, List
from pathlib import Path
from datetime import datetime

from ..models.table_metadata import TableMetadata
from ..utils.logger import setup_logger
from ..utils.exceptions import StorageError

logger = setup_logger(__name__)


class MetadataStore:
    """
    Storage abstraction for table metadata.
    
    Uses SQLite for local development, with schema designed
    for easy migration to PostgreSQL for production.
    """
    
    def __init__(self, db_path: str = "metadata.db"):
        """
        Initialize metadata store.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self._initialize_database()
        logger.info(f"MetadataStore initialized with database: {db_path}")
    
    def _initialize_database(self):
        """Create database schema if it doesn't exist."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Main table metadata table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS table_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL UNIQUE,
                    format TEXT NOT NULL,
                    location TEXT NOT NULL,
                    partitions TEXT,
                    properties TEXT,
                    supports_time_travel BOOLEAN NOT NULL DEFAULT 0,
                    num_files INTEGER,
                    size_bytes INTEGER,
                    row_count INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Column metadata table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS column_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_id INTEGER NOT NULL,
                    column_name TEXT NOT NULL,
                    data_type TEXT NOT NULL,
                    nullable BOOLEAN NOT NULL DEFAULT 1,
                    comment TEXT,
                    column_order INTEGER NOT NULL,
                    FOREIGN KEY (table_id) REFERENCES table_metadata (id) ON DELETE CASCADE,
                    UNIQUE(table_id, column_name)
                )
            """)
            
            # Index for faster lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_table_name 
                ON table_metadata(table_name)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_format 
                ON table_metadata(format)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_table_id 
                ON column_metadata(table_id)
            """)
            
            conn.commit()
            conn.close()
            
            logger.debug("Database schema initialized successfully")
            
        except sqlite3.Error as e:
            raise StorageError(
                f"Failed to initialize database: {str(e)}",
                details={"db_path": self.db_path, "error": str(e)}
            )
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable column access by name
            # Enable foreign key constraints
            conn.execute("PRAGMA foreign_keys = ON")
            return conn
        except sqlite3.Error as e:
            raise StorageError(
                f"Failed to connect to database: {str(e)}",
                details={"db_path": self.db_path, "error": str(e)}
            )
    
    def save_table_metadata(self, metadata: TableMetadata) -> int:
        """
        Save or update table metadata.
        
        Args:
            metadata: TableMetadata object to persist
            
        Returns:
            Table ID (integer primary key)
            
        Raises:
            StorageError: If save operation fails
        """
        logger.info(f"Saving metadata for table: {metadata.table_name}")
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Check if table already exists
            cursor.execute(
                "SELECT id FROM table_metadata WHERE table_name = ?",
                (metadata.table_name,)
            )
            existing = cursor.fetchone()
            
            if existing:
                # Update existing table
                table_id = existing[0]
                self._update_table_metadata(cursor, table_id, metadata)
            else:
                # Insert new table
                table_id = self._insert_table_metadata(cursor, metadata)
            
            conn.commit()
            conn.close()
            
            logger.info(f"Successfully saved metadata for table: {metadata.table_name} (id={table_id})")
            return table_id
            
        except sqlite3.Error as e:
            raise StorageError(
                f"Failed to save table metadata: {str(e)}",
                details={"table_name": metadata.table_name, "error": str(e)}
            )
    
    def _insert_table_metadata(self, cursor: sqlite3.Cursor, 
                              metadata: TableMetadata) -> int:
        """Insert new table metadata."""
        cursor.execute("""
            INSERT INTO table_metadata (
                table_name, format, location, partitions, properties,
                supports_time_travel, num_files, size_bytes, row_count,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            metadata.table_name,
            metadata.format,
            metadata.location,
            json.dumps(metadata.partitions),
            json.dumps(metadata.properties),
            metadata.supports_time_travel,
            metadata.num_files,
            metadata.size_bytes,
            metadata.row_count,
            metadata.created_at or datetime.now(),
            metadata.updated_at or datetime.now()
        ))
        
        table_id = cursor.lastrowid
        
        # Insert columns
        for idx, column in enumerate(metadata.columns):
            cursor.execute("""
                INSERT INTO column_metadata (
                    table_id, column_name, data_type, nullable, comment, column_order
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                table_id,
                column.name,
                column.data_type,
                column.nullable,
                column.comment,
                idx
            ))
        
        return table_id
    
    def _update_table_metadata(self, cursor: sqlite3.Cursor, 
                              table_id: int, metadata: TableMetadata):
        """Update existing table metadata."""
        cursor.execute("""
            UPDATE table_metadata SET
                format = ?,
                location = ?,
                partitions = ?,
                properties = ?,
                supports_time_travel = ?,
                num_files = ?,
                size_bytes = ?,
                row_count = ?,
                updated_at = ?
            WHERE id = ?
        """, (
            metadata.format,
            metadata.location,
            json.dumps(metadata.partitions),
            json.dumps(metadata.properties),
            metadata.supports_time_travel,
            metadata.num_files,
            metadata.size_bytes,
            metadata.row_count,
            datetime.now(),
            table_id
        ))
        
        # Delete old columns and insert new ones
        cursor.execute("DELETE FROM column_metadata WHERE table_id = ?", (table_id,))
        
        for idx, column in enumerate(metadata.columns):
            cursor.execute("""
                INSERT INTO column_metadata (
                    table_id, column_name, data_type, nullable, comment, column_order
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                table_id,
                column.name,
                column.data_type,
                column.nullable,
                column.comment,
                idx
            ))
    
    def get_table_metadata(self, table_name: str) -> Optional[TableMetadata]:
        """
        Retrieve table metadata by name.
        
        Args:
            table_name: Name of the table
            
        Returns:
            TableMetadata object or None if not found
            
        Raises:
            StorageError: If retrieval fails
        """
        logger.debug(f"Retrieving metadata for table: {table_name}")
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Get table metadata
            cursor.execute(
                "SELECT * FROM table_metadata WHERE table_name = ?",
                (table_name,)
            )
            table_row = cursor.fetchone()
            
            if not table_row:
                logger.debug(f"Table not found: {table_name}")
                conn.close()
                return None
            
            # Get column metadata
            cursor.execute(
                """
                SELECT column_name, data_type, nullable, comment
                FROM column_metadata
                WHERE table_id = ?
                ORDER BY column_order
                """,
                (table_row['id'],)
            )
            column_rows = cursor.fetchall()
            
            conn.close()
            
            # Build TableMetadata object
            from ..models.table_metadata import ColumnMetadata
            
            columns = [
                ColumnMetadata(
                    name=row['column_name'],
                    data_type=row['data_type'],
                    nullable=bool(row['nullable']),
                    comment=row['comment']
                )
                for row in column_rows
            ]
            
            metadata = TableMetadata(
                table_name=table_row['table_name'],
                format=table_row['format'],
                location=table_row['location'],
                columns=columns,
                partitions=json.loads(table_row['partitions']) if table_row['partitions'] else [],
                properties=json.loads(table_row['properties']) if table_row['properties'] else {},
                supports_time_travel=bool(table_row['supports_time_travel']),
                created_at=datetime.fromisoformat(table_row['created_at']) if table_row['created_at'] else None,
                updated_at=datetime.fromisoformat(table_row['updated_at']) if table_row['updated_at'] else None,
                num_files=table_row['num_files'],
                size_bytes=table_row['size_bytes'],
                row_count=table_row['row_count']
            )
            
            logger.debug(f"Retrieved metadata for table: {table_name}")
            return metadata
            
        except sqlite3.Error as e:
            raise StorageError(
                f"Failed to retrieve table metadata: {str(e)}",
                details={"table_name": table_name, "error": str(e)}
            )
    
    def list_tables(self, format_filter: Optional[str] = None) -> List[str]:
        """
        List all table names, optionally filtered by format.
        
        Args:
            format_filter: Optional format to filter by ("ICEBERG", "DELTA", "HUDI")
            
        Returns:
            List of table names
            
        Raises:
            StorageError: If listing fails
        """
        logger.debug(f"Listing tables (format_filter={format_filter})")
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if format_filter:
                cursor.execute(
                    "SELECT table_name FROM table_metadata WHERE format = ? ORDER BY table_name",
                    (format_filter,)
                )
            else:
                cursor.execute("SELECT table_name FROM table_metadata ORDER BY table_name")
            
            tables = [row[0] for row in cursor.fetchall()]
            
            conn.close()
            
            logger.debug(f"Found {len(tables)} tables")
            return tables
            
        except sqlite3.Error as e:
            raise StorageError(
                f"Failed to list tables: {str(e)}",
                details={"format_filter": format_filter, "error": str(e)}
            )
    
    def delete_table_metadata(self, table_name: str) -> bool:
        """
        Delete table metadata.
        
        Args:
            table_name: Name of the table to delete
            
        Returns:
            True if deleted, False if not found
            
        Raises:
            StorageError: If deletion fails
        """
        logger.info(f"Deleting metadata for table: {table_name}")
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                "DELETE FROM table_metadata WHERE table_name = ?",
                (table_name,)
            )
            
            deleted = cursor.rowcount > 0
            
            conn.commit()
            conn.close()
            
            if deleted:
                logger.info(f"Deleted metadata for table: {table_name}")
            else:
                logger.debug(f"Table not found for deletion: {table_name}")
            
            return deleted
            
        except sqlite3.Error as e:
            raise StorageError(
                f"Failed to delete table metadata: {str(e)}",
                details={"table_name": table_name, "error": str(e)}
            )
    
    def get_table_count(self) -> int:
        """
        Get total number of tables in the store.
        
        Returns:
            Number of tables
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM table_metadata")
            count = cursor.fetchone()[0]
            
            conn.close()
            
            return count
            
        except sqlite3.Error as e:
            raise StorageError(
                f"Failed to get table count: {str(e)}",
                details={"error": str(e)}
            )
