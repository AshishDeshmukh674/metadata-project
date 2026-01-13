"""
Configuration settings for the Unified Data Access Platform.
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class AWSConfig:
    """AWS configuration settings."""
    
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    region_name: str = "us-east-1"
    
    @classmethod
    def from_env(cls) -> "AWSConfig":
        """Load AWS configuration from environment variables."""
        return cls(
            access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION", "us-east-1")
        )


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    
    type: str = "sqlite"  # sqlite or postgresql
    path: str = "metadata.db"  # For SQLite
    
    # For PostgreSQL (future)
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Load database configuration from environment variables."""
        db_type = os.getenv("DB_TYPE", "sqlite")
        
        if db_type == "sqlite":
            return cls(
                type="sqlite",
                path=os.getenv("DB_PATH", "metadata.db")
            )
        elif db_type == "postgresql":
            return cls(
                type="postgresql",
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT", "5432")),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD")
            )
        else:
            raise ValueError(f"Unsupported database type: {db_type}")


@dataclass
class PlatformConfig:
    """Main platform configuration."""
    
    aws: AWSConfig
    database: DatabaseConfig
    log_level: str = "INFO"
    
    @classmethod
    def from_env(cls) -> "PlatformConfig":
        """Load platform configuration from environment variables."""
        return cls(
            aws=AWSConfig.from_env(),
            database=DatabaseConfig.from_env(),
            log_level=os.getenv("LOG_LEVEL", "INFO")
        )
    
    @classmethod
    def default(cls) -> "PlatformConfig":
        """Get default configuration."""
        return cls(
            aws=AWSConfig(),
            database=DatabaseConfig(),
            log_level="INFO"
        )
