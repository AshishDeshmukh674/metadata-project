"""
Configuration Management
========================
Centralized configuration using pydantic-settings.
Loads configuration from environment variables (.env file).

Security Features:
- Sensitive data (AWS keys) only loaded from environment
- Validation for required fields
- Type checking for all config values
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    
    Priority (highest to lowest):
    1. Environment variables
    2. .env file
    3. Default values
    """
    
    # ===================================
    # Application Settings
    # ===================================
    app_name: str = "Metastore Viewer"
    app_version: str = "1.0.0"
    environment: str = "development"
    debug: bool = True
    host: str = "0.0.0.0"
    port: int = 8000
    
    # ===================================
    # AWS Configuration
    # ===================================
    # Security Note: These will be loaded from environment variables
    # Never hardcode credentials in code!
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_default_region: str = "us-east-1"
    
    # Optional: Use IAM role instead of access keys (more secure)
    use_iam_role: bool = False
    
    # ===================================
    # Credential Mode Configuration
    # ===================================
    # Choose credential approach:
    # - "backend": Use backend credentials (Approach 1)
    # - "user": Users provide their own credentials (Approach 2 - Recommended for production)
    credential_mode: str = "user"
    
    # Session settings for user credentials
    session_ttl_minutes: int = 30  # Session expiration time (reduced for security)
    require_https: bool = True      # Enforce HTTPS for user credentials
    
    # ===================================
    # S3 Configuration
    # ===================================
    default_s3_bucket: Optional[str] = None
    default_s3_prefix: Optional[str] = None
    
    # ===================================
    # API Security
    # ===================================
    # CORS settings - which domains can call this API
    cors_origins: str = "http://localhost:3000,http://localhost:5173"
    
    # Optional: API key for basic authentication
    api_key: Optional[str] = None
    
    # ===================================
    # Caching
    # ===================================
    enable_cache: bool = False
    cache_ttl_seconds: int = 300  # 5 minutes
    
    # ===================================
    # Logging
    # ===================================
    log_level: str = "INFO"
    
    # ===================================
    # LLM Configuration (Groq)
    # ===================================
    groq_api_key: Optional[str] = None
    groq_model: str = "llama-3.3-70b-versatile"
    
    # ===================================
    # Pydantic Settings Config
    # ===================================
    model_config = SettingsConfigDict(
        # Read from .env file
        env_file=".env",
        env_file_encoding="utf-8",
        # Case insensitive environment variables
        case_sensitive=False,
        # Don't expose extra fields
        extra="ignore"
    )
    
    def get_cors_origins(self) -> list[str]:
        """
        Convert comma-separated CORS origins to list.
        
        Returns:
            List of allowed origin URLs
        """
        return [origin.strip() for origin in self.cors_origins.split(",")]
    
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment.lower() == "production"
    
    def has_aws_credentials(self) -> bool:
        """
        Check if AWS credentials are configured.
        
        Returns:
            True if either access keys or IAM role is configured
        """
        has_keys = bool(self.aws_access_key_id and self.aws_secret_access_key)
        return has_keys or self.use_iam_role
    
    def is_user_credential_mode(self) -> bool:
        """Check if app is configured to use user-provided credentials."""
        return self.credential_mode.lower() == "user"
    
    def is_backend_credential_mode(self) -> bool:
        """Check if app is configured to use backend credentials."""
        return self.credential_mode.lower() == "backend"


# ===================================
# Global Settings Instance
# ===================================
# Create a single instance to be imported throughout the app
# This will automatically load from .env file
settings = Settings()


# ===================================
# Validation on Startup
# ===================================
def validate_settings():
    """
    Validate critical settings on application startup.
    Raises ValueError if configuration is invalid.
    """
    print(f"✅ Configuration loaded successfully")
    print(f"   Environment: {settings.environment}")
    print(f"   AWS Region: {settings.aws_default_region}")
    print(f"   Debug Mode: {settings.debug}")
    print(f"   Session TTL: {settings.session_ttl_minutes} minutes")
    print(f"   Groq Model: {settings.groq_model}")
    
    if settings.require_https and not settings.debug:
        print(f"   ✅ HTTPS: Required for user credentials")
    else:
        print(f"   ⚠️  HTTPS: Disabled (development mode)")
    
    if not settings.groq_api_key:
        print(f"   ⚠️  Warning: GROQ_API_KEY not set - data modification features will not work")
