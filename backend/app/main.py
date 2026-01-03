"""
Metastore Viewer - Main Application
====================================

FastAPI application for exploring lakehouse table metadata.

This application provides REST APIs to:
- Connect to S3 and other object stores
- Read metadata from Iceberg, Delta, Hudi, and Parquet tables
- Display table schemas, partitions, and statistics
- Track version history and snapshots

Security Features:
- CORS protection
- Environment-based configuration
- No hardcoded credentials
- Request validation
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import sys

from app.config import settings, validate_settings
from app.api import health, auth, datastore, parquet, iceberg, delta, hudi, formats

# ===================================
# Configure Logging
# ===================================
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ===================================
# Lifespan Event Handler
# ===================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan event handler for startup and shutdown.
    Replaces deprecated @app.on_event decorators.
    """
    # Startup
    logger.info("=" * 60)
    logger.info(f"🚀 Starting {settings.app_name} v{settings.app_version}")
    logger.info("=" * 60)
    
    try:
        # Validate configuration
        validate_settings()
        
        logger.info("=" * 60)
        
        # Display appropriate message based on credential mode
        logger.info("📋 USER CREDENTIAL MODE")
        logger.info("   Users provide their own AWS credentials for secure access")
        logger.info("")
        logger.info("📝 API Endpoints:")
        logger.info("   Authentication:")
        logger.info("   1. POST /api/v1/auth/validate-credentials")
        logger.info("   2. POST /api/v1/auth/create-session")
        logger.info("")
        logger.info("   Table Format APIs (requires X-Session-ID header):")
        logger.info("   - Parquet: /api/v1/parquet/preview-changes, /execute-changes")
        logger.info("   - Iceberg: /api/v1/iceberg/preview-changes, /execute-changes")
        logger.info("   - Delta:   /api/v1/delta/preview-changes, /execute-changes")
        logger.info("   - Hudi:    /api/v1/hudi/preview-changes, /execute-changes")
        logger.info("")
        logger.info("   Legacy Datastore (requires X-Session-ID header):")
        logger.info("   3. GET /api/v1/datastore/info")
        logger.info("   4. GET /api/v1/datastore/metadata")
        logger.info("")
        logger.info("🔒 Security features:")
        logger.info(f"   - Session TTL: {settings.session_ttl_minutes} minutes")
        logger.info(f"   - HTTPS required: {settings.require_https and not settings.debug}")
        logger.info(f"   - CORS origins: {settings.get_cors_origins()}")
        
    except Exception as e:
        logger.error(f"❌ Configuration Error: {str(e)}")
        logger.error("Please check your .env file and ensure all required settings are configured.")
    
    logger.info("=" * 60)
    logger.info(f"📚 API Documentation available at:")
    logger.info(f"   Swagger UI: http://{settings.host}:{settings.port}/docs")
    logger.info(f"   ReDoc:      http://{settings.host}:{settings.port}/redoc")
    logger.info("=" * 60)
    
    yield  # Application runs here(yield is a Python keyword that creates a pause point in a function)
    
    #after yield below like is printed during shutdown
    # Shutdown
    logger.info("👋 Shutting down Metastore Viewer")


# ===================================
# Create FastAPI Application
# ===================================
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="""
    🔍 **Metastore Viewer API**
    
    Web-based metastore viewer for lakehouse tables.
    Users provide their own AWS credentials for secure access.
    
    ## Authentication Flow
    1. POST /api/v1/auth/validate-credentials - Validate AWS credentials
    2. POST /api/v1/auth/create-session - Create session (returns session_id)
    3. Use session_id in X-Session-ID header for all API calls
    
    ## Table Format APIs (Requires X-Session-ID header)
    
    ### Parquet Operations
    - POST /api/v1/parquet/preview-changes - Preview modifications
    - POST /api/v1/parquet/execute-changes - Execute modifications
    
    ### Iceberg Operations (Coming Soon)
    - POST /api/v1/iceberg/preview-changes
    - POST /api/v1/iceberg/execute-changes
    
    ### Delta Lake Operations
    - POST /api/v1/delta/preview-changes - Preview modifications
    - POST /api/v1/delta/execute-changes - Execute modifications (read-only currently)
    
    ### Hudi Operations (Coming Soon)
    - POST /api/v1/hudi/preview-changes
    - POST /api/v1/hudi/execute-changes
    
    ## Legacy Datastore APIs (Requires X-Session-ID header)
    - GET /api/v1/datastore/info - Get table information
    - GET /api/v1/datastore/metadata - Get schema and columns
    
    ## Supported Formats
    - ✅ Apache Parquet (Full support)
    - 🚧 Apache Iceberg (In development)
    - 🚧 Delta Lake (Read support, write coming soon)
    - 🚧 Apache Hudi (Requires Spark integration)
    """,
    debug=settings.debug,
    docs_url="/docs",      # Swagger UI
    redoc_url="/redoc",    # ReDoc
    lifespan=lifespan      # Use lifespan event handler
)


# ===================================
# CORS Middleware (Security)
# ===================================
# Allow frontend applications to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.get_cors_origins(),  # Which domains can call this API
    allow_credentials=True,
    allow_methods=["*"],        # GET, POST, PUT, DELETE, etc.
    allow_headers=["*"],        # All headers allowed
)


# ===================================
# Register API Routers
# ===================================
app.include_router(health.router)
app.include_router(auth.router)
app.include_router(formats.router)  # Format discovery
app.include_router(datastore.router)

# Table format-specific APIs
app.include_router(parquet.router)
app.include_router(iceberg.router)
app.include_router(delta.router)
app.include_router(hudi.router)


# ===================================
# Root Endpoint
# ===================================
#@ -->In FastAPI, it means "register this function as an API endpoint
@app.get("/")
async def root():
    """
    Root endpoint - redirects to API docs.
    """
    return {
        "message": f"Welcome to {settings.app_name}",
        "version": settings.app_version,
        "docs": "/docs",
        "health": "/api/v1/health"
    }


# ===================================
# Main Entry Point
# ===================================
if __name__ == "__main__":
    # Code here ONLY runs when file is executed directly
    # NOT when imported by another file
    import uvicorn
    
    # Run the application
    # Use: python -m app.main
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,  # Auto-reload on code changes in debug mode
        log_level=settings.log_level.lower()
    )
