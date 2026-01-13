"""
FastAPI application for Unified Data Access Platform API.
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import sys

from .routes import router
from .models import HealthResponse, ErrorResponse
from ..utils.logger import setup_logger
from ..utils.exceptions import PlatformException

logger = setup_logger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Unified Data Access Platform API",
    description="""
    REST API for discovering and querying lakehouse table metadata.
    
    ## Features
    
    * **Format Detection**: Automatically detect Iceberg, Delta Lake, and Hudi tables
    * **Metadata Discovery**: Extract and normalize table metadata from S3
    * **Unified Schema**: Store all table formats in a unified internal schema
    * **Query Interface**: List and retrieve table metadata via REST API
    
    ## Supported Formats
    
    * Apache Iceberg
    * Delta Lake
    * Apache Hudi
    """,
    version="1.0.0",
    contact={
        "name": "Platform Team",
        "email": "platform@example.com",
    },
    license_info={
        "name": "Proprietary",
    },
)

# Add CORS middleware
# CORS = Cross-Origin Resource Sharing
# It is a browser security rule.
# Example:

# Frontend → http://localhost:3000

# Backend → http://localhost:8000

# ❌ Browser blocks this unless backend explicitly allows it

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production(it may take values like       localhost:3000,example.com,mobile app))
    allow_credentials=True,  #allows cookies, authorization headers, etc.
    allow_methods=["*"], # allows all HTTP methods (GET,POST,PUT,DELETE,etc.)
    allow_headers=["*"], # allows all headers (Content-Type,Authorization,etc.
)

# Include routers
app.include_router(router)


@app.exception_handler(PlatformException)
async def platform_exception_handler(request: Request, exc: PlatformException):
    """Handle platform-specific exceptions."""
    logger.error(f"Platform exception: {exc.message}")
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            success=False,
            error=exc.message,
            details=exc.details
        ).model_dump()
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle general exceptions."""
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            success=False,
            error="Internal server error",
            details={"message": str(exc)}
        ).model_dump()
    )


@app.get("/", tags=["root"])
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Unified Data Access Platform API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health", response_model=HealthResponse, tags=["health"])
async def health_check():
    """
    Health check endpoint.
    
    Returns system status and basic metrics.
    """
    try:
        from .routes import get_engine
        
        engine = get_engine()
        table_count = engine.metadata_store.get_table_count()
        
        return HealthResponse(
            status="healthy",
            version="1.0.0",
            timestamp=datetime.now(),
            database_connected=True,
            tables_count=table_count
        )
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return HealthResponse(
            status="unhealthy",
            version="1.0.0",
            timestamp=datetime.now(),
            database_connected=False,
            tables_count=0
        )


@app.on_event("startup")
async def startup_event():
    """Application startup event."""
    logger.info("Starting Unified Data Access Platform API")
    logger.info(f"Python version: {sys.version}")
    logger.info("API documentation available at: /docs")


@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown event."""
    logger.info("Shutting down Unified Data Access Platform API")
