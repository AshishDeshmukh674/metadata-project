"""
Run the FastAPI server for the Unified Data Access Platform.
"""

import uvicorn
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

if __name__ == "__main__":
    logger.info("Starting FastAPI server...")
    
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
        access_log=True
    )
