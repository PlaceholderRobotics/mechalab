"""
Backend-only script for LeLab
Runs just the FastAPI server with uvicorn
"""

import logging
import uvicorn

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Start the server"""
    logger.info("🤖 Starting Mechaverse `mechalab` server...")
    uvicorn.run(
        "app.main:app", host="0.0.0.0", port=8000, reload=True, log_level="info"
    )


if __name__ == "__main__":
    main()
