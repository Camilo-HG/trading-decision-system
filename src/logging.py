import sys
from loguru import logger

def setup_logging():
    """Configures structured logging for the application."""
    logger.remove()  # Remove default handler
    logger.add(
        sys.stderr,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
        level="INFO",
        diagnose=True,
    )

    # Add a custom JSON formatter for structured logging
    logger.add(
        "file_{time}.log",
        format="{message}",
        level="INFO",
        serialize=True,
        backtrace=True,
        diagnose=True,
        rotation="10 MB",
        retention="1 week"
    )

    return logger

if __name__ == "__main__":
    log = setup_logging()
    log.info("Logging setup complete.")
