import logging
import os
import sys
from pathlib import Path


def _get_log_level() -> int:
    """
    Determine log level based on environment variable.
    
    Returns:
        logging.DEBUG if ENV is 'dev', otherwise logging.INFO
    """
    env = os.getenv("ENV", "").lower()
    if env == "dev":
        return logging.DEBUG
    return logging.INFO


def setup_logger(name: str = None, log_level: int = None) -> logging.Logger:
    """
    Set up and configure a logger instance.
    
    Args:
        name: Name of the logger (typically __name__). If None, uses root logger.
        log_level: Logging level. If None, determined from ENV variable (DEBUG for 'dev', INFO otherwise)
    
    Returns:
        Configured logger instance
    """
    if log_level is None:
        log_level = _get_log_level()
    
    logger = logging.getLogger(name)
    
    # Avoid adding handlers multiple times if logger already configured
    if logger.handlers:
        return logger
    
    logger.setLevel(log_level)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(console_handler)
    
    return logger


def get_logger(name: str = None) -> logging.Logger:
    """
    Get a logger instance. If not configured, sets it up with defaults.
    Log level is automatically determined from ENV variable (DEBUG for 'dev', INFO otherwise).
    
    Args:
        name: Name of the logger (typically __name__)
    
    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    
    # If logger has no handlers, set it up
    if not logger.handlers:
        return setup_logger(name)
    
    return logger

