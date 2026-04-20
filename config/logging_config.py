import logging
import sys
from pathlib import Path


def setup_logging(level=logging.INFO, log_file=None):
    """
    Configure logging for the entire application.
    Call this ONCE at the entry point (DAG or main.py).
    """
    
    # Format: timestamp - logger name - level - message
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Root logger - catches all logs
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Clear existing handlers (prevents duplicate logs)
    root_logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Reduce noise from third-party libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    return root_logger


# Convenience function to get module logger
def get_logger(name):
    """Get a logger for a module. Use: logger = get_logger(__name__)"""
    return logging.getLogger(name)
