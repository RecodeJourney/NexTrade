import logging
import inspect
import os
from typing import Optional


def _caller_basename(skip: int = 2) -> str:
    stack = inspect.stack()
    if len(stack) <= skip:
        return "unknown"
    frame = stack[skip]
    return os.path.basename(frame.filename) or "unknown"


def get_logger(name: Optional[str] = None, level: int = logging.INFO) -> logging.Logger:

    if name is None:
        name = _caller_basename(skip=2)

    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler()
        # Remove .py extension and take the last component (e.g., "Quarterly.db_writer" -> "db_writer")
        name = name.replace('.py', '').split(".")[-1]
        fmt = f"[{name}] %(message)s"
        handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(handler)
        logger.propagate = False

    logger.setLevel(level)
    return logger


def log(message: str, level: int = logging.INFO) -> None:
    """Convenience function: log a single message using an auto-detected logger.

    Examples:
        log("hello")
        log("something", level=logging.ERROR)
    """
    logger = get_logger()
    logger.log(level, message)