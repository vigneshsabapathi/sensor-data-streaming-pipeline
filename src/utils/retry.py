"""Retry with exponential backoff for transient failures."""

import logging
import time
from functools import wraps
from typing import Callable, Type

logger = logging.getLogger(__name__)


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: tuple[Type[Exception], ...] = (Exception,),
) -> Callable:
    """Decorator that retries a function with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts.
        base_delay: Initial delay in seconds (doubles each retry).
        max_delay: Cap on delay between retries.
        exceptions: Tuple of exception types to catch and retry on.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(
                            "%s failed after %d attempts: %s",
                            func.__name__, max_retries + 1, e,
                        )
                        raise
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.warning(
                        "%s failed (attempt %d/%d): %s. Retrying in %.1fs...",
                        func.__name__, attempt + 1, max_retries + 1, e, delay,
                    )
                    time.sleep(delay)
            raise last_exception  # unreachable, but satisfies type checker
        return wrapper
    return decorator
