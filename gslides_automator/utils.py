#!/usr/bin/env python3
"""
Shared utility functions for Google API clients.
"""

from __future__ import annotations
import random
import time
from googleapiclient.errors import HttpError


def retry_with_exponential_backoff(
    func,
    max_retries=5,
    initial_delay=5,
    max_delay=60,
    backoff_factor=2,
):
    """
    Retry a function with exponential backoff on 429 and 5xx errors.

    Args:
        func: Function to retry (should be a callable that takes no arguments)
        max_retries: Maximum number of retry attempts (default: 5)
        initial_delay: Initial delay in seconds before first retry (default: 5)
        max_delay: Maximum delay in seconds between retries (default: 60)
        backoff_factor: Factor to multiply delay by after each retry (default: 2)

    Returns:
        The return value of func() if successful

    Raises:
        HttpError: If the error is not retryable or if max_retries is exceeded
        Exception: Any other exception raised by func()
    """
    delay = initial_delay

    for attempt in range(max_retries + 1):
        try:
            return func()
        except HttpError as error:
            status = error.resp.status
            # Check if it's a retryable error (429 Too Many Requests or 5xx Server Errors)
            is_retryable = (status == 429) or (500 <= status < 600)

            if is_retryable:
                if attempt < max_retries:
                    # Calculate wait time with exponential backoff and random jitter
                    base_wait_time = min(delay, max_delay)
                    # Add random jitter: ±20% of the base wait time
                    jitter = base_wait_time * 0.2 * (2 * random.random() - 1)
                    wait_time = max(0.1, base_wait_time + jitter)  # Ensure minimum 0.1s
                    if status == 429:
                        error_msg = "Rate limit exceeded (429)"
                    else:
                        error_msg = f"Server error ({status})"
                    print(
                        f"  ⚠️  {error_msg}. Retrying in {wait_time:.1f} seconds... (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    delay *= backoff_factor
                else:
                    if status == 429:
                        error_msg = "Rate limit exceeded (429)"
                    else:
                        error_msg = f"Server error ({status})"
                    print(
                        f"  ✗ {error_msg}. Max retries ({max_retries}) reached."
                    )
                    raise
            else:
                # For non-retryable errors, re-raise immediately
                raise
        except Exception:
            # For non-HttpError exceptions, re-raise immediately
            raise
