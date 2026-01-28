#!/usr/bin/env python3
"""
Google Slides API service with token bucket rate limiting.

This module provides a singleton GSlidesAPI service that wraps Google Slides API calls
with rate limiting (600 reads/min, 60 writes/min) and automatic retry logic for 429 errors.
"""

from __future__ import annotations
import logging
import random
import threading
import time
from datetime import datetime
from typing import Optional
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class TokenBucket:
    """
    Thread-safe token bucket rate limiter for Google Slides API.

    Maintains separate buckets for read and write operations with different rate limits.
    """

    def __init__(self, read_rate: float, write_rate: float, read_capacity: Optional[int] = None, write_capacity: Optional[int] = None):
        """
        Initialize token bucket with specified rates.

        Args:
            read_rate: Number of read tokens per minute
            write_rate: Number of write tokens per minute
            read_capacity: Maximum read tokens (defaults to read_rate)
            write_capacity: Maximum write tokens (defaults to write_rate)
        """
        self.read_rate = read_rate  # tokens per minute
        self.write_rate = write_rate  # tokens per minute

        # Capacity defaults to rate (allows burst up to full rate)
        self.read_capacity = read_capacity if read_capacity is not None else read_rate
        self.write_capacity = write_capacity if write_capacity is not None else write_rate

        # Current token counts (start full)
        self.read_tokens = self.read_capacity
        self.write_tokens = self.write_capacity

        # Last refill timestamps
        self.last_read_refill = time.time()
        self.last_write_refill = time.time()

        # Lock for thread safety
        self.lock = threading.Lock()

    def acquire(self, operation_type: str) -> None:
        """
        Acquire a token for the specified operation type.

        Blocks until a token is available. Refills tokens based on elapsed time.

        Args:
            operation_type: Either "read" or "write"
        """
        if operation_type not in ("read", "write"):
            raise ValueError(f"operation_type must be 'read' or 'write', got '{operation_type}'")

        with self.lock:
            # Refill tokens based on elapsed time
            self._refill()

            # Determine which bucket to use
            if operation_type == "read":
                tokens = self.read_tokens
                capacity = self.read_capacity
                rate = self.read_rate
            else:  # write
                tokens = self.write_tokens
                capacity = self.write_capacity
                rate = self.write_rate

            # If no tokens available, wait and refill
            if tokens < 1:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                logger.debug(f"[TokenBucket] {current_time} - Rate limit - waiting for {operation_type} token")
                # Calculate time needed to get one token
                tokens_per_second = rate / 60.0
                wait_time = (1.0 - tokens) / tokens_per_second
                if wait_time > 0:
                    time.sleep(wait_time)
                    self._refill()

                # After waiting, we should have at least one token
                if operation_type == "read":
                    tokens = self.read_tokens
                else:
                    tokens = self.write_tokens

                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                logger.debug(f"[TokenBucket] {current_time} - Rate limit - {operation_type} token acquired, proceeding")

            # Consume one token
            if operation_type == "read":
                self.read_tokens -= 1
            else:
                self.write_tokens -= 1

    def _refill(self) -> None:
        """
        Refill tokens based on elapsed time since last refill.

        This is called with the lock held.
        """
        now = time.time()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        # Refill read tokens
        elapsed_minutes = (now - self.last_read_refill) / 60.0
        tokens_to_add = elapsed_minutes * self.read_rate
        self.read_tokens = min(self.read_capacity, self.read_tokens + tokens_to_add)
        self.last_read_refill = now

        # Refill write tokens
        elapsed_minutes = (now - self.last_write_refill) / 60.0
        tokens_to_add = elapsed_minutes * self.write_rate
        self.write_tokens = min(self.write_capacity, self.write_tokens + tokens_to_add)
        self.last_write_refill = now

        # Debug log with prefix and timestamp
        logger.debug(
            f"[TokenBucket] {current_time} - Refill completed: "
            f"read_tokens={self.read_tokens}, write_tokens={self.write_tokens}"
        )


class GSlidesAPI:
    """
    Singleton service that wraps Google Slides API with rate limiting and retry logic.

    Provides rate-limited access to Google Slides API operations with automatic
    retry on 429 errors using exponential backoff.
    """

    _instance: Optional[GSlidesAPI] = None
    _lock = threading.Lock()

    def __init__(self, creds):
        """
        Initialize GSlidesAPI service.

        Args:
            creds: Google OAuth credentials
        """
        self.creds = creds
        self.service = build("slides", "v1", credentials=creds)
        # Initialize token bucket with Google Slides API limits
        # 600 reads/min, 60 writes/min
        self.token_bucket = TokenBucket(read_rate=600.0, write_rate=60.0)

    @classmethod
    def get_instance(cls, creds) -> GSlidesAPI:
        """
        Get or create the singleton instance of GSlidesAPI.

        Args:
            creds: Google OAuth credentials

        Returns:
            GSlidesAPI instance
        """
        if cls._instance is None:
            with cls._lock:
                # Double-check pattern
                if cls._instance is None:
                    cls._instance = cls(creds)
        return cls._instance

    def _retry_with_exponential_backoff(self, func, max_retries=5, initial_delay=1, max_delay=60, backoff_factor=2):
        """
        Retry a function with exponential backoff on 429 and 5xx errors.

        Args:
            func: Function to retry (should be a callable that takes no arguments)
            max_retries: Maximum number of retry attempts (default: 5)
            initial_delay: Initial delay in seconds before first retry (default: 1)
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
                        print(f"  ✗ {error_msg}. Max retries ({max_retries}) reached.")
                        raise
                else:
                    # For non-retryable errors, re-raise immediately
                    raise
            except Exception:
                # For non-HttpError exceptions, re-raise immediately
                raise

    def get_presentation(self, presentation_id: str):
        """
        Get a presentation by ID (rate-limited read operation).

        Args:
            presentation_id: ID of the presentation

        Returns:
            Presentation resource dictionary
        """
        # Acquire read token (blocks if needed)
        self.token_bucket.acquire("read")

        # Execute with retry logic
        def _get():
            return self.service.presentations().get(presentationId=presentation_id).execute()

        return self._retry_with_exponential_backoff(_get)

    def batch_update(self, presentation_id: str, body: dict):
        """
        Execute a batch update on a presentation (rate-limited write operation).

        Args:
            presentation_id: ID of the presentation
            body: Request body containing the batch update requests

        Returns:
            BatchUpdatePresentationResponse
        """
        # Acquire write token (blocks if needed)
        self.token_bucket.acquire("write")

        # Execute with retry logic
        def _batch_update():
            return self.service.presentations().batchUpdate(
                presentationId=presentation_id,
                body=body
            ).execute()

        return self._retry_with_exponential_backoff(_batch_update)
