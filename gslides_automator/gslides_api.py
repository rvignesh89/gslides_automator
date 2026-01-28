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
from typing import Optional
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from gslides_automator.token_bucket import TokenBucket

logger = logging.getLogger(__name__)


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
