#!/usr/bin/env python3
"""
Google Slides API service with token bucket rate limiting.

This module provides a GSlidesAPI service that wraps Google Slides API calls
with rate limiting (600 reads/min, 60 writes/min) and automatic retry logic for 429 errors.

The module provides a shared service instance via get_shared_slides_service() for
use across the codebase to maintain rate limiting.
"""

from __future__ import annotations
import logging
import threading
from typing import Optional
from googleapiclient.discovery import build

from gslides_automator.leaky_bucket import LeakyBucket
from gslides_automator.utils import retry_with_exponential_backoff

logger = logging.getLogger(__name__)

# Module-level shared service instance
_service: Optional[GSlidesAPI] = None
_service_lock = threading.Lock()


class GSlidesAPI:
    """
    Service that wraps Google Slides API with rate limiting and retry logic.

    Provides rate-limited access to Google Slides API operations with automatic
    retry on 429 errors using exponential backoff.
    """

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
        self.token_bucket = LeakyBucket(read_rate=600.0, write_rate=60.0)

    def get_shared_slides_service(creds) -> GSlidesAPI:
        """
        Get or create the shared GSlidesAPI service instance.

        This function provides a module-level shared service instance that maintains
        rate limiting across all API calls in the application.

        Args:
            creds: Google OAuth credentials

        Returns:
            GSlidesAPI instance (shared across the application)
        """
        global _service
        if _service is None:
            with _service_lock:
                # Double-check pattern for thread safety
                if _service is None:
                    _service = GSlidesAPI(creds)
        return _service

    def reset_service():
        """
        Reset the shared service instance (useful for testing).

        This function is thread-safe and should be used in test fixtures
        to ensure test isolation when needed.
        """
        global _service
        with _service_lock:
            _service = None

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
            return (
                self.service.presentations()
                .get(presentationId=presentation_id)
                .execute()
            )

        return retry_with_exponential_backoff(_get)

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
            return (
                self.service.presentations()
                .batchUpdate(presentationId=presentation_id, body=body)
                .execute()
            )

        return retry_with_exponential_backoff(_batch_update)
