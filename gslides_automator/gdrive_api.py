#!/usr/bin/env python3
"""
Google Drive API service with token bucket rate limiting.

This module provides a singleton GDriveAPI service that wraps Google Drive API calls
with rate limiting (12,000 queries/60s) and automatic retry logic for 429 errors.
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


class GDriveAPI:
    """
    Singleton service that wraps Google Drive API with rate limiting and retry logic.

    Provides rate-limited access to Google Drive API operations with automatic
    retry on 429 errors using exponential backoff.

    Note: Google Drive API uses a single quota bucket (no read/write distinction).
    """

    _instance: Optional[GDriveAPI] = None
    _lock = threading.Lock()

    def __init__(self, creds):
        """
        Initialize GDriveAPI service.

        Args:
            creds: Google OAuth credentials
        """
        self.creds = creds
        self.service = build("drive", "v3", credentials=creds)
        # Initialize token bucket with Google Drive API limits
        # 12,000 queries per 60 seconds (single bucket, no read/write distinction)
        self.token_bucket = TokenBucket(read_rate=12000.0, write_rate=None)

    @classmethod
    def get_instance(cls, creds) -> GDriveAPI:
        """
        Get or create the singleton instance of GDriveAPI.

        Args:
            creds: Google OAuth credentials

        Returns:
            GDriveAPI instance
        """
        if cls._instance is None:
            with cls._lock:
                # Double-check pattern
                if cls._instance is None:
                    cls._instance = cls(creds)
        return cls._instance

    def _retry_with_exponential_backoff(
        self,
        func,
        max_retries=5,
        initial_delay=1,
        max_delay=60,
        backoff_factor=2,
    ):
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

    def list_files(self, query: str = None, **kwargs):
        """
        List files matching the query (rate-limited operation).

        Args:
            query: Query string for filtering files (e.g., "'folder_id' in parents")
            **kwargs: Additional arguments to pass to the API call

        Returns:
            FileList resource dictionary
        """
        # Acquire token (blocks if needed)
        self.token_bucket.acquire()

        # Execute with retry logic
        def _list():
            files_resource = self.service.files()
            if query is not None:
                kwargs["q"] = query
            return files_resource.list(**kwargs).execute()

        return self._retry_with_exponential_backoff(_list)

    def get_file(self, file_id: str, **kwargs):
        """
        Get file metadata by ID (rate-limited operation).

        Args:
            file_id: ID of the file
            **kwargs: Additional arguments to pass to the API call

        Returns:
            File resource dictionary
        """
        # Acquire token (blocks if needed)
        self.token_bucket.acquire()

        # Execute with retry logic
        def _get():
            return self.service.files().get(fileId=file_id, **kwargs).execute()

        return self._retry_with_exponential_backoff(_get)

    def create_file(self, body: dict, **kwargs):
        """
        Create a new file or folder (rate-limited operation).

        Args:
            body: File metadata dictionary (name, mimeType, parents, etc.)
            **kwargs: Additional arguments to pass to the API call (e.g., media_body, fields)

        Returns:
            File resource dictionary
        """
        # Acquire token (blocks if needed)
        self.token_bucket.acquire()

        # Execute with retry logic
        def _create():
            return self.service.files().create(body=body, **kwargs).execute()

        return self._retry_with_exponential_backoff(_create)

    def update_file(self, file_id: str, body: dict = None, **kwargs):
        """
        Update file metadata (rate-limited operation).

        Args:
            file_id: ID of the file to update
            body: File metadata dictionary (optional, can use kwargs instead)
            **kwargs: Additional arguments to pass to the API call

        Returns:
            File resource dictionary
        """
        # Acquire token (blocks if needed)
        self.token_bucket.acquire()

        # Execute with retry logic
        def _update():
            if body is not None:
                kwargs["body"] = body
            return self.service.files().update(fileId=file_id, **kwargs).execute()

        return self._retry_with_exponential_backoff(_update)

    def delete_file(self, file_id: str, **kwargs):
        """
        Delete a file by ID (rate-limited operation).

        Args:
            file_id: ID of the file to delete
            **kwargs: Additional arguments to pass to the API call

        Returns:
            None (empty response on success)
        """
        # Acquire token (blocks if needed)
        self.token_bucket.acquire()

        # Execute with retry logic
        def _delete():
            return self.service.files().delete(fileId=file_id, **kwargs).execute()

        return self._retry_with_exponential_backoff(_delete)

    def get_media(self, file_id: str, **kwargs):
        """
        Get a request object for downloading file media content (rate-limited operation).

        Note: This returns a request object (not executed) to allow the caller to handle streaming.

        Args:
            file_id: ID of the file
            **kwargs: Additional arguments to pass to the API call

        Returns:
            HttpRequest object (not executed)
        """
        # Acquire token (blocks if needed)
        self.token_bucket.acquire()

        # Return request object (not executed) for streaming
        return self.service.files().get_media(fileId=file_id, **kwargs)

    def export_file(self, file_id: str, mime_type: str, **kwargs):
        """
        Get a request object for exporting a file in a different format (rate-limited operation).

        Note: This returns a request object (not executed) to allow the caller to handle streaming.

        Args:
            file_id: ID of the file to export
            mime_type: MIME type of the export format (e.g., "application/pdf")
            **kwargs: Additional arguments to pass to the API call

        Returns:
            HttpRequest object (not executed)
        """
        # Acquire token (blocks if needed)
        self.token_bucket.acquire()

        # Return request object (not executed) for streaming
        return self.service.files().export(fileId=file_id, mimeType=mime_type, **kwargs)

    def copy_file(self, file_id: str, body: dict = None, **kwargs):
        """
        Copy a file (rate-limited operation).

        Args:
            file_id: ID of the file to copy
            body: File metadata dictionary (name, etc.)
            **kwargs: Additional arguments to pass to the API call

        Returns:
            File resource dictionary
        """
        # Acquire token (blocks if needed)
        self.token_bucket.acquire()

        # Execute with retry logic
        def _copy():
            if body is not None:
                kwargs["body"] = body
            return self.service.files().copy(fileId=file_id, **kwargs).execute()

        return self._retry_with_exponential_backoff(_copy)

    def list_permissions(self, file_id: str, **kwargs):
        """
        List permissions for a file (rate-limited operation).

        Args:
            file_id: ID of the file
            **kwargs: Additional arguments to pass to the API call

        Returns:
            PermissionList resource dictionary
        """
        # Acquire token (blocks if needed)
        self.token_bucket.acquire()

        # Execute with retry logic
        def _list():
            return self.service.permissions().list(fileId=file_id, **kwargs).execute()

        return self._retry_with_exponential_backoff(_list)

    def create_permission(self, file_id: str, body: dict, **kwargs):
        """
        Create a permission for a file (rate-limited operation).

        Args:
            file_id: ID of the file
            body: Permission metadata dictionary (type, role, etc.)
            **kwargs: Additional arguments to pass to the API call

        Returns:
            Permission resource dictionary
        """
        # Acquire token (blocks if needed)
        self.token_bucket.acquire()

        # Execute with retry logic
        def _create():
            return (
                self.service.permissions()
                .create(fileId=file_id, body=body, **kwargs)
                .execute()
            )

        return self._retry_with_exponential_backoff(_create)

    def delete_permission(self, file_id: str, permission_id: str, **kwargs):
        """
        Delete a permission for a file (rate-limited operation).

        Args:
            file_id: ID of the file
            permission_id: ID of the permission to delete
            **kwargs: Additional arguments to pass to the API call

        Returns:
            None (empty response on success)
        """
        # Acquire token (blocks if needed)
        self.token_bucket.acquire()

        # Execute with retry logic
        def _delete():
            return (
                self.service.permissions()
                .delete(fileId=file_id, permissionId=permission_id, **kwargs)
                .execute()
            )

        return self._retry_with_exponential_backoff(_delete)
