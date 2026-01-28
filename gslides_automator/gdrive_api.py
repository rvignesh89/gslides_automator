#!/usr/bin/env python3
"""
Google Drive API service with token bucket rate limiting.

This module provides a GDriveAPI service that wraps Google Drive API calls
with rate limiting (12,000 queries/60s) and automatic retry logic for 429 errors.

The module provides a shared service instance via get_shared_drive_service() for
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
_service: Optional[GDriveAPI] = None
_service_lock = threading.Lock()


class GDriveAPI:
    """
    Service that wraps Google Drive API with rate limiting and retry logic.

    Provides rate-limited access to Google Drive API operations with automatic
    retry on 429 errors using exponential backoff.

    Note: Google Drive API uses a single quota bucket (no read/write distinction).
    """

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
        self.token_bucket = LeakyBucket(read_rate=12000.0, write_rate=None)

    def get_shared_drive_service(creds) -> GDriveAPI:
        """
        Get or create the shared GDriveAPI service instance.

        This function provides a module-level shared service instance that maintains
        rate limiting across all API calls in the application.

        Args:
            creds: Google OAuth credentials

        Returns:
            GDriveAPI instance (shared across the application)
        """
        global _service
        if _service is None:
            with _service_lock:
                # Double-check pattern for thread safety
                if _service is None:
                    _service = GDriveAPI(creds)
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

        return retry_with_exponential_backoff(_list)

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

        return retry_with_exponential_backoff(_get)

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

        return retry_with_exponential_backoff(_create)

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

        return retry_with_exponential_backoff(_update)

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

        return retry_with_exponential_backoff(_delete)

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

        return retry_with_exponential_backoff(_copy)

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

        return retry_with_exponential_backoff(_list)

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

        return retry_with_exponential_backoff(_create)

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

        return retry_with_exponential_backoff(_delete)
