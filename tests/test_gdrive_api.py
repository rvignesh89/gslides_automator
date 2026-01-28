"""
Tests for Google Drive API rate limiting service.
"""

from __future__ import annotations

import logging
import time
from unittest.mock import MagicMock, Mock, patch

import pytest
from googleapiclient.errors import HttpError

from gslides_automator.gdrive_api import GDriveAPI
from gslides_automator.leaky_bucket import LeakyBucket


class TestGDriveAPI:
    """Test GDriveAPI rate limiting."""

    @patch("gslides_automator.gdrive_api.build")
    def test_list_files(self, mock_build):
        """Test list_files method."""
        # Setup mocks
        mock_service = MagicMock()
        mock_files = MagicMock()
        mock_list = MagicMock()
        mock_execute = MagicMock(return_value={"files": [{"id": "file1", "name": "test.txt"}]})

        mock_build.return_value = mock_service
        mock_service.files.return_value = mock_files
        mock_files.list.return_value = mock_list
        mock_list.execute = mock_execute

        mock_creds = MagicMock()
        api = GDriveAPI(mock_creds)

        # Call list_files
        result = api.list_files(query="name='test.txt'")

        # Verify
        assert result == {"files": [{"id": "file1", "name": "test.txt"}]}
        mock_files.list.assert_called_once_with(q="name='test.txt'")

    @patch("gslides_automator.gdrive_api.build")
    def test_get_file(self, mock_build):
        """Test get_file method."""
        # Setup mocks
        mock_service = MagicMock()
        mock_files = MagicMock()
        mock_get = MagicMock()
        mock_execute = MagicMock(return_value={"id": "file1", "name": "test.txt"})

        mock_build.return_value = mock_service
        mock_service.files.return_value = mock_files
        mock_files.get.return_value = mock_get
        mock_get.execute = mock_execute

        mock_creds = MagicMock()
        api = GDriveAPI(mock_creds)

        # Call get_file
        result = api.get_file("file1")

        # Verify
        assert result == {"id": "file1", "name": "test.txt"}
        mock_files.get.assert_called_once_with(fileId="file1")

    @patch("gslides_automator.gdrive_api.build")
    def test_create_file(self, mock_build):
        """Test create_file method."""
        # Setup mocks
        mock_service = MagicMock()
        mock_files = MagicMock()
        mock_create = MagicMock()
        mock_execute = MagicMock(return_value={"id": "file1", "name": "test.txt"})

        mock_build.return_value = mock_service
        mock_service.files.return_value = mock_files
        mock_files.create.return_value = mock_create
        mock_create.execute = mock_execute

        mock_creds = MagicMock()
        api = GDriveAPI(mock_creds)

        # Call create_file
        body = {"name": "test.txt", "mimeType": "text/plain"}
        result = api.create_file(body)

        # Verify
        assert result == {"id": "file1", "name": "test.txt"}
        mock_files.create.assert_called_once_with(body=body)

    @patch("gslides_automator.gdrive_api.build")
    def test_update_file(self, mock_build):
        """Test update_file method."""
        # Setup mocks
        mock_service = MagicMock()
        mock_files = MagicMock()
        mock_update = MagicMock()
        mock_execute = MagicMock(return_value={"id": "file1", "name": "updated.txt"})

        mock_build.return_value = mock_service
        mock_service.files.return_value = mock_files
        mock_files.update.return_value = mock_update
        mock_update.execute = mock_execute

        mock_creds = MagicMock()
        api = GDriveAPI(mock_creds)

        # Call update_file
        body = {"name": "updated.txt"}
        result = api.update_file("file1", body)

        # Verify
        assert result == {"id": "file1", "name": "updated.txt"}
        mock_files.update.assert_called_once_with(fileId="file1", body=body)

    @patch("gslides_automator.gdrive_api.build")
    def test_delete_file(self, mock_build):
        """Test delete_file method."""
        # Setup mocks
        mock_service = MagicMock()
        mock_files = MagicMock()
        mock_delete = MagicMock()
        mock_execute = MagicMock(return_value=None)

        mock_build.return_value = mock_service
        mock_service.files.return_value = mock_files
        mock_files.delete.return_value = mock_delete
        mock_delete.execute = mock_execute

        mock_creds = MagicMock()
        api = GDriveAPI(mock_creds)

        # Call delete_file
        result = api.delete_file("file1")

        # Verify
        assert result is None
        mock_files.delete.assert_called_once_with(fileId="file1")

    @patch("gslides_automator.gdrive_api.build")
    def test_get_media(self, mock_build):
        """Test get_media method returns request object."""
        # Setup mocks
        mock_service = MagicMock()
        mock_files = MagicMock()
        mock_get_media = MagicMock()

        mock_build.return_value = mock_service
        mock_service.files.return_value = mock_files
        mock_files.get_media.return_value = mock_get_media

        mock_creds = MagicMock()
        api = GDriveAPI(mock_creds)

        # Call get_media
        result = api.get_media("file1")

        # Verify it returns the request object (not executed)
        assert result is mock_get_media
        mock_files.get_media.assert_called_once_with(fileId="file1")

    @patch("gslides_automator.gdrive_api.build")
    def test_export_file(self, mock_build):
        """Test export_file method returns request object."""
        # Setup mocks
        mock_service = MagicMock()
        mock_files = MagicMock()
        mock_export = MagicMock()

        mock_build.return_value = mock_service
        mock_service.files.return_value = mock_files
        mock_files.export.return_value = mock_export

        mock_creds = MagicMock()
        api = GDriveAPI(mock_creds)

        # Call export_file
        result = api.export_file("file1", "application/pdf")

        # Verify it returns the request object (not executed)
        assert result is mock_export
        mock_files.export.assert_called_once_with(fileId="file1", mimeType="application/pdf")

    @patch("gslides_automator.gdrive_api.build")
    def test_copy_file(self, mock_build):
        """Test copy_file method."""
        # Setup mocks
        mock_service = MagicMock()
        mock_files = MagicMock()
        mock_copy = MagicMock()
        mock_execute = MagicMock(return_value={"id": "file2", "name": "copied.txt"})

        mock_build.return_value = mock_service
        mock_service.files.return_value = mock_files
        mock_files.copy.return_value = mock_copy
        mock_copy.execute = mock_execute

        mock_creds = MagicMock()
        api = GDriveAPI(mock_creds)

        # Call copy_file
        body = {"name": "copied.txt"}
        result = api.copy_file("file1", body)

        # Verify
        assert result == {"id": "file2", "name": "copied.txt"}
        mock_files.copy.assert_called_once_with(fileId="file1", body=body)

    @patch("gslides_automator.gdrive_api.build")
    def test_list_permissions(self, mock_build):
        """Test list_permissions method."""
        # Setup mocks
        mock_service = MagicMock()
        mock_permissions = MagicMock()
        mock_list = MagicMock()
        mock_execute = MagicMock(return_value={"permissions": [{"id": "perm1", "type": "user"}]})

        mock_build.return_value = mock_service
        mock_service.permissions.return_value = mock_permissions
        mock_permissions.list.return_value = mock_list
        mock_list.execute = mock_execute

        mock_creds = MagicMock()
        api = GDriveAPI(mock_creds)

        # Call list_permissions
        result = api.list_permissions("file1")

        # Verify
        assert result == {"permissions": [{"id": "perm1", "type": "user"}]}
        mock_permissions.list.assert_called_once_with(fileId="file1")

    @patch("gslides_automator.gdrive_api.build")
    def test_create_permission(self, mock_build):
        """Test create_permission method."""
        # Setup mocks
        mock_service = MagicMock()
        mock_permissions = MagicMock()
        mock_create = MagicMock()
        mock_execute = MagicMock(return_value={"id": "perm1", "type": "anyone"})

        mock_build.return_value = mock_service
        mock_service.permissions.return_value = mock_permissions
        mock_permissions.create.return_value = mock_create
        mock_create.execute = mock_execute

        mock_creds = MagicMock()
        api = GDriveAPI(mock_creds)

        # Call create_permission
        body = {"type": "anyone", "role": "reader"}
        result = api.create_permission("file1", body)

        # Verify
        assert result == {"id": "perm1", "type": "anyone"}
        mock_permissions.create.assert_called_once_with(fileId="file1", body=body)

    @patch("gslides_automator.gdrive_api.build")
    def test_429_error_retry(self, mock_build):
        """Test that 429 errors trigger retry with exponential backoff."""
        # Setup mocks
        mock_service = MagicMock()
        mock_files = MagicMock()
        mock_get = MagicMock()

        # First call raises 429, second succeeds
        error_429 = HttpError(Mock(status=429), b"Rate limit exceeded")
        mock_execute = MagicMock(
            side_effect=[error_429, {"id": "file1", "name": "test.txt"}]
        )

        mock_build.return_value = mock_service
        mock_service.files.return_value = mock_files
        mock_files.get.return_value = mock_get
        mock_get.execute = mock_execute

        mock_creds = MagicMock()
        api = GDriveAPI(mock_creds)

        # Call get_file
        with patch("time.sleep"):  # Mock sleep to speed up test
            result = api.get_file("file1")

        # Verify retry occurred
        assert result == {"id": "file1", "name": "test.txt"}
        assert mock_execute.call_count == 2

    @patch("gslides_automator.gdrive_api.build")
    def test_500_error_retry(self, mock_build):
        """Test that 500 errors trigger retry."""
        # Setup mocks
        mock_service = MagicMock()
        mock_files = MagicMock()
        mock_get = MagicMock()

        # First call raises 500, second succeeds
        error_500 = HttpError(Mock(status=500), b"Server error")
        mock_execute = MagicMock(
            side_effect=[error_500, {"id": "file1", "name": "test.txt"}]
        )

        mock_build.return_value = mock_service
        mock_service.files.return_value = mock_files
        mock_files.get.return_value = mock_get
        mock_get.execute = mock_execute

        mock_creds = MagicMock()
        api = GDriveAPI(mock_creds)

        # Call get_file
        with patch("time.sleep"):  # Mock sleep to speed up test
            result = api.get_file("file1")

        # Verify retry occurred
        assert result == {"id": "file1", "name": "test.txt"}
        assert mock_execute.call_count == 2

    @patch("gslides_automator.gdrive_api.build")
    def test_non_retryable_error(self, mock_build):
        """Test that non-retryable errors are not retried."""
        # Setup mocks
        mock_service = MagicMock()
        mock_files = MagicMock()
        mock_get = MagicMock()

        # 404 error should not be retried
        error_404 = HttpError(Mock(status=404), b"Not found")
        mock_execute = MagicMock(side_effect=error_404)

        mock_build.return_value = mock_service
        mock_service.files.return_value = mock_files
        mock_files.get.return_value = mock_get
        mock_get.execute = mock_execute

        mock_creds = MagicMock()
        api = GDriveAPI(mock_creds)

        # Call get_file - should raise immediately
        with pytest.raises(HttpError):
            api.get_file("file1")

        # Verify no retry occurred
        assert mock_execute.call_count == 1

    @patch("gslides_automator.gdrive_api.build")
    def test_rate_limiting_with_reduced_limits(self, mock_build):
        """Test rate limiting with reduced limits."""
        # Setup mocks
        mock_service = MagicMock()
        mock_files = MagicMock()
        mock_get = MagicMock()
        mock_execute = MagicMock(return_value={"id": "file1", "name": "test.txt"})

        mock_build.return_value = mock_service
        mock_service.files.return_value = mock_files
        mock_files.get.return_value = mock_get
        mock_get.execute = mock_execute

        mock_creds = MagicMock()
        api = GDriveAPI(mock_creds)

        # Replace token bucket with reduced limits (2 per minute = 30 seconds between calls)
        api.token_bucket = LeakyBucket(read_rate=2.0, write_rate=None)

        # First call should be immediate
        result1 = api.get_file("file1")
        assert result1 == {"id": "file1", "name": "test.txt"}

        # Second call should wait approximately 30 seconds
        start_time = time.time()
        result2 = api.get_file("file1")
        elapsed = time.time() - start_time

        # Should have waited approximately 30 seconds
        assert elapsed >= 29.0  # At least 29 seconds
        assert elapsed < 35.0  # But not too long
        assert result2 == {"id": "file1", "name": "test.txt"}

    @patch("gslides_automator.gdrive_api.build")
    def test_debug_logging_on_rate_limit(self, mock_build, caplog):
        """Test that debug logs are emitted when rate limiting occurs."""
        # Setup mocks
        mock_service = MagicMock()
        mock_files = MagicMock()
        mock_get = MagicMock()
        mock_execute = MagicMock(return_value={"id": "file1", "name": "test.txt"})

        mock_build.return_value = mock_service
        mock_service.files.return_value = mock_files
        mock_files.get.return_value = mock_get
        mock_get.execute = mock_execute

        mock_creds = MagicMock()
        api = GDriveAPI(mock_creds)

        # Replace token bucket with very low rate to trigger rate limiting
        api.token_bucket = LeakyBucket(read_rate=2.0, write_rate=None)

        # Enable debug logging
        with caplog.at_level(logging.DEBUG):
            # First call - immediate, no wait log
            api.get_file("file1")
            # Second call - should wait and log
            api.get_file("file1")

        # Check that debug logs were emitted for the second call
        assert any("Rate limit - waiting" in record.message and "operation" in record.message for record in caplog.records)
        assert any("Rate limit - operation allowed" in record.message for record in caplog.records)

    @patch("gslides_automator.gdrive_api.build")
    def test_no_logging_when_no_rate_limit(self, mock_build, caplog):
        """Test that no debug logs are emitted when rate limiting doesn't occur."""
        # Setup mocks
        mock_service = MagicMock()
        mock_files = MagicMock()
        mock_get = MagicMock()
        mock_execute = MagicMock(return_value={"id": "file1", "name": "test.txt"})

        mock_build.return_value = mock_service
        mock_service.files.return_value = mock_files
        mock_files.get.return_value = mock_get
        mock_get.execute = mock_execute

        mock_creds = MagicMock()
        api = GDriveAPI(mock_creds)

        # Enable debug logging
        with caplog.at_level(logging.DEBUG):
            api.get_file("file1")

        # Check that no rate limit logs were emitted (tokens available immediately)
        rate_limit_logs = [
            record for record in caplog.records
            if "Rate limit" in record.message
        ]
        assert len(rate_limit_logs) == 0
