"""
Tests for Google Sheets API rate limiting service.
"""

from __future__ import annotations

import logging
import time
from unittest.mock import MagicMock, Mock, patch

import pytest
from googleapiclient.errors import HttpError

from gslides_automator.gsheets_api import GSheetsAPI
from gslides_automator.token_bucket import TokenBucket


class TestGSheetsAPI:
    """Test GSheetsAPI singleton and rate limiting."""

    def test_singleton_pattern(self):
        """Test that GSheetsAPI is a singleton."""
        mock_creds = MagicMock()

        # Reset singleton
        GSheetsAPI._instance = None

        instance1 = GSheetsAPI.get_instance(mock_creds)
        instance2 = GSheetsAPI.get_instance(mock_creds)

        assert instance1 is instance2

    @patch("gslides_automator.gsheets_api.build")
    def test_get_spreadsheet(self, mock_build):
        """Test get_spreadsheet method."""
        # Setup mocks
        mock_service = MagicMock()
        mock_spreadsheets = MagicMock()
        mock_get = MagicMock()
        mock_execute = MagicMock(return_value={"spreadsheetId": "test-id", "sheets": []})

        mock_build.return_value = mock_service
        mock_service.spreadsheets.return_value = mock_spreadsheets
        mock_spreadsheets.get.return_value = mock_get
        mock_get.execute = mock_execute

        # Reset singleton
        GSheetsAPI._instance = None

        mock_creds = MagicMock()
        api = GSheetsAPI.get_instance(mock_creds)

        # Call get_spreadsheet
        result = api.get_spreadsheet("test-id")

        # Verify
        assert result == {"spreadsheetId": "test-id", "sheets": []}
        mock_spreadsheets.get.assert_called_once_with(spreadsheetId="test-id")

    @patch("gslides_automator.gsheets_api.build")
    def test_get_values(self, mock_build):
        """Test get_values method."""
        # Setup mocks
        mock_service = MagicMock()
        mock_spreadsheets = MagicMock()
        mock_values = MagicMock()
        mock_get = MagicMock()
        mock_execute = MagicMock(return_value={"values": [["A1", "B1"], ["A2", "B2"]]})

        mock_build.return_value = mock_service
        mock_service.spreadsheets.return_value = mock_spreadsheets
        mock_spreadsheets.values.return_value = mock_values
        mock_values.get.return_value = mock_get
        mock_get.execute = mock_execute

        # Reset singleton
        GSheetsAPI._instance = None

        mock_creds = MagicMock()
        api = GSheetsAPI.get_instance(mock_creds)

        # Call get_values
        result = api.get_values("test-id", "Sheet1!A1:B2")

        # Verify
        assert result == {"values": [["A1", "B1"], ["A2", "B2"]]}
        mock_values.get.assert_called_once_with(spreadsheetId="test-id", range="Sheet1!A1:B2")

    @patch("gslides_automator.gsheets_api.build")
    def test_update_values(self, mock_build):
        """Test update_values method."""
        # Setup mocks
        mock_service = MagicMock()
        mock_spreadsheets = MagicMock()
        mock_values = MagicMock()
        mock_update = MagicMock()
        mock_execute = MagicMock(return_value={"updatedCells": 4})

        mock_build.return_value = mock_service
        mock_service.spreadsheets.return_value = mock_spreadsheets
        mock_spreadsheets.values.return_value = mock_values
        mock_values.update.return_value = mock_update
        mock_update.execute = mock_execute

        # Reset singleton
        GSheetsAPI._instance = None

        mock_creds = MagicMock()
        api = GSheetsAPI.get_instance(mock_creds)

        # Call update_values
        values = [["A1", "B1"], ["A2", "B2"]]
        result = api.update_values("test-id", "Sheet1!A1:B2", values)

        # Verify
        assert result == {"updatedCells": 4}
        mock_values.update.assert_called_once()
        call_kwargs = mock_values.update.call_args[1]
        assert call_kwargs["spreadsheetId"] == "test-id"
        assert call_kwargs["range"] == "Sheet1!A1:B2"
        assert call_kwargs["valueInputOption"] == "RAW"
        assert call_kwargs["body"]["values"] == values

    @patch("gslides_automator.gsheets_api.build")
    def test_batch_update_values(self, mock_build):
        """Test batch_update_values method."""
        # Setup mocks
        mock_service = MagicMock()
        mock_spreadsheets = MagicMock()
        mock_values = MagicMock()
        mock_batch_update = MagicMock()
        mock_execute = MagicMock(return_value={"responses": []})

        mock_build.return_value = mock_service
        mock_service.spreadsheets.return_value = mock_spreadsheets
        mock_spreadsheets.values.return_value = mock_values
        mock_values.batchUpdate.return_value = mock_batch_update
        mock_batch_update.execute = mock_execute

        # Reset singleton
        GSheetsAPI._instance = None

        mock_creds = MagicMock()
        api = GSheetsAPI.get_instance(mock_creds)

        # Call batch_update_values
        data = [{"range": "Sheet1!A1:B2", "values": [["A1", "B1"]]}]
        result = api.batch_update_values("test-id", data)

        # Verify
        assert result == {"responses": []}
        mock_values.batchUpdate.assert_called_once()

    @patch("gslides_automator.gsheets_api.build")
    def test_batch_update(self, mock_build):
        """Test batch_update method."""
        # Setup mocks
        mock_service = MagicMock()
        mock_spreadsheets = MagicMock()
        mock_batch_update = MagicMock()
        mock_execute = MagicMock(return_value={"replies": []})

        mock_build.return_value = mock_service
        mock_service.spreadsheets.return_value = mock_spreadsheets
        mock_spreadsheets.batchUpdate.return_value = mock_batch_update
        mock_batch_update.execute = mock_execute

        # Reset singleton
        GSheetsAPI._instance = None

        mock_creds = MagicMock()
        api = GSheetsAPI.get_instance(mock_creds)

        # Call batch_update
        body = {"requests": [{"addSheet": {"properties": {"title": "NewSheet"}}}]}
        result = api.batch_update("test-id", body)

        # Verify
        assert result == {"replies": []}
        mock_spreadsheets.batchUpdate.assert_called_once_with(spreadsheetId="test-id", body=body)

    @patch("gslides_automator.gsheets_api.build")
    def test_429_error_retry(self, mock_build):
        """Test that 429 errors trigger retry with exponential backoff."""
        # Setup mocks
        mock_service = MagicMock()
        mock_spreadsheets = MagicMock()
        mock_get = MagicMock()

        # First call raises 429, second succeeds
        error_429 = HttpError(Mock(status=429), b"Rate limit exceeded")
        mock_execute = MagicMock(
            side_effect=[error_429, {"spreadsheetId": "test-id", "sheets": []}]
        )

        mock_build.return_value = mock_service
        mock_service.spreadsheets.return_value = mock_spreadsheets
        mock_spreadsheets.get.return_value = mock_get
        mock_get.execute = mock_execute

        # Reset singleton
        GSheetsAPI._instance = None

        mock_creds = MagicMock()
        api = GSheetsAPI.get_instance(mock_creds)

        # Call get_spreadsheet
        with patch("time.sleep"):  # Mock sleep to speed up test
            result = api.get_spreadsheet("test-id")

        # Verify retry occurred
        assert result == {"spreadsheetId": "test-id", "sheets": []}
        assert mock_execute.call_count == 2

    @patch("gslides_automator.gsheets_api.build")
    def test_500_error_retry(self, mock_build):
        """Test that 500 errors trigger retry."""
        # Setup mocks
        mock_service = MagicMock()
        mock_spreadsheets = MagicMock()
        mock_get = MagicMock()

        # First call raises 500, second succeeds
        error_500 = HttpError(Mock(status=500), b"Server error")
        mock_execute = MagicMock(
            side_effect=[error_500, {"spreadsheetId": "test-id", "sheets": []}]
        )

        mock_build.return_value = mock_service
        mock_service.spreadsheets.return_value = mock_spreadsheets
        mock_spreadsheets.get.return_value = mock_get
        mock_get.execute = mock_execute

        # Reset singleton
        GSheetsAPI._instance = None

        mock_creds = MagicMock()
        api = GSheetsAPI.get_instance(mock_creds)

        # Call get_spreadsheet
        with patch("time.sleep"):  # Mock sleep to speed up test
            result = api.get_spreadsheet("test-id")

        # Verify retry occurred
        assert result == {"spreadsheetId": "test-id", "sheets": []}
        assert mock_execute.call_count == 2

    @patch("gslides_automator.gsheets_api.build")
    def test_non_retryable_error(self, mock_build):
        """Test that non-retryable errors are not retried."""
        # Setup mocks
        mock_service = MagicMock()
        mock_spreadsheets = MagicMock()
        mock_get = MagicMock()

        # 404 error should not be retried
        error_404 = HttpError(Mock(status=404), b"Not found")
        mock_execute = MagicMock(side_effect=error_404)

        mock_build.return_value = mock_service
        mock_service.spreadsheets.return_value = mock_spreadsheets
        mock_spreadsheets.get.return_value = mock_get
        mock_get.execute = mock_execute

        # Reset singleton
        GSheetsAPI._instance = None

        mock_creds = MagicMock()
        api = GSheetsAPI.get_instance(mock_creds)

        # Call get_spreadsheet - should raise immediately
        with pytest.raises(HttpError):
            api.get_spreadsheet("test-id")

        # Verify no retry occurred
        assert mock_execute.call_count == 1

    @patch("gslides_automator.gsheets_api.build")
    def test_rate_limiting_with_reduced_limits(self, mock_build):
        """Test rate limiting with reduced limits."""
        # Setup mocks
        mock_service = MagicMock()
        mock_spreadsheets = MagicMock()
        mock_get = MagicMock()
        mock_execute = MagicMock(return_value={"spreadsheetId": "test-id", "sheets": []})

        mock_build.return_value = mock_service
        mock_service.spreadsheets.return_value = mock_spreadsheets
        mock_spreadsheets.get.return_value = mock_get
        mock_get.execute = mock_execute

        # Reset singleton
        GSheetsAPI._instance = None

        mock_creds = MagicMock()
        api = GSheetsAPI.get_instance(mock_creds)

        # Replace token bucket with reduced limits (2 reads/min = 1 read per 30 seconds)
        api.token_bucket = TokenBucket(read_rate=2.0, write_rate=1.0)
        api.token_bucket.read_tokens = 0.0  # Start with no tokens

        # First call should block
        start_time = time.time()
        result = api.get_spreadsheet("test-id")
        elapsed = time.time() - start_time

        # Should have waited for token
        assert elapsed >= 25.0  # At least 25 seconds
        assert result == {"spreadsheetId": "test-id", "sheets": []}

    @patch("gslides_automator.gsheets_api.build")
    def test_debug_logging_on_rate_limit(self, mock_build, caplog):
        """Test that debug logs are emitted when rate limiting occurs."""
        # Setup mocks
        mock_service = MagicMock()
        mock_spreadsheets = MagicMock()
        mock_get = MagicMock()
        mock_execute = MagicMock(return_value={"spreadsheetId": "test-id", "sheets": []})

        mock_build.return_value = mock_service
        mock_service.spreadsheets.return_value = mock_spreadsheets
        mock_spreadsheets.get.return_value = mock_get
        mock_get.execute = mock_execute

        # Reset singleton
        GSheetsAPI._instance = None

        mock_creds = MagicMock()
        api = GSheetsAPI.get_instance(mock_creds)

        # Replace token bucket with very low rate to trigger rate limiting
        api.token_bucket = TokenBucket(read_rate=2.0, write_rate=1.0)
        api.token_bucket.read_tokens = 0.0  # Start with no tokens

        # Enable debug logging
        with caplog.at_level(logging.DEBUG):
            api.get_spreadsheet("test-id")

        # Check that debug logs were emitted
        assert any("Rate limit - waiting for read token" in record.message for record in caplog.records)
        assert any("Rate limit - read token acquired, proceeding" in record.message for record in caplog.records)

    @patch("gslides_automator.gsheets_api.build")
    def test_no_logging_when_no_rate_limit(self, mock_build, caplog):
        """Test that no debug logs are emitted when rate limiting doesn't occur."""
        # Setup mocks
        mock_service = MagicMock()
        mock_spreadsheets = MagicMock()
        mock_get = MagicMock()
        mock_execute = MagicMock(return_value={"spreadsheetId": "test-id", "sheets": []})

        mock_build.return_value = mock_service
        mock_service.spreadsheets.return_value = mock_spreadsheets
        mock_spreadsheets.get.return_value = mock_get
        mock_get.execute = mock_execute

        # Reset singleton
        GSheetsAPI._instance = None

        mock_creds = MagicMock()
        api = GSheetsAPI.get_instance(mock_creds)

        # Enable debug logging
        with caplog.at_level(logging.DEBUG):
            api.get_spreadsheet("test-id")

        # Check that no rate limit logs were emitted (tokens available immediately)
        rate_limit_logs = [
            record for record in caplog.records
            if "Rate limit" in record.message
        ]
        assert len(rate_limit_logs) == 0
