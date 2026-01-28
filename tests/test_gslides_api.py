"""
Tests for Google Slides API rate limiting service.
"""

from __future__ import annotations

import logging
import threading
import time
from unittest.mock import MagicMock, Mock, patch

import pytest
from googleapiclient.errors import HttpError

from gslides_automator.gslides_api import GSlidesAPI, TokenBucket


class TestTokenBucket:
    """Test TokenBucket rate limiting."""

    def test_token_bucket_initialization(self):
        """Test token bucket initializes with correct capacity."""
        bucket = TokenBucket(read_rate=600.0, write_rate=60.0)
        assert bucket.read_tokens == 600.0
        assert bucket.write_tokens == 60.0
        assert bucket.read_capacity == 600.0
        assert bucket.write_capacity == 60.0

    def test_token_bucket_custom_capacity(self):
        """Test token bucket with custom capacity."""
        bucket = TokenBucket(
            read_rate=600.0, write_rate=60.0, read_capacity=1000.0, write_capacity=100.0
        )
        assert bucket.read_tokens == 1000.0
        assert bucket.write_tokens == 100.0

    def test_acquire_read_token(self):
        """Test acquiring a read token."""
        bucket = TokenBucket(read_rate=600.0, write_rate=60.0)
        # Should not block when tokens are available
        start_time = time.time()
        bucket.acquire("read")
        elapsed = time.time() - start_time
        assert elapsed < 0.1  # Should be very fast
        assert bucket.read_tokens == 599.0

    def test_acquire_write_token(self):
        """Test acquiring a write token."""
        bucket = TokenBucket(read_rate=600.0, write_rate=60.0)
        # Should not block when tokens are available
        start_time = time.time()
        bucket.acquire("write")
        elapsed = time.time() - start_time
        assert elapsed < 0.1  # Should be very fast
        assert bucket.write_tokens == 59.0

    def test_rate_limiting_read(self):
        """Test that read operations are rate limited."""
        # Use very low rate for testing (2 reads per minute = 1 read per 30 seconds)
        bucket = TokenBucket(read_rate=2.0, write_rate=60.0)

        # Consume all tokens
        bucket.read_tokens = 0.0

        # Acquiring should block until token is available
        start_time = time.time()
        bucket.acquire("read")
        elapsed = time.time() - start_time

        # Should have waited approximately 30 seconds (1 token per 30 seconds)
        # Allow some tolerance for test execution time
        assert elapsed >= 25.0  # At least 25 seconds
        assert elapsed < 40.0  # But not too long

    def test_rate_limiting_write(self):
        """Test that write operations are rate limited."""
        # Use very low rate for testing (1 write per minute = 1 write per 60 seconds)
        bucket = TokenBucket(read_rate=600.0, write_rate=1.0)

        # Consume all tokens
        bucket.write_tokens = 0.0

        # Acquiring should block until token is available
        start_time = time.time()
        bucket.acquire("write")
        elapsed = time.time() - start_time

        # Should have waited approximately 60 seconds
        # Allow some tolerance for test execution time
        assert elapsed >= 55.0  # At least 55 seconds
        assert elapsed < 70.0  # But not too long

    def test_token_refill(self):
        """Test that tokens refill over time."""
        bucket = TokenBucket(read_rate=60.0, write_rate=6.0)  # 1 per second for reads, 0.1 per second for writes

        # Consume all tokens
        bucket.read_tokens = 0.0
        bucket.write_tokens = 0.0

        # Wait a bit
        time.sleep(1.1)

        # Refill should have occurred
        bucket._refill()
        assert bucket.read_tokens > 0.0
        assert bucket.write_tokens > 0.0

    def test_invalid_operation_type(self):
        """Test that invalid operation type raises ValueError."""
        bucket = TokenBucket(read_rate=600.0, write_rate=60.0)
        with pytest.raises(ValueError, match="operation_type must be 'read' or 'write'"):
            bucket.acquire("invalid")

    def test_thread_safety(self):
        """Test that token bucket is thread-safe."""
        bucket = TokenBucket(read_rate=1000.0, write_rate=1000.0)
        results = []

        def acquire_tokens():
            for _ in range(10):
                bucket.acquire("read")
                results.append(threading.current_thread().ident)

        threads = [threading.Thread(target=acquire_tokens) for _ in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Should have acquired 50 tokens total (5 threads * 10 tokens)
        # Tokens may have refilled slightly during execution, so check range
        assert 950.0 <= bucket.read_tokens <= 1000.0  # Started with 1000, consumed 50 (may have refilled)
        assert len(results) == 50


class TestGSlidesAPI:
    """Test GSlidesAPI singleton and rate limiting."""

    def test_singleton_pattern(self):
        """Test that GSlidesAPI is a singleton."""
        mock_creds = MagicMock()

        # Reset singleton
        GSlidesAPI._instance = None

        instance1 = GSlidesAPI.get_instance(mock_creds)
        instance2 = GSlidesAPI.get_instance(mock_creds)

        assert instance1 is instance2

    @patch("gslides_automator.gslides_api.build")
    def test_get_presentation(self, mock_build):
        """Test get_presentation method."""
        # Setup mocks
        mock_service = MagicMock()
        mock_presentations = MagicMock()
        mock_get = MagicMock()
        mock_execute = MagicMock(return_value={"presentationId": "test-id", "slides": []})

        mock_build.return_value = mock_service
        mock_service.presentations.return_value = mock_presentations
        mock_presentations.get.return_value = mock_get
        mock_get.execute = mock_execute

        # Reset singleton
        GSlidesAPI._instance = None

        mock_creds = MagicMock()
        api = GSlidesAPI.get_instance(mock_creds)

        # Call get_presentation
        result = api.get_presentation("test-id")

        # Verify
        assert result == {"presentationId": "test-id", "slides": []}
        mock_presentations.get.assert_called_once_with(presentationId="test-id")
        mock_execute.assert_called_once()

    @patch("gslides_automator.gslides_api.build")
    def test_batch_update(self, mock_build):
        """Test batch_update method."""
        # Setup mocks
        mock_service = MagicMock()
        mock_presentations = MagicMock()
        mock_batch_update = MagicMock()
        mock_execute = MagicMock(return_value={"replies": []})

        mock_build.return_value = mock_service
        mock_service.presentations.return_value = mock_presentations
        mock_presentations.batchUpdate.return_value = mock_batch_update
        mock_batch_update.execute = mock_execute

        # Reset singleton
        GSlidesAPI._instance = None

        mock_creds = MagicMock()
        api = GSlidesAPI.get_instance(mock_creds)

        # Call batch_update
        body = {"requests": [{"createSlide": {}}]}
        result = api.batch_update("test-id", body)

        # Verify
        assert result == {"replies": []}
        mock_presentations.batchUpdate.assert_called_once_with(
            presentationId="test-id", body=body
        )
        mock_execute.assert_called_once()

    @patch("gslides_automator.gslides_api.build")
    def test_429_error_retry(self, mock_build):
        """Test that 429 errors trigger retry with exponential backoff."""
        # Setup mocks
        mock_service = MagicMock()
        mock_presentations = MagicMock()
        mock_get = MagicMock()

        # First call raises 429, second succeeds
        error_429 = HttpError(Mock(status=429), b"Rate limit exceeded")
        mock_execute = MagicMock(
            side_effect=[error_429, {"presentationId": "test-id", "slides": []}]
        )

        mock_build.return_value = mock_service
        mock_service.presentations.return_value = mock_presentations
        mock_presentations.get.return_value = mock_get
        mock_get.execute = mock_execute

        # Reset singleton
        GSlidesAPI._instance = None

        mock_creds = MagicMock()
        api = GSlidesAPI.get_instance(mock_creds)

        # Call get_presentation
        with patch("time.sleep"):  # Mock sleep to speed up test
            result = api.get_presentation("test-id")

        # Verify retry occurred
        assert result == {"presentationId": "test-id", "slides": []}
        assert mock_execute.call_count == 2

    @patch("gslides_automator.gslides_api.build")
    def test_500_error_retry(self, mock_build):
        """Test that 500 errors trigger retry."""
        # Setup mocks
        mock_service = MagicMock()
        mock_presentations = MagicMock()
        mock_get = MagicMock()

        # First call raises 500, second succeeds
        error_500 = HttpError(Mock(status=500), b"Server error")
        mock_execute = MagicMock(
            side_effect=[error_500, {"presentationId": "test-id", "slides": []}]
        )

        mock_build.return_value = mock_service
        mock_service.presentations.return_value = mock_presentations
        mock_presentations.get.return_value = mock_get
        mock_get.execute = mock_execute

        # Reset singleton
        GSlidesAPI._instance = None

        mock_creds = MagicMock()
        api = GSlidesAPI.get_instance(mock_creds)

        # Call get_presentation
        with patch("time.sleep"):  # Mock sleep to speed up test
            result = api.get_presentation("test-id")

        # Verify retry occurred
        assert result == {"presentationId": "test-id", "slides": []}
        assert mock_execute.call_count == 2

    @patch("gslides_automator.gslides_api.build")
    def test_non_retryable_error(self, mock_build):
        """Test that non-retryable errors are not retried."""
        # Setup mocks
        mock_service = MagicMock()
        mock_presentations = MagicMock()
        mock_get = MagicMock()

        # 404 error should not be retried
        error_404 = HttpError(Mock(status=404), b"Not found")
        mock_execute = MagicMock(side_effect=error_404)

        mock_build.return_value = mock_service
        mock_service.presentations.return_value = mock_presentations
        mock_presentations.get.return_value = mock_get
        mock_get.execute = mock_execute

        # Reset singleton
        GSlidesAPI._instance = None

        mock_creds = MagicMock()
        api = GSlidesAPI.get_instance(mock_creds)

        # Call get_presentation - should raise immediately
        with pytest.raises(HttpError):
            api.get_presentation("test-id")

        # Verify no retry occurred
        assert mock_execute.call_count == 1

    @patch("gslides_automator.gslides_api.build")
    def test_rate_limiting_with_reduced_limits(self, mock_build):
        """Test rate limiting with reduced limits."""
        # Setup mocks
        mock_service = MagicMock()
        mock_presentations = MagicMock()
        mock_get = MagicMock()
        mock_execute = MagicMock(return_value={"presentationId": "test-id", "slides": []})

        mock_build.return_value = mock_service
        mock_service.presentations.return_value = mock_presentations
        mock_presentations.get.return_value = mock_get
        mock_get.execute = mock_execute

        # Reset singleton
        GSlidesAPI._instance = None

        mock_creds = MagicMock()
        api = GSlidesAPI.get_instance(mock_creds)

        # Replace token bucket with reduced limits (2 reads/min = 1 read per 30 seconds)
        api.token_bucket = TokenBucket(read_rate=2.0, write_rate=1.0)
        api.token_bucket.read_tokens = 0.0  # Start with no tokens

        # First call should block
        start_time = time.time()
        result = api.get_presentation("test-id")
        elapsed = time.time() - start_time

        # Should have waited for token
        assert elapsed >= 25.0  # At least 25 seconds
        assert result == {"presentationId": "test-id", "slides": []}

    @patch("gslides_automator.gslides_api.build")
    def test_debug_logging_on_rate_limit(self, mock_build, caplog):
        """Test that debug logs are emitted when rate limiting occurs."""
        # Setup mocks
        mock_service = MagicMock()
        mock_presentations = MagicMock()
        mock_get = MagicMock()
        mock_execute = MagicMock(return_value={"presentationId": "test-id", "slides": []})

        mock_build.return_value = mock_service
        mock_service.presentations.return_value = mock_presentations
        mock_presentations.get.return_value = mock_get
        mock_get.execute = mock_execute

        # Reset singleton
        GSlidesAPI._instance = None

        mock_creds = MagicMock()
        api = GSlidesAPI.get_instance(mock_creds)

        # Replace token bucket with very low rate to trigger rate limiting
        api.token_bucket = TokenBucket(read_rate=2.0, write_rate=1.0)
        api.token_bucket.read_tokens = 0.0  # Start with no tokens

        # Enable debug logging
        with caplog.at_level(logging.DEBUG):
            api.get_presentation("test-id")

        # Check that debug logs were emitted
        assert any("Rate limit - waiting for read token" in record.message for record in caplog.records)
        assert any("Rate limit - read token acquired, proceeding" in record.message for record in caplog.records)

    @patch("gslides_automator.gslides_api.build")
    def test_no_logging_when_no_rate_limit(self, mock_build, caplog):
        """Test that no debug logs are emitted when rate limiting doesn't occur."""
        # Setup mocks
        mock_service = MagicMock()
        mock_presentations = MagicMock()
        mock_get = MagicMock()
        mock_execute = MagicMock(return_value={"presentationId": "test-id", "slides": []})

        mock_build.return_value = mock_service
        mock_service.presentations.return_value = mock_presentations
        mock_presentations.get.return_value = mock_get
        mock_get.execute = mock_execute

        # Reset singleton
        GSlidesAPI._instance = None

        mock_creds = MagicMock()
        api = GSlidesAPI.get_instance(mock_creds)

        # Enable debug logging
        with caplog.at_level(logging.DEBUG):
            api.get_presentation("test-id")

        # Check that no rate limit logs were emitted (tokens available immediately)
        rate_limit_logs = [
            record for record in caplog.records
            if "Rate limit" in record.message
        ]
        assert len(rate_limit_logs) == 0
