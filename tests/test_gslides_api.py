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

from gslides_automator.gslides_api import GSlidesAPI
from gslides_automator.leaky_bucket import LeakyBucket


class TestLeakyBucket:
    """Test LeakyBucket rate limiting."""

    def test_leaky_bucket_initialization(self):
        """Test leaky bucket initializes with correct intervals."""
        bucket = LeakyBucket(read_rate=600.0, write_rate=60.0)
        # 600/minute = 0.1 seconds between calls
        assert abs(bucket.read_interval - 0.1) < 0.001
        # 60/minute = 1.0 seconds between calls
        assert abs(bucket.write_interval - 1.0) < 0.001
        assert bucket.single_bucket_mode is False
        # Backward compatibility attributes
        assert bucket.read_capacity == 600.0
        assert bucket.write_capacity == 60.0

    def test_token_bucket_custom_capacity(self):
        """Test leaky bucket with custom capacity (ignored but kept for compatibility)."""
        bucket = LeakyBucket(
            read_rate=600.0, write_rate=60.0, read_capacity=1000.0, write_capacity=100.0
        )
        # Capacity is kept for backward compatibility but doesn't affect behavior
        assert bucket.read_capacity == 600.0  # Uses read_rate, not read_capacity
        assert bucket.write_capacity == 60.0  # Uses write_rate, not write_capacity

    def test_acquire_read_token(self):
        """Test acquiring a read token - first call should be immediate."""
        bucket = LeakyBucket(read_rate=600.0, write_rate=60.0)
        # First call should not block
        start_time = time.time()
        bucket.acquire("read")
        elapsed = time.time() - start_time
        assert elapsed < 0.1  # Should be very fast

    def test_acquire_write_token(self):
        """Test acquiring a write token - first call should be immediate."""
        bucket = LeakyBucket(read_rate=600.0, write_rate=60.0)
        # First call should not block
        start_time = time.time()
        bucket.acquire("write")
        elapsed = time.time() - start_time
        assert elapsed < 0.1  # Should be very fast

    def test_rate_limiting_read(self):
        """Test that read operations are rate limited with fixed intervals."""
        # Use very low rate for testing (2 reads per minute = 30 seconds between calls)
        bucket = LeakyBucket(read_rate=2.0, write_rate=60.0)
        # Interval should be 30 seconds
        assert abs(bucket.read_interval - 30.0) < 0.1

        # First call should be immediate
        start_time = time.time()
        bucket.acquire("read")
        first_elapsed = time.time() - start_time
        assert first_elapsed < 0.1

        # Second call should wait approximately 30 seconds
        start_time = time.time()
        bucket.acquire("read")
        elapsed = time.time() - start_time

        # Should have waited approximately 30 seconds
        # Allow some tolerance for test execution time
        assert elapsed >= 29.0  # At least 29 seconds
        assert elapsed < 35.0  # But not too long

    def test_rate_limiting_write(self):
        """Test that write operations are rate limited with fixed intervals."""
        # Use very low rate for testing (1 write per minute = 60 seconds between calls)
        bucket = LeakyBucket(read_rate=600.0, write_rate=1.0)
        # Interval should be 60 seconds
        assert abs(bucket.write_interval - 60.0) < 0.1

        # First call should be immediate
        start_time = time.time()
        bucket.acquire("write")
        first_elapsed = time.time() - start_time
        assert first_elapsed < 0.1

        # Second call should wait approximately 60 seconds
        start_time = time.time()
        bucket.acquire("write")
        elapsed = time.time() - start_time

        # Should have waited approximately 60 seconds
        # Allow some tolerance for test execution time
        assert elapsed >= 59.0  # At least 59 seconds
        assert elapsed < 65.0  # But not too long

    def test_fixed_rate_enforcement(self):
        """Test that leaky bucket enforces fixed rate (no bursts)."""
        # 60/minute = 1 call per second
        bucket = LeakyBucket(read_rate=60.0, write_rate=60.0)

        # Make multiple calls and verify they're spaced correctly
        call_times = []
        for _ in range(3):
            bucket.acquire("read")
            call_times.append(time.time())

        # Check intervals between calls
        interval1 = call_times[1] - call_times[0]
        interval2 = call_times[2] - call_times[1]

        # First call is immediate, subsequent calls should be ~1 second apart
        assert interval1 >= 0.95  # At least 0.95 seconds
        assert interval1 < 1.5  # But not too long
        assert interval2 >= 0.95
        assert interval2 < 1.5

    def test_invalid_operation_type(self):
        """Test that invalid operation type raises ValueError."""
        bucket = LeakyBucket(read_rate=600.0, write_rate=60.0)
        with pytest.raises(ValueError, match="operation_type must be 'read' or 'write'"):
            bucket.acquire("invalid")

    def test_thread_safety(self):
        """Test that leaky bucket is thread-safe."""
        # Use a moderate rate for testing
        bucket = LeakyBucket(read_rate=60.0, write_rate=60.0)  # 1 per second
        results = []
        call_times = []

        def acquire_tokens():
            for _ in range(5):
                bucket.acquire("read")
                call_times.append(time.time())
                results.append(threading.current_thread().ident)

        threads = [threading.Thread(target=acquire_tokens) for _ in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Should have acquired 15 tokens total (3 threads * 5 tokens)
        assert len(results) == 15
        # All calls should have completed (thread safety verified by no exceptions)


class TestGSlidesAPI:
    """Test GSlidesAPI rate limiting."""

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

        mock_creds = MagicMock()
        api = GSlidesAPI(mock_creds)

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

        mock_creds = MagicMock()
        api = GSlidesAPI(mock_creds)

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

        mock_creds = MagicMock()
        api = GSlidesAPI(mock_creds)

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

        mock_creds = MagicMock()
        api = GSlidesAPI(mock_creds)

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

        mock_creds = MagicMock()
        api = GSlidesAPI(mock_creds)

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

        mock_creds = MagicMock()
        api = GSlidesAPI(mock_creds)

        # Replace token bucket with reduced limits (2 reads/min = 30 seconds between calls)
        api.token_bucket = LeakyBucket(read_rate=2.0, write_rate=1.0)

        # First call should be immediate
        result1 = api.get_presentation("test-id")
        assert result1 == {"presentationId": "test-id", "slides": []}

        # Second call should wait approximately 30 seconds
        start_time = time.time()
        result2 = api.get_presentation("test-id")
        elapsed = time.time() - start_time

        # Should have waited approximately 30 seconds
        assert elapsed >= 29.0  # At least 29 seconds
        assert elapsed < 35.0  # But not too long
        assert result2 == {"presentationId": "test-id", "slides": []}

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

        mock_creds = MagicMock()
        api = GSlidesAPI(mock_creds)

        # Replace token bucket with very low rate to trigger rate limiting
        api.token_bucket = LeakyBucket(read_rate=2.0, write_rate=1.0)

        # Enable debug logging
        with caplog.at_level(logging.DEBUG):
            # First call - immediate, no wait log
            api.get_presentation("test-id")
            # Second call - should wait and log
            api.get_presentation("test-id")

        # Check that debug logs were emitted for the second call
        assert any("Rate limit - waiting" in record.message and "read" in record.message for record in caplog.records)
        assert any("Rate limit - read operation allowed" in record.message for record in caplog.records)

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

        mock_creds = MagicMock()
        api = GSlidesAPI(mock_creds)

        # Enable debug logging
        with caplog.at_level(logging.DEBUG):
            api.get_presentation("test-id")

        # Check that no rate limit logs were emitted (tokens available immediately)
        rate_limit_logs = [
            record for record in caplog.records
            if "Rate limit" in record.message
        ]
        assert len(rate_limit_logs) == 0
