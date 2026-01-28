"""
Tests for TokenBucket rate limiting.
"""

from __future__ import annotations

import logging
import threading
import time

import pytest

from gslides_automator.token_bucket import TokenBucket


class TestTokenBucketDualBucket:
    """Test TokenBucket in dual-bucket mode (read/write)."""

    def test_token_bucket_initialization(self):
        """Test token bucket initializes with correct capacity."""
        bucket = TokenBucket(read_rate=600.0, write_rate=60.0)
        assert bucket.read_tokens == 600.0
        assert bucket.write_tokens == 60.0
        assert bucket.read_capacity == 600.0
        assert bucket.write_capacity == 60.0
        assert bucket.single_bucket_mode is False

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


class TestTokenBucketSingleBucket:
    """Test TokenBucket in single-bucket mode (for Drive API)."""

    def test_single_bucket_initialization(self):
        """Test single-bucket mode initializes correctly."""
        bucket = TokenBucket(read_rate=12000.0, write_rate=None)
        assert bucket.read_tokens == 12000.0
        assert bucket.read_capacity == 12000.0
        assert bucket.single_bucket_mode is True
        assert bucket.write_tokens == 0.0  # Not used in single-bucket mode

    def test_single_bucket_custom_capacity(self):
        """Test single-bucket mode with custom capacity."""
        bucket = TokenBucket(read_rate=12000.0, write_rate=None, read_capacity=15000.0)
        assert bucket.read_tokens == 15000.0
        assert bucket.read_capacity == 15000.0

    def test_single_bucket_acquire(self):
        """Test acquiring a token in single-bucket mode."""
        bucket = TokenBucket(read_rate=12000.0, write_rate=None)
        # Should not block when tokens are available
        start_time = time.time()
        bucket.acquire()  # operation_type is ignored in single-bucket mode
        elapsed = time.time() - start_time
        assert elapsed < 0.1  # Should be very fast
        assert bucket.read_tokens == 11999.0

    def test_single_bucket_acquire_with_any_string(self):
        """Test that operation_type is ignored in single-bucket mode."""
        bucket = TokenBucket(read_rate=12000.0, write_rate=None)
        bucket.acquire("read")  # Should work
        bucket.acquire("write")  # Should work
        bucket.acquire("anything")  # Should work
        assert bucket.read_tokens == 11997.0

    def test_single_bucket_rate_limiting(self):
        """Test that single-bucket operations are rate limited."""
        # Use very low rate for testing (2 per minute = 1 per 30 seconds)
        bucket = TokenBucket(read_rate=2.0, write_rate=None)

        # Consume all tokens
        bucket.read_tokens = 0.0

        # Acquiring should block until token is available
        start_time = time.time()
        bucket.acquire()
        elapsed = time.time() - start_time

        # Should have waited approximately 30 seconds
        assert elapsed >= 25.0
        assert elapsed < 40.0

    def test_single_bucket_token_refill(self):
        """Test that tokens refill over time in single-bucket mode."""
        bucket = TokenBucket(read_rate=60.0, write_rate=None)  # 1 per second

        # Consume all tokens
        bucket.read_tokens = 0.0

        # Wait a bit
        time.sleep(1.1)

        # Refill should have occurred
        bucket._refill()
        assert bucket.read_tokens > 0.0

    def test_single_bucket_thread_safety(self):
        """Test that single-bucket mode is thread-safe."""
        bucket = TokenBucket(read_rate=1000.0, write_rate=None)
        results = []

        def acquire_tokens():
            for _ in range(10):
                bucket.acquire()
                results.append(threading.current_thread().ident)

        threads = [threading.Thread(target=acquire_tokens) for _ in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Should have acquired 50 tokens total
        assert 950.0 <= bucket.read_tokens <= 1000.0
        assert len(results) == 50

    def test_single_bucket_debug_logging(self, caplog):
        """Test that debug logs are emitted in single-bucket mode."""
        bucket = TokenBucket(read_rate=2.0, write_rate=None)
        bucket.read_tokens = 0.0  # Start with no tokens

        with caplog.at_level(logging.DEBUG):
            bucket.acquire()

        # Check that debug logs were emitted
        assert any("Rate limit - waiting for operation token" in record.message for record in caplog.records)
        assert any("Rate limit - operation token acquired, proceeding" in record.message for record in caplog.records)
