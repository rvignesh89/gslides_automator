"""
Tests for LeakyBucket rate limiting.
"""

from __future__ import annotations

import logging
import threading
import time

import pytest

from gslides_automator.leaky_bucket import LeakyBucket


@pytest.mark.serial
class TestLeakyBucketDualBucket:
    """Test LeakyBucket in dual-bucket mode (read/write)."""

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

    def test_leaky_bucket_custom_capacity(self):
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


@pytest.mark.serial
class TestLeakyBucketSingleBucket:
    """Test LeakyBucket in single-bucket mode (for Drive API)."""

    def test_single_bucket_initialization(self):
        """Test single-bucket mode initializes correctly."""
        bucket = LeakyBucket(read_rate=12000.0, write_rate=None)
        # 12000/minute = 0.005 seconds between calls
        assert abs(bucket.read_interval - 0.005) < 0.001
        assert bucket.single_bucket_mode is True
        # Backward compatibility attributes
        assert bucket.read_capacity == 12000.0

    def test_single_bucket_custom_capacity(self):
        """Test single-bucket mode with custom capacity (ignored but kept for compatibility)."""
        bucket = LeakyBucket(read_rate=12000.0, write_rate=None, read_capacity=15000.0)
        # Capacity is kept for backward compatibility but doesn't affect behavior
        assert bucket.read_capacity == 12000.0  # Uses read_rate, not read_capacity

    def test_single_bucket_acquire(self):
        """Test acquiring permission in single-bucket mode - first call should be immediate."""
        bucket = LeakyBucket(read_rate=12000.0, write_rate=None)
        # First call should not block
        start_time = time.time()
        bucket.acquire()  # operation_type is ignored in single-bucket mode
        elapsed = time.time() - start_time
        assert elapsed < 0.1  # Should be very fast

    def test_single_bucket_acquire_with_any_string(self):
        """Test that operation_type is ignored in single-bucket mode."""
        bucket = LeakyBucket(read_rate=12000.0, write_rate=None)
        bucket.acquire("read")  # Should work
        bucket.acquire("write")  # Should work
        bucket.acquire("anything")  # Should work
        # All calls should complete without error

    def test_single_bucket_rate_limiting(self):
        """Test that single-bucket operations are rate limited with fixed intervals."""
        # Use very low rate for testing (2 per minute = 30 seconds between calls)
        bucket = LeakyBucket(read_rate=2.0, write_rate=None)
        # Interval should be 30 seconds
        assert abs(bucket.read_interval - 30.0) < 0.1

        # First call should be immediate
        start_time = time.time()
        bucket.acquire()
        first_elapsed = time.time() - start_time
        assert first_elapsed < 0.1

        # Second call should wait approximately 30 seconds
        start_time = time.time()
        bucket.acquire()
        elapsed = time.time() - start_time

        # Should have waited approximately 30 seconds
        assert elapsed >= 29.0
        assert elapsed < 35.0

    def test_single_bucket_fixed_rate_enforcement(self):
        """Test that single-bucket mode enforces fixed rate (no bursts)."""
        # 60/minute = 1 call per second
        bucket = LeakyBucket(read_rate=60.0, write_rate=None)

        # Make multiple calls and verify they're spaced correctly
        call_times = []
        for _ in range(3):
            bucket.acquire()
            call_times.append(time.time())

        # Check intervals between calls
        interval1 = call_times[1] - call_times[0]
        interval2 = call_times[2] - call_times[1]

        # First call is immediate, subsequent calls should be ~1 second apart
        assert interval1 >= 0.95  # At least 0.95 seconds
        assert interval1 < 1.5  # But not too long
        assert interval2 >= 0.95
        assert interval2 < 1.5

    def test_single_bucket_thread_safety(self):
        """Test that single-bucket mode is thread-safe."""
        bucket = LeakyBucket(read_rate=60.0, write_rate=None)  # 1 per second
        results = []
        call_times = []

        def acquire_tokens():
            for _ in range(5):
                bucket.acquire()
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

    def test_single_bucket_debug_logging(self, caplog):
        """Test that debug logs are emitted in single-bucket mode."""
        bucket = LeakyBucket(read_rate=2.0, write_rate=None)  # 30 seconds between calls

        with caplog.at_level(logging.DEBUG):
            # First call - should be immediate, no wait log
            bucket.acquire()
            # Second call - should wait and log
            bucket.acquire()

        # Check that debug logs were emitted for the second call
        assert any("Rate limit - waiting" in record.message and "operation" in record.message for record in caplog.records)
        assert any("Rate limit - operation allowed" in record.message for record in caplog.records)
