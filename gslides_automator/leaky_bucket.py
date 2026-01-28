#!/usr/bin/env python3
"""
Leaky bucket rate limiter for Google APIs.

This module provides a thread-safe LeakyBucket class that supports both dual-bucket
(read/write) and single-bucket modes for rate limiting API requests with fixed rates.

The leaky bucket enforces a fixed rate: if the limit is 600/minute, then exactly
10 calls can be made every second (no bursts allowed).
"""

from __future__ import annotations
import logging
import threading
import time
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class LeakyBucket:
    """
    Thread-safe leaky bucket rate limiter for Google APIs.

    Enforces a fixed rate of API calls. For example, if read_rate is 600/minute,
    then exactly 10 calls can be made every second (600/60 = 10 calls/second).

    Supports two modes:
    - Dual-bucket mode: Separate buckets for read and write operations (default)
    - Single-bucket mode: Single bucket for all operations (when write_rate is None)
    """

    def __init__(
        self,
        read_rate: float,
        write_rate: Optional[float] = None,
        read_capacity: Optional[float] = None,
        write_capacity: Optional[float] = None,
    ):
        """
        Initialize leaky bucket with specified rates.

        Args:
            read_rate: Number of read operations per minute (or all operations per minute in single-bucket mode)
            write_rate: Number of write operations per minute. If None, uses single-bucket mode.
            read_capacity: Ignored (kept for API compatibility)
            write_capacity: Ignored (kept for API compatibility)
        """
        self.read_rate = read_rate  # operations per minute
        self.write_rate = write_rate  # operations per minute, None for single-bucket mode
        self.single_bucket_mode = write_rate is None

        # Calculate minimum interval between calls (in seconds)
        # If rate is 600/minute, then interval is 60/600 = 0.1 seconds
        self.read_interval = 60.0 / read_rate if read_rate > 0 else 0.0
        if not self.single_bucket_mode:
            self.write_interval = 60.0 / write_rate if write_rate > 0 else 0.0
        else:
            self.write_interval = 0.0  # Not used in single-bucket mode

        # Track last call time for each operation type
        self.last_read_call = 0.0
        if not self.single_bucket_mode:
            self.last_write_call = 0.0
        else:
            self.last_write_call = 0.0  # Not used in single-bucket mode

        # Lock for thread safety
        self.lock = threading.Lock()

        # For backward compatibility with tests that access these attributes
        self.read_tokens = 0.0
        self.write_tokens = 0.0
        self.read_capacity = read_rate
        if not self.single_bucket_mode:
            self.write_capacity = write_rate
        else:
            self.write_capacity = 0.0

    def acquire(self, operation_type: str = "read") -> None:
        """
        Acquire permission for the specified operation type.

        Blocks until enough time has passed since the last call of this type.

        Args:
            operation_type: "read" or "write" for dual-bucket mode.
                          Any string (ignored) for single-bucket mode.
        """
        with self.lock:
            now = time.time()

            if self.single_bucket_mode:
                # Single-bucket mode: use read interval for all operations
                interval = self.read_interval
                last_call = self.last_read_call
                op_name = "operation"
            else:
                # Dual-bucket mode: validate operation_type
                if operation_type not in ("read", "write"):
                    raise ValueError(
                        f"operation_type must be 'read' or 'write', got '{operation_type}'"
                    )

                # Determine which interval to use
                if operation_type == "read":
                    interval = self.read_interval
                    last_call = self.last_read_call
                    op_name = "read"
                else:  # write
                    interval = self.write_interval
                    last_call = self.last_write_call
                    op_name = "write"

            # Calculate time elapsed since last call
            elapsed = now - last_call


            # If not enough time has passed, wait
            if elapsed < interval:
                wait_time = interval - elapsed
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                if self.single_bucket_mode:
                    logger.debug(
                        f"[LeakyBucket] {current_time} - Rate limit - waiting {wait_time:.3f}s for operation"
                    )
                else:
                    logger.debug(
                        f"[LeakyBucket] {current_time} - Rate limit - waiting {wait_time:.3f}s for {op_name} operation"
                    )
                time.sleep(wait_time)
                now = time.time()  # Update time after sleep
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                if self.single_bucket_mode:
                    logger.debug(
                        f"[LeakyBucket] {current_time} - Rate limit - operation allowed, proceeding"
                    )
                else:
                    logger.debug(
                        f"[LeakyBucket] {current_time} - Rate limit - {op_name} operation allowed, proceeding"
                    )

            # Update last call time
            if self.single_bucket_mode:
                self.last_read_call = now
            elif operation_type == "read":
                self.last_read_call = now
            else:
                self.last_write_call = now

