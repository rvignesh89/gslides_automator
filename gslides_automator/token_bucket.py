#!/usr/bin/env python3
"""
Token bucket rate limiter for Google APIs.

This module provides a thread-safe TokenBucket class that supports both dual-bucket
(read/write) and single-bucket modes for rate limiting API requests.
"""

from __future__ import annotations
import logging
import threading
import time
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class TokenBucket:
    """
    Thread-safe token bucket rate limiter for Google APIs.

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
        Initialize token bucket with specified rates.

        Args:
            read_rate: Number of read tokens per minute (or all tokens per minute in single-bucket mode)
            write_rate: Number of write tokens per minute. If None, uses single-bucket mode.
            read_capacity: Maximum read tokens (defaults to read_rate)
            write_capacity: Maximum write tokens (defaults to write_rate, ignored in single-bucket mode)
        """
        self.read_rate = read_rate  # tokens per minute
        self.write_rate = write_rate  # tokens per minute, None for single-bucket mode
        self.single_bucket_mode = write_rate is None

        # Capacity defaults to rate (allows burst up to full rate)
        self.read_capacity = read_capacity if read_capacity is not None else read_rate
        if not self.single_bucket_mode:
            self.write_capacity = (
                write_capacity if write_capacity is not None else write_rate
            )
        else:
            self.write_capacity = 0.0  # Not used in single-bucket mode

        # Current token counts (start full)
        self.read_tokens = self.read_capacity
        if not self.single_bucket_mode:
            self.write_tokens = self.write_capacity
        else:
            self.write_tokens = 0.0  # Not used in single-bucket mode

        # Last refill timestamps
        self.last_read_refill = time.time()
        if not self.single_bucket_mode:
            self.last_write_refill = time.time()
        else:
            self.last_write_refill = 0.0  # Not used in single-bucket mode

        # Lock for thread safety
        self.lock = threading.Lock()

    def acquire(self, operation_type: str = "read") -> None:
        """
        Acquire a token for the specified operation type.

        Blocks until a token is available. Refills tokens based on elapsed time.

        Args:
            operation_type: "read" or "write" for dual-bucket mode.
                          Any string (ignored) for single-bucket mode.
        """
        with self.lock:
            # Refill tokens based on elapsed time
            self._refill()

            if self.single_bucket_mode:
                # Single-bucket mode: use read_tokens for all operations
                tokens = self.read_tokens
                rate = self.read_rate
            else:
                # Dual-bucket mode: validate operation_type
                if operation_type not in ("read", "write"):
                    raise ValueError(
                        f"operation_type must be 'read' or 'write', got '{operation_type}'"
                    )

                # Determine which bucket to use
                if operation_type == "read":
                    tokens = self.read_tokens
                    rate = self.read_rate
                else:  # write
                    tokens = self.write_tokens
                    rate = self.write_rate

            # If no tokens available, wait and refill
            if tokens < 1:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                op_name = (
                    operation_type if not self.single_bucket_mode else "operation"
                )
                logger.debug(
                    f"[TokenBucket] {current_time} - Rate limit - waiting for {op_name} token"
                )
                # Calculate time needed to get one token
                tokens_per_second = rate / 60.0
                wait_time = (1.0 - tokens) / tokens_per_second
                if wait_time > 0:
                    time.sleep(wait_time)
                    self._refill()

                # After waiting, we should have at least one token
                if self.single_bucket_mode:
                    tokens = self.read_tokens
                elif operation_type == "read":
                    tokens = self.read_tokens
                else:
                    tokens = self.write_tokens

                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                logger.debug(
                    f"[TokenBucket] {current_time} - Rate limit - {op_name} token acquired, proceeding"
                )

            # Consume one token
            if self.single_bucket_mode:
                self.read_tokens -= 1
            elif operation_type == "read":
                self.read_tokens -= 1
            else:
                self.write_tokens -= 1

    def _refill(self) -> None:
        """
        Refill tokens based on elapsed time since last refill.

        This is called with the lock held.
        """
        now = time.time()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        # Refill read tokens (or single bucket tokens)
        elapsed_minutes = (now - self.last_read_refill) / 60.0
        tokens_to_add = elapsed_minutes * self.read_rate
        self.read_tokens = min(self.read_capacity, self.read_tokens + tokens_to_add)
        self.last_read_refill = now

        # Refill write tokens (only in dual-bucket mode)
        if not self.single_bucket_mode:
            elapsed_minutes = (now - self.last_write_refill) / 60.0
            tokens_to_add = elapsed_minutes * self.write_rate
            self.write_tokens = min(
                self.write_capacity, self.write_tokens + tokens_to_add
            )
            self.last_write_refill = now

            # Debug log with prefix and timestamp
            logger.debug(
                f"[TokenBucket] {current_time} - Refill completed: "
                f"read_tokens={self.read_tokens}, write_tokens={self.write_tokens}"
            )
        else:
            # Debug log for single-bucket mode
            logger.debug(
                f"[TokenBucket] {current_time} - Refill completed: "
                f"tokens={self.read_tokens}"
            )
