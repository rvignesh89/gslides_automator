#!/usr/bin/env python3
"""
Google Sheets API service with token bucket rate limiting.

This module provides a GSheetsAPI service that wraps Google Sheets API calls
with rate limiting (60 reads/min, 60 writes/min) and automatic retry logic for 429 errors.

The module provides a shared service instance via get_shared_sheets_service() for
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
_service: Optional[GSheetsAPI] = None
_service_lock = threading.Lock()


class GSheetsAPI:
    """
    Service that wraps Google Sheets API with rate limiting and retry logic.

    Provides rate-limited access to Google Sheets API operations with automatic
    retry on 429 errors using exponential backoff.
    """

    def __init__(self, creds):
        """
        Initialize GSheetsAPI service.

        Args:
            creds: Google OAuth credentials
        """
        self.creds = creds
        self.service = build("sheets", "v4", credentials=creds)
        # Initialize token bucket with Google Sheets API limits
        # 60 reads/min, 60 writes/min (conservative per-user limits)
        self.token_bucket = LeakyBucket(read_rate=60.0, write_rate=60.0)

    def get_shared_sheets_service(creds) -> GSheetsAPI:
        """
        Get or create the shared GSheetsAPI service instance.

        This function provides a module-level shared service instance that maintains
        rate limiting across all API calls in the application.

        Args:
            creds: Google OAuth credentials

        Returns:
            GSheetsAPI instance (shared across the application)
        """
        global _service
        if _service is None:
            with _service_lock:
                # Double-check pattern for thread safety
                if _service is None:
                    _service = GSheetsAPI(creds)
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

    def get_spreadsheet(self, spreadsheet_id: str, **kwargs):
        """
        Get a spreadsheet by ID (rate-limited read operation).

        Args:
            spreadsheet_id: ID of the spreadsheet
            **kwargs: Additional arguments to pass to the API call

        Returns:
            Spreadsheet resource dictionary
        """
        # Acquire read token (blocks if needed)
        self.token_bucket.acquire("read")

        # Execute with retry logic
        def _get():
            return (
                self.service.spreadsheets()
                .get(spreadsheetId=spreadsheet_id, **kwargs)
                .execute()
            )

        return retry_with_exponential_backoff(_get)

    def get_values(self, spreadsheet_id: str, range_name: str, **kwargs):
        """
        Get values from a range in a spreadsheet (rate-limited read operation).

        Args:
            spreadsheet_id: ID of the spreadsheet
            range_name: A1 notation range (e.g., "Sheet1!A1:B2")
            **kwargs: Additional arguments to pass to the API call

        Returns:
            ValueRange resource dictionary
        """
        # Acquire read token (blocks if needed)
        self.token_bucket.acquire("read")

        # Execute with retry logic
        def _get():
            return (
                self.service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=range_name, **kwargs)
                .execute()
            )

        return retry_with_exponential_backoff(_get)

    def update_values(
        self,
        spreadsheet_id: str,
        range_name: str,
        values: list,
        value_input_option: str = "RAW",
        **kwargs,
    ):
        """
        Update values in a range of a spreadsheet (rate-limited write operation).

        Args:
            spreadsheet_id: ID of the spreadsheet
            range_name: A1 notation range (e.g., "Sheet1!A1:B2")
            values: List of rows, where each row is a list of values
            value_input_option: How to interpret input values (RAW or USER_ENTERED)
            **kwargs: Additional arguments to pass to the API call

        Returns:
            UpdateValuesResponse
        """
        # Acquire write token (blocks if needed)
        self.token_bucket.acquire("write")

        # Execute with retry logic
        def _update():
            body = {"values": values}
            return (
                self.service.spreadsheets()
                .values()
                .update(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption=value_input_option,
                    body=body,
                    **kwargs,
                )
                .execute()
            )

        return retry_with_exponential_backoff(_update)

    def batch_update_values(self, spreadsheet_id: str, data: list, **kwargs):
        """
        Update multiple ranges in a spreadsheet (rate-limited write operation).

        Args:
            spreadsheet_id: ID of the spreadsheet
            data: List of ValueRange objects, each with 'range' and 'values'
            **kwargs: Additional arguments to pass to the API call

        Returns:
            BatchUpdateValuesResponse
        """
        # Acquire write token (blocks if needed)
        self.token_bucket.acquire("write")

        # Execute with retry logic
        def _batch_update():
            body = {
                "valueInputOption": kwargs.pop("valueInputOption", "RAW"),
                "data": data,
            }
            return (
                self.service.spreadsheets()
                .values()
                .batchUpdate(spreadsheetId=spreadsheet_id, body=body, **kwargs)
                .execute()
            )

        return retry_with_exponential_backoff(_batch_update)

    def batch_update(self, spreadsheet_id: str, body: dict, **kwargs):
        """
        Execute a batch update on a spreadsheet for structural changes (rate-limited write operation).

        This is used for operations like adding sheets, updating sheet properties, etc.

        Args:
            spreadsheet_id: ID of the spreadsheet
            body: Request body containing the batch update requests
            **kwargs: Additional arguments to pass to the API call

        Returns:
            BatchUpdateSpreadsheetResponse
        """
        # Acquire write token (blocks if needed)
        self.token_bucket.acquire("write")

        # Execute with retry logic
        def _batch_update():
            return (
                self.service.spreadsheets()
                .batchUpdate(spreadsheetId=spreadsheet_id, body=body, **kwargs)
                .execute()
            )

        return retry_with_exponential_backoff(_batch_update)
