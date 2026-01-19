#!/usr/bin/env python3
"""
Shared authentication module for Google API access.
Provides service account credential management for Google Sheets, Drive, and Slides APIs.
"""

from google.oauth2 import service_account
import os

# Combined scopes required by all scripts
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",  # Full access (covers readonly)
    "https://www.googleapis.com/auth/drive.readonly",  # For reading/copying templates
    "https://www.googleapis.com/auth/drive.file",  # For creating and moving files
    "https://www.googleapis.com/auth/drive",  # Full Drive access for modifying file permissions
    "https://www.googleapis.com/auth/presentations",  # For modifying slides
]

# Get the project root directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVICE_ACCOUNT_CREDENTIALS = os.path.join(
    PROJECT_ROOT, "service-account-credentials.json"
)


def get_oauth_credentials(service_account_credentials: str | None = None, scopes=None):
    """
    Get service account credentials using a JSON key file. Defaults to service-account-credentials.json in repo root.

    Args:
        service_account_credentials: Optional path to a service account key JSON.
        scopes: Optional list of scopes to request.

    Returns:
        google.oauth2.service_account.Credentials: Service account credentials object

    Raises:
        FileNotFoundError: If the credentials file is not found
    """
    key_path = service_account_credentials or SERVICE_ACCOUNT_CREDENTIALS
    if not os.path.exists(key_path):
        raise FileNotFoundError(
            f"Service account credentials file '{key_path}' not found. "
            "Please download service account credentials from Google Cloud Console."
        )

    requested_scopes = scopes or SCOPES

    creds = service_account.Credentials.from_service_account_file(
        key_path, scopes=requested_scopes
    )

    return creds


def load_credentials(service_account_credentials: str):
    """
    Load service-account credentials from a provided path.

    Args:
        service_account_credentials: Path to the service account JSON key file.

    Returns:
        google.oauth2.service_account.Credentials: Service account credentials object

    Raises:
        ValueError: If service_account_credentials is empty
        FileNotFoundError: If the credentials file is not found
    """
    if not service_account_credentials:
        raise ValueError("service_account_credentials is required.")
    if not os.path.exists(service_account_credentials):
        raise FileNotFoundError(
            f"Credentials file not found: {service_account_credentials}"
        )
    return get_oauth_credentials(
        service_account_credentials=service_account_credentials, scopes=SCOPES
    )


def get_service_account_email():
    """
    Get the service account email from the credentials file.
    Useful for sharing files/folders with the service account.

    Returns:
        str: Service account email address

    Raises:
        FileNotFoundError: If service-account-credentials.json is not found
        KeyError: If email is not found in the credentials file
    """
    import json

    if not os.path.exists(SERVICE_ACCOUNT_CREDENTIALS):
        raise FileNotFoundError(
            f"Service account credentials file '{SERVICE_ACCOUNT_CREDENTIALS}' not found."
        )

    with open(SERVICE_ACCOUNT_CREDENTIALS, "r") as f:
        creds_data = json.load(f)

    email = creds_data.get("client_email")
    if not email:
        raise KeyError("'client_email' not found in service account credentials file")

    return email
