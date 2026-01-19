"""
Pytest fixtures for Google Slides Automator tests.
"""

from __future__ import annotations

import os
import pytest
import uuid

from googleapiclient.discovery import build

from gslides_automator.auth import get_oauth_credentials
from tests.test_utils import (
    create_test_drive_structure,
    create_test_entities_csv,
    create_test_data_template,
    create_test_slide_template,
    create_test_l0_data,
)


@pytest.fixture(scope="session")
def test_credentials():
    """
    Provides test service account credentials.

    Uses TEST_SERVICE_ACCOUNT_CREDENTIALS environment variable if set,
    otherwise falls back to service-account-credentials.json in project root.
    """
    creds_path = os.getenv(
        "TEST_SERVICE_ACCOUNT_CREDENTIALS",
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "service-account-credentials.json")
    )

    if not os.path.exists(creds_path):
        pytest.skip(f"Test credentials file not found: {creds_path}. Set TEST_SERVICE_ACCOUNT_CREDENTIALS environment variable.")

    return get_oauth_credentials(service_account_credentials=creds_path)


@pytest.fixture(scope="function")
def test_drive_root(test_credentials):
    """
    Creates a test Google Drive folder for each test inside a parent folder.

    Uses TEST_DRIVE_FOLDER_ID environment variable to specify a parent folder
    where test subfolders will be created. Each test creates a unique subfolder
    with a UUID-based name to ensure isolation.

    Returns the folder ID of the test root folder (the created subfolder).
    """
    drive_service = build("drive", "v3", credentials=test_credentials)

    # Get parent folder ID from environment variable
    parent_id = os.getenv("TEST_DRIVE_FOLDER_ID")

    if not parent_id:
        pytest.skip(
            "TEST_DRIVE_FOLDER_ID environment variable must be set. "
            "This should be a folder ID where test subfolders can be created. "
            "Service accounts cannot create folders at the Drive root level."
        )

    # Verify parent folder exists and is accessible
    try:
        parent_folder = drive_service.files().get(
            fileId=parent_id,
            fields="id, name, mimeType",
            supportsAllDrives=True,
        ).execute()

        if parent_folder.get("mimeType") != "application/vnd.google-apps.folder":
            pytest.skip(f"TEST_DRIVE_FOLDER_ID ({parent_id}) is not a folder")
    except Exception as e:
        pytest.skip(f"TEST_DRIVE_FOLDER_ID folder not accessible: {e}")

    # Create a unique test folder inside the parent
    test_folder_name = f"test-gslides-automator-{uuid.uuid4().hex[:8]}"
    file_metadata = {
        "name": test_folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }

    folder = drive_service.files().create(
        body=file_metadata,
        fields="id",
        supportsAllDrives=True,
    ).execute()

    root_id = folder.get("id")

    yield root_id

    # Cleanup: Delete the test folder
    try:
        drive_service.files().delete(
            fileId=root_id,
            supportsAllDrives=True,
        ).execute()
    except Exception as e:
        # Log but don't fail test if cleanup fails
        print(f"Warning: Failed to cleanup test drive folder {root_id}: {e}")


@pytest.fixture(scope="function")
def test_drive_layout(test_drive_root, test_credentials):
    """
    Creates a complete test Drive structure with all required folders and files.

    Returns a DriveLayout object configured for the test drive.
    """
    layout = create_test_drive_structure(test_drive_root, test_credentials)

    yield layout

    # Cleanup is handled by test_drive_root fixture


@pytest.fixture(scope="function")
def test_entities_data(test_drive_layout, test_credentials):
    """
    Creates test entities.csv file with sample entities.

    Returns a tuple of (entities_dict, csv_file_id) where entities_dict maps
    entity names to their L1, L2, and L3 flags.
    """
    entities = {
        "entity-1": {"l1": "Y", "l2": "All", "l3": "N"},
        "entity-2": {"l1": "Y", "l2": "1,2", "l3": "Y"},
        "entity-3": {"l1": "N", "l2": "", "l3": "N"},
        "entity-4": {"l1": "Y", "l2": "1-3", "l3": "N"},
    }

    csv_file_id = create_test_entities_csv(
        test_drive_layout.root_id,
        entities,
        test_credentials,
    )

    # Update layout with the entities CSV ID
    test_drive_layout.entities_csv_id = csv_file_id

    yield entities, csv_file_id


@pytest.fixture(scope="function")
def test_templates(test_drive_layout, test_credentials):
    """
    Creates test data template and slide template.

    Returns a tuple of (data_template_id, slide_template_id).
    """
    data_template_id = create_test_data_template(
        test_drive_layout.templates_id,
        test_credentials,
    )

    slide_template_id = create_test_slide_template(
        test_drive_layout.templates_id,
        test_credentials,
    )

    # Update layout with template IDs
    test_drive_layout.data_template_id = data_template_id
    test_drive_layout.report_template_id = slide_template_id

    yield data_template_id, slide_template_id


@pytest.fixture(scope="function")
def test_l0_data(test_drive_layout, test_credentials):
    """
    Creates test L0-Raw data for entities.

    Returns a dict mapping entity names to their L0 folder IDs.
    """
    entities = ["entity-1", "entity-2"]
    l0_data = {}

    for entity_name in entities:
        folder_id = create_test_l0_data(
            test_drive_layout.l0_raw_id,
            entity_name,
            test_credentials,
        )
        l0_data[entity_name] = folder_id

    yield l0_data


@pytest.fixture(scope="function")
def complete_test_setup(test_drive_layout, test_entities_data, test_templates, test_l0_data):
    """
    Complete test setup with all components.

    Returns a dict with all test components:
    - layout: DriveLayout
    - entities: dict of entity data
    - templates: tuple of (data_template_id, slide_template_id)
    - l0_data: dict mapping entity names to L0 folder IDs
    """
    entities, _ = test_entities_data
    data_template_id, slide_template_id = test_templates

    yield {
        "layout": test_drive_layout,
        "entities": entities,
        "templates": (data_template_id, slide_template_id),
        "l0_data": test_l0_data,
    }

