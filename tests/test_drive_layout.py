"""
Tests for drive_layout module.
"""

from __future__ import annotations

import pytest
from googleapiclient.discovery import build

from gslides_automator.drive_layout import (
    DriveLayout,
    resolve_layout,
    load_entities,
    load_entities_with_slides,
    _extract_id_from_url,
)
from tests.test_utils import create_test_entities_csv


class TestExtractIdFromUrl:
    """Tests for _extract_id_from_url function."""

    def test_extract_id_from_folder_url(self):
        """Test extracting ID from a folder URL."""
        url = "https://drive.google.com/drive/folders/1ABC123def456GHI789"
        assert _extract_id_from_url(url) == "1ABC123def456GHI789"

    def test_extract_id_from_url_with_query(self):
        """Test extracting ID from URL with query parameters."""
        url = "https://drive.google.com/drive/folders/1ABC123def456GHI789?usp=sharing"
        assert _extract_id_from_url(url) == "1ABC123def456GHI789"

    def test_extract_id_from_raw_id(self):
        """Test extracting ID when input is already a raw ID."""
        raw_id = "1ABC123def456GHI789"
        assert _extract_id_from_url(raw_id) == raw_id

    def test_extract_id_invalid_url(self):
        """Test that invalid URL raises ValueError."""
        with pytest.raises(ValueError, match="Could not extract"):
            _extract_id_from_url("not-a-valid-url")


class TestResolveLayout:
    """Tests for resolve_layout function."""

    def test_resolve_layout_success(self, test_drive_layout, test_credentials):
        """Test successful layout resolution."""
        layout = resolve_layout(test_drive_layout.root_id, test_credentials)

        assert isinstance(layout, DriveLayout)
        assert layout.root_id == test_drive_layout.root_id
        assert layout.l0_data_id == test_drive_layout.l0_data_id
        assert layout.l1_data_id == test_drive_layout.l1_data_id
        assert layout.l2_report_id == test_drive_layout.l2_report_id
        assert layout.templates_id == test_drive_layout.templates_id

    def test_resolve_layout_missing_folders(self, test_credentials):
        """Test that missing folders raise FileNotFoundError."""
        drive_service = build("drive", "v3", credentials=test_credentials)

        # Create a folder without required subfolders
        file_metadata = {
            "name": "test-empty-folder",
            "mimeType": "application/vnd.google-apps.folder",
        }
        folder = drive_service.files().create(
            body=file_metadata,
            fields="id",
            supportsAllDrives=True,
        ).execute()

        root_id = folder.get("id")

        try:
            with pytest.raises(FileNotFoundError):
                resolve_layout(root_id, test_credentials)
        finally:
            # Cleanup
            drive_service.files().delete(
                fileId=root_id,
                supportsAllDrives=True,
            ).execute()

    def test_resolve_layout_invalid_root(self, test_credentials):
        """Test that invalid root ID raises error."""
        with pytest.raises(Exception):  # Could be FileNotFoundError or HttpError
            resolve_layout("invalid-id-12345", test_credentials)


class TestLoadEntities:
    """Tests for load_entities function."""

    def test_load_entities_success(self, test_drive_layout, test_credentials):
        """Test loading entities with generate=Y."""
        entities = {
            "entity-1": {"generate": "Y", "slides": ""},
            "entity-2": {"generate": "Y", "slides": ""},
            "entity-3": {"generate": "N", "slides": ""},
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )

        loaded = load_entities(csv_file_id, test_credentials)

        assert len(loaded) == 2
        assert "entity-1" in loaded
        assert "entity-2" in loaded
        assert "entity-3" not in loaded

    def test_load_entities_empty(self, test_drive_layout, test_credentials):
        """Test loading entities when none are marked for generation."""
        entities = {
            "entity-1": {"generate": "N", "slides": ""},
            "entity-2": {"generate": "N", "slides": ""},
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )

        loaded = load_entities(csv_file_id, test_credentials)
        assert len(loaded) == 0

    def test_load_entities_with_header(self, test_drive_layout, test_credentials):
        """Test that header row is properly skipped."""
        entities = {
            "Entity": {"generate": "Generate", "slides": "Slides"},  # Header row
            "entity-1": {"generate": "Y", "slides": ""},
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )

        loaded = load_entities(csv_file_id, test_credentials)
        assert "Entity" not in loaded
        assert "entity-1" in loaded


class TestLoadEntitiesWithSlides:
    """Tests for load_entities_with_slides function."""

    def test_load_entities_with_slides_all(self, test_drive_layout, test_credentials):
        """Test loading entities with slides=None (all slides)."""
        entities = {
            "entity-1": {"generate": "Y", "slides": ""},
            "entity-2": {"generate": "Y", "slides": ""},
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )

        loaded = load_entities_with_slides(csv_file_id, test_credentials)

        assert len(loaded) == 2
        assert loaded["entity-1"] is None  # None means all slides
        assert loaded["entity-2"] is None

    def test_load_entities_with_slides_specific(self, test_drive_layout, test_credentials):
        """Test loading entities with specific slide numbers."""
        entities = {
            "entity-1": {"generate": "Y", "slides": "1,2,3"},
            "entity-2": {"generate": "Y", "slides": "2-4"},
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )

        loaded = load_entities_with_slides(csv_file_id, test_credentials)

        assert len(loaded) == 2
        assert loaded["entity-1"] == {1, 2, 3}
        assert loaded["entity-2"] == {2, 3, 4}

    def test_load_entities_with_slides_range(self, test_drive_layout, test_credentials):
        """Test loading entities with slide range."""
        entities = {
            "entity-1": {"generate": "Y", "slides": "1-3"},
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )

        loaded = load_entities_with_slides(csv_file_id, test_credentials)

        assert loaded["entity-1"] == {1, 2, 3}

    def test_load_entities_with_slides_mixed(self, test_drive_layout, test_credentials):
        """Test loading entities with mixed slide specification."""
        entities = {
            "entity-1": {"generate": "Y", "slides": "1,3,5-7"},
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )

        loaded = load_entities_with_slides(csv_file_id, test_credentials)

        assert loaded["entity-1"] == {1, 3, 5, 6, 7}

